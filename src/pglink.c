#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/uuid.h"

#include "binary.h"
#include "fdw.h"
#include "http.h"
#include "http_streaming.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

static bool initialized = false;

/* Rows buffered for one Native INSERT over HTTP. */
typedef struct {
    char* sql;       /* INSERT statement, for error reporting */
    char* sql_begin; /* sql plus the FORMAT clause the body follows */
    pgch_writer* writer;
    AttrNumber* attnums; /* slot attribute feeding each column */
    Oid* atttypids;
    size_t ncols;
    ch_http_connection_t* conn;
} ch_http_insert_state;

static void
http_disconnect(void* conn);
static ch_cursor*
http_simple_query(void* conn, const ch_query* query);
static void
http_simple_insert(void* conn, const ch_query* query);
static void
http_cursor_free(void*);
static ch_cursor*
http_native_cursor(void* conn, const ch_query* query);
static void
http_native_read_error(ch_cursor* cursor);
static void
http_native_cursor_free(void*);
static void
native_cursor_state_free(void*);
static void
native_cursor_raise_error(ch_cursor* cursor);
static Datum*
apply_binary_row(ChFdwScanRowContext* ctx);
static Datum*
native_fetch_row(ChFdwScanRowContext* ctx);
static void
binary_fetch_row_errcb(void* arg);
static void
configure_native_cursor(ch_cursor* cursor, const ch_query* query);
static void*
http_prepare_insert(void*, ResultRelInfo*, List*, const ch_query*, char*);
static void
http_insert_tuple(void*, TupleTableSlot*);
static void
report_http_stream_query_failure(void* conn, const ch_query* query, HttpStream* stream);
static ch_server_version
http_server_version(void* conn);

static libclickhouse_methods http_methods = {
    .disconnect          = http_disconnect,
    .simple_query        = http_simple_query,
    .fetch_row           = native_fetch_row,
    .prepare_insert      = http_prepare_insert,
    .insert_tuple        = http_insert_tuple,
    .streaming_query     = http_native_cursor,
    .streaming_fetch_row = native_fetch_row,
    .server_version      = http_server_version,
};

static void
binary_disconnect(void* conn);
static ch_cursor*
binary_simple_query(void* conn, const ch_query* query);
static void
binary_cursor_free(void* cursor);
static bool
binary_is_broken(const void* conn);

/* static void binary_simple_insert(void *conn, const char *query); */
static void
binary_insert_tuple(void*, TupleTableSlot* slot);
static void
binary_finalize_insert(void* istate);
static void*
binary_prepare_insert(
    void*,
    ResultRelInfo*,
    List*,
    const ch_query* query,
    char* table_name
);
static char*
ch_escape_string(const char* s, size_t len);
static void
ch_quote_literal_internal(char* dst, const char* src, size_t len);
extern char*
ch_quote_literal(const char* rawstr);
extern const char*
ch_quote_ident(const char* rawstr);
static ch_server_version
binary_server_version(void* conn);

static libclickhouse_methods binary_methods = {
    .disconnect          = binary_disconnect,
    .simple_query        = binary_simple_query,
    .fetch_row           = native_fetch_row,
    .prepare_insert      = binary_prepare_insert,
    .insert_tuple        = binary_insert_tuple,
    .finalize_insert     = binary_finalize_insert,
    .streaming_query     = NULL,
    .streaming_fetch_row = NULL,
    .is_broken           = binary_is_broken,
    .server_version      = binary_server_version,
};

static int
http_progress_callback(
    void* clientp,
    curl_off_t dltotal,
    curl_off_t dlnow,
    curl_off_t ultotal,
    curl_off_t ulnow
) {
    if (ProcDiePending || QueryCancelPending) {
        return 1;
    }

    return 0;
}

static bool
is_canceled(void) {
    /* this variable is bool on pg < 12, but sig_atomic_t on above versions */
    if (QueryCancelPending) {
        return true;
    }

    return false;
}

ch_connection
chfdw_http_connect(ch_connection_details* details) {
    ch_connection res;
    ch_http_connection_t* conn;

    if (!initialized) {
        initialized = true;
        ch_http_init(0, (uint32_t)MyProcPid);
    }

    /*
     * Since http.c will set the database name in a plain text header, we
     * cannot allow line endings because they could allow header injection.
     */
    if (details->dbname) {
        for (char* c = details->dbname; *c != '\0'; c++) {
            if (*c == '\n' || *c == '\r') {
                ereport(
                    ERROR,
                    errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
                    errmsg(
                        "pg_clickhouse: unsupported line ending character in database "
                        "name"
                    ),
                    errdetail(
                        "Invalid database name: %s", ch_quote_literal(details->dbname)
                    )
                );
            }
        }
    }

    conn = ch_http_connection(details);
    if (conn == NULL) {
        char* error = ch_http_last_error();

        if (error == NULL) {
            error = "undefined";
        }

        ereport(
            ERROR,
            errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
            errmsg("could not connect to server: %s", error)
        );
    }

    res.conn      = conn;
    res.methods   = &http_methods;
    res.is_binary = false;
    return res;
}

/*
 * Disconnect any open connection for a connection cache entry.
 */
static void
http_disconnect(void* conn) {
    if (conn != NULL) {
        ch_http_close((ch_http_connection_t*)conn);
    }
}

static ch_server_version
http_server_version(void* conn) {
    ch_server_version v = { 0, 0, 0 };

    ch_http_server_version((ch_http_connection_t*)conn, &v.major, &v.minor, &v.patch);
    return v;
}

/*
 * Return text before version mentioning
 */
static char*
format_error(char* errstring) {
    size_t n = strlen(errstring);

    for (size_t i = 0; i < n; i++) {
        if (strncmp(errstring + i, "version", 7) == 0) {
            return pnstrdup(errstring, i - 2);
        }
    }

    /*
     * For some reason ClickHouse 25.12 added a newline to an auth failure
     * error. Strip it out.
     */
    if (n > 0 && errstring[n - 1] == '\n') {
        errstring[--n] = '\0';
    }

    return errstring;
}

static void
kill_query(void* conn, const char* query_id) {
    ch_http_response_t* resp;
    ch_query query = new_query(
        psprintf("kill query where query_id=%s", ch_quote_literal(query_id)),
        0,
        NULL,
        NULL,
        NULL
    );

    ch_http_set_progress_func(NULL);
    resp = ch_http_simple_query(conn, &query);
    if (resp != NULL) {
        ch_http_response_free(resp);
    }
}

static void
report_http_stream_query_failure(
    void* conn,
    const ch_query* query,
    HttpStream* stream
) {
    long status = ch_http_stream_status(stream);

    PG_TRY();
    {
        if (status == CH_HTTP_STATUS_CANCELED) {
            char qid[CH_HTTP_QUERY_ID_LEN];

            memcpy(qid, ch_http_stream_query_id(stream), sizeof(qid));
            kill_query(conn, qid);
            ereport(
                ERROR,
                errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("pg_clickhouse: query was aborted")
            );
        } else if (status == CH_HTTP_STATUS_TRANSPORT_ERROR) {
            const char* err = ch_http_stream_error(stream);

            ereport(
                ERROR,
                errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
                errmsg(
                    "pg_clickhouse: communication error: %s",
                    err ? err : "connection error"
                )
            );
        } else {
            char* error = pnstrdup(
                ch_http_stream_buffer(stream), ch_http_stream_available(stream)
            );

            ereport(
                ERROR,
                errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("pg_clickhouse: %s", format_error(error)),
                status < 404 ? 0
                             : errdetail_internal("Remote Query: %.64000s", query->sql),
                errcontext("HTTP status code: %li", status)
            );
        }
    }
    PG_FINALLY();
    { ch_http_stream_end(stream); }
    PG_END_TRY();
}

static ch_cursor*
http_simple_query(void* conn, const ch_query* query) {
    int attempts = 0;
    if (!query->raw_result) {
        return http_native_cursor(conn, query);
    }
    /*
     * volatile: changed after setjmp (PG_TRY) and read after longjmp
     * (PG_CATCH); longjmp needn't restore register-cached locals, so a
     * non-volatile such local has an indeterminate value per C setjmp rules.
     */
    volatile MemoryContext tempcxt = NULL;
    MemoryContext oldcxt;
    ch_cursor* cursor;
    ch_http_response_t* resp;

    ch_http_set_progress_func(http_progress_callback);

again:
    resp = ch_http_simple_query(conn, query);
    if (resp == NULL) {
        ereport(ERROR, errcode(ERRCODE_FDW_OUT_OF_MEMORY), errmsg("out of memory"));
    }

    attempts++;
    if (resp->http_status == CH_HTTP_STATUS_TRANSPORT_ERROR) {
        char* error = pnstrdup(resp->data, resp->datasize);

        ch_http_response_free(resp);

        if (attempts < 3) {
            goto again;
        }

        ereport(
            ERROR,
            errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
            errmsg("pg_clickhouse: communication error: %s", error)
        );
    } else if (resp->http_status == CH_HTTP_STATUS_CANCELED) {
        kill_query(conn, resp->query_id);
        ch_http_response_free(resp);

        ereport(
            ERROR,
            errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
            errmsg("pg_clickhouse: query was aborted")
        );
    } else if (resp->http_status != CH_HTTP_STATUS_OK) {
        char* error = pnstrdup(resp->data, resp->datasize);
        long status = resp->http_status;

        ch_http_response_free(resp);

        ereport(
            ERROR,
            errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
            errmsg("pg_clickhouse: %s", format_error(error)),
            status < 404 ? 0 : errdetail_internal("Remote Query: %.64000s", query->sql),
            errcontext("HTTP status code: %li", status)
        );
    }

    PG_TRY();
    {
        /*
         * If any palloc below throws, use PG_CATCH to free the Curl response.
         */
        tempcxt = AllocSetContextCreate(
            PortalContext, "pg_clickhouse cursor", ALLOCSET_DEFAULT_SIZES
        );
        oldcxt = MemoryContextSwitchTo(tempcxt);

        cursor                 = palloc0(sizeof(ch_cursor));
        cursor->conn           = conn;
        cursor->query_response = resp;
        cursor->query          = pstrdup(query->sql);
        cursor->request_time   = resp->pretransfer_time * 1000;
        cursor->total_time     = resp->total_time * 1000;

        cursor->memcxt        = tempcxt;
        cursor->callback.func = http_cursor_free;
        cursor->callback.arg  = cursor;
        MemoryContextRegisterResetCallback(tempcxt, &cursor->callback);
        MemoryContextSwitchTo(oldcxt);
    }
    PG_CATCH();
    {
        if (resp) {
            ch_http_response_free(resp);
        }
        if (tempcxt) {
            MemoryContextDelete(tempcxt);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    return cursor;
}

static void
http_simple_insert(void* conn, const ch_query* query) {
    ch_http_response_t* resp = ch_http_simple_query(conn, query);

    if (resp == NULL) {
        char* error = ch_http_last_error();

        if (error == NULL) {
            error = "undefined";
        }

        ereport(
            ERROR,
            errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
            errmsg("pg_clickhouse: communication error: %s", error)
        );
    }

    if (resp->http_status != CH_HTTP_STATUS_OK) {
        char* error = pnstrdup(resp->data, resp->datasize);
        long status = resp->http_status;

        ch_http_response_free(resp);

        ereport(
            ERROR,
            errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
            errmsg("pg_clickhouse: %s", format_error(error)),
            status < 404 ? 0 : errdetail_internal("Remote Query: %.64000s", query->sql),
            errcontext("HTTP status code: %li", status)
        );
    }

    ch_http_response_free(resp);
}

inline static void
http_cursor_free(void* c) {
    ch_http_response_free(((ch_cursor*)c)->query_response);
}

/* pgch_chunk_source cancellation poll, checked between reads. */
static bool
native_chunks_cancelled(void* ud pg_attribute_unused()) {
    return QueryCancelPending || ProcDiePending;
}

/* Create shared-decoder cursor over HTTP Native response. */
static ch_cursor*
http_native_cursor(void* conn, const ch_query* query) {
    int attempts = 0;
    /* volatile: modified inside PG_TRY, read after longjmp in PG_CATCH */
    volatile MemoryContext tempcxt = NULL;
    HttpStream* volatile stream;
    MemoryContext oldcxt;
    ch_cursor* cursor;
    pgch_reader* state;

    ch_http_set_progress_func(http_progress_callback);

again:
    stream = ch_http_stream_begin(conn, query, true);
    if (stream == NULL) {
        ereport(
            ERROR,
            errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("pg_clickhouse: failed to initialize HTTP stream")
        );
    }

    attempts++;
    if (ch_http_stream_status(stream) == CH_HTTP_STATUS_TRANSPORT_ERROR &&
        attempts < 3) {
        ch_http_stream_end(stream);
        goto again;
    }
    if (ch_http_stream_status(stream) != CH_HTTP_STATUS_OK) {
        report_http_stream_query_failure(conn, query, stream);
    }

    PG_TRY();
    {
        tempcxt = AllocSetContextCreate(
            PortalContext, "pg_clickhouse native cursor", ALLOCSET_DEFAULT_SIZES
        );
        oldcxt = MemoryContextSwitchTo(tempcxt);

        cursor               = palloc0(sizeof(ch_cursor));
        cursor->conn         = conn;
        cursor->query        = pstrdup(query->sql);
        cursor->request_time = ch_http_stream_request_time(stream);
        cursor->total_time   = ch_http_stream_total_time(stream);
        cursor->read_error   = http_native_read_error;
        state                = palloc0(sizeof(pgch_reader));
        cursor->read_state   = state;

        /* Register before taking the stream, so unwinding closes it. */
        cursor->memcxt        = tempcxt;
        cursor->callback.func = http_native_cursor_free;
        cursor->callback.arg  = cursor;
        MemoryContextRegisterResetCallback(tempcxt, &cursor->callback);
        cursor->query_response = stream;
        stream                 = NULL;

        pgch_chunk_source src = { .ud         = cursor->query_response,
                                  .next_chunk = ch_http_stream_next_chunk,
                                  .cancelled  = native_chunks_cancelled };

        /* Blocks decode into tempcxt, outliving the per-row context. */
        pgch_reader_init_chunks(state, &src, NULL);
        cursor->columns_count = pgch_reader_columns(state);

        MemoryContextSwitchTo(oldcxt);
    }
    PG_CATCH();
    {
        if (stream) {
            ch_http_stream_end(stream);
        }
        if (tempcxt) {
            MemoryContextDelete(tempcxt);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (state->error) {
        native_cursor_raise_error(cursor);
    }

    configure_native_cursor(cursor, query);

    return cursor;
}

text*
chfdw_http_fetch_raw_data(ch_cursor* cursor) {
    ch_http_response_t* resp = cursor->query_response;

    if (resp->data == NULL) {
        return NULL;
    }

    return cstring_to_text_with_len(resp->data, resp->datasize);
}

/*
 * Convert a Datum to a ClickHouse literal string. Returns NULL if the value
 * cannot be converted to a literal.
 */
extern char*
chfdw_datum_to_ch_literal(Datum value, Oid type) {
    if (type_is_array(type)) {
        return chfdw_array_to_ch_literal(value);
    }

    switch (type) {
    case BOOLOID:
    case INT2OID:
    case INT4OID:
        return psprintf("%d", DatumGetInt32(value));
    case INT8OID:
        return psprintf(INT64_FORMAT, DatumGetInt64(value));
    case FLOAT4OID:
        return psprintf("%f", DatumGetFloat4(value));
    case FLOAT8OID:
        return psprintf("%f", DatumGetFloat8(value));
    case NUMERICOID:
        return DatumGetCString(DirectFunctionCall1(numeric_out, value));
    case BPCHAROID:
    case VARCHAROID:
    case TEXTOID:
    case JSONOID:
    case JSONBOID:
    case NAMEOID:
    case BITOID:
    case UUIDOID:
    case INETOID: {
        char* text;
        bool tl       = false;
        Oid typoutput = InvalidOid;

        getTypeOutputInfo(type, &typoutput, &tl);
        text = OidOutputFunctionCall(typoutput, value);
        return ch_escape_string(text, strlen(text));
    }
    case BYTEAOID: {
        /* Copy all of the bytes into a ClickHouse literal string. */
        bytea* bytes = PG_DETOAST_DATUM(value);

        return ch_escape_string(VARDATA(bytes), VARSIZE_ANY_EXHDR(bytes));
    }
    case DATEOID:
        /* we expect Date on other side */
        return DatumGetCString(DirectFunctionCall1(ch_date_out, value));
    case TIMEOID: {
        /* we expect DateTime on other side */
        char* extval = DatumGetCString(DirectFunctionCall1(ch_time_out, value));
        char* retval = psprintf("1970-01-01 %s", extval);

        pfree(extval);
        return retval;
    }
    case TIMESTAMPOID:
    case TIMESTAMPTZOID:
        /* we expect DateTime on other side */
        return DatumGetCString(DirectFunctionCall1(ch_timestamp_out, value));
    default:
        ereport(
            ERROR,
            errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
            errmsg("cannot convert value to clickhouse value"),
            errhint("Value data type: %u", type)
        );
    }
}

/*
 * Serialize buffered rows as a Native block and POST them.
 *
 * Column types come from PostgreSQL, so they rarely match the destination
 * exactly. ClickHouse casts them per column name under
 * input_format_native_allow_types_conversion, on by default since 23.3.
 */
static void
http_flush_insert(ch_http_insert_state* state) {
    pgch_buf body = {};

    if (pgch_writer_rows(state->writer) == 0) {
        return;
    }

    pgch_buf_append(&body, state->sql_begin, strlen(state->sql_begin));
    /* NULL opts: no block info or custom serialization, matching the reader. */
    pgch_writer_flush(state->writer, &body, NULL);

    ch_query query = new_body_query(state->sql, body.data, body.len);

    http_simple_insert(state->conn, &query);
    pgch_buf_reset(&body);
}

static void*
http_prepare_insert(
    void* conn,
    ResultRelInfo* rri,
    List* target_attrs,
    const ch_query* query,
    char* table_name
) {
    ch_http_insert_state* state = palloc0(sizeof(ch_http_insert_state));
    Relation rel                = rri->ri_RelationDesc;
    TupleDesc tupdesc           = RelationGetDescr(rel);
    Oid relid                   = RelationGetRelid(rel);
    size_t ncols                = list_length(target_attrs);
    pgch_col* cols              = palloc0(ncols * sizeof(pgch_col));
    ListCell* lc;
    size_t i = 0;

    state->ncols     = ncols;
    state->attnums   = palloc0(ncols * sizeof(AttrNumber));
    state->atttypids = palloc0(ncols * sizeof(Oid));

    foreach (lc, target_attrs) {
        AttrNumber attnum       = lfirst_int(lc);
        Form_pg_attribute attr  = TupleDescAttr(tupdesc, attnum - 1);
        CustomColumnInfo* cinfo = chfdw_get_custom_column_info(relid, attnum);
        /* Name must match the INSERT column list chfdw_deparse_insert_sql built */
        const char* colname =
            (cinfo && cinfo->colname[0]) ? cinfo->colname : NameStr(attr->attname);
        const char* chtype;
        chc_err err = {};
        chc_type* coltype;

        /*
         * ClickHouse gained Time64 in 25.6 and casts it to none of the types
         * a table holds a time of day in, so send a timestamp on the epoch
         * date, as the TabSeparated payload did.
         */
        if (attr->atttypid == TIMEOID) {
            chtype = attr->attnotnull ? "DateTime64(6, 'UTC')"
                                      : "Nullable(DateTime64(6, 'UTC'))";
        } else {
            chtype = pgch_ch_type_for(
                attr->atttypid, attr->atttypmod, attr->attnotnull, NULL
            );
        }

        /* A PostgreSQL array type carries no dimension count, only the
         * declared attndims does, and ClickHouse nests one Array per
         * dimension */
        for (int dim = 1; dim < attr->attndims; dim++) {
            chtype = psprintf("Array(%s)", chtype);
        }

        if (chc_type_parse(chtype, strlen(chtype), &pgch_alloc, &coltype, &err) !=
            CHC_OK) {
            ereport(
                ERROR,
                errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                errmsg(
                    "pg_clickhouse: could not build ClickHouse type for column \"%s\"",
                    colname
                ),
                errdetail_internal("%s: %s", chtype, err.msg)
            );
        }

        state->attnums[i]   = attnum;
        state->atttypids[i] = attr->atttypid;
        cols[i].name        = colname;
        cols[i].name_len    = strlen(colname);
        cols[i].type        = coltype;
        i++;
    }

    state->writer    = pgch_writer_new(CurrentMemoryContext, cols, ncols);
    state->sql       = pstrdup(query->sql);
    state->sql_begin = psprintf("%s FORMAT Native\n", query->sql);
    state->conn      = conn;

    return state;
}

static void
http_insert_tuple(void* istate, TupleTableSlot* slot) {
    ch_http_insert_state* state = istate;

    if (slot != NULL) {
        for (size_t i = 0; i < state->ncols; i++) {
            bool isnull;
            Datum value = slot_getattr(slot, state->attnums[i], &isnull);
            Oid valtype = state->atttypids[i];

            /* PostgreSQL casts inet to text through network_show, which
             * appends a netmask ClickHouse rejects for IPv4 and IPv6. The
             * output function omits it for single hosts. */
            if (valtype == INETOID && !isnull) {
                value   = CStringGetTextDatum(OidOutputFunctionCall(F_INET_OUT, value));
                valtype = TEXTOID;
            } else if (valtype == TIMEOID && !isnull) {
                /* Pair with the DateTime64 column http_prepare_insert declares */
                value = TimestampTzGetDatum(
                    DatumGetTimeADT(value) -
                    (TimestampTz)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *
                        USECS_PER_DAY
                );
                valtype = TIMESTAMPTZOID;
            }

            pgch_append_datum(state->writer, i, value, valtype, isnull);
        }

        /* Flush at 64MiB so bulk loads stream instead of buffering every row */
        if (pgch_writer_bytes(state->writer) < 64 * 1024 * 1024) {
            return;
        }
    }
    http_flush_insert(state);
}

/*** BINARY PROTOCOL ***/

ch_connection
chfdw_binary_connect(ch_connection_details* details) {
    ch_connection res;

    res.conn      = ch_binary_connect(details);
    res.methods   = &binary_methods;
    res.is_binary = true;
    return res;
}

static void
binary_disconnect(void* conn) {
    if (conn != NULL) {
        ch_binary_close((ch_binary_connection_t*)conn);
    }
}

static bool
binary_is_broken(const void* conn) {
    return ch_binary_is_broken((const ch_binary_connection_t*)conn);
}

static ch_server_version
binary_server_version(void* conn) {
    ch_server_version v = { 0, 0, 0 };

    ch_binary_server_version(
        (ch_binary_connection_t*)conn, &v.major, &v.minor, &v.patch
    );
    return v;
}

static ch_cursor*
binary_simple_query(void* conn, const ch_query* query) {
    MemoryContext tempcxt, oldcxt;
    ch_cursor* cursor;
    pgch_reader* state;

    ch_binary_response_t* resp = ch_binary_simple_query(conn, query, &is_canceled);

    if (!ch_binary_response_success(resp)) {
        char* error = pstrdup(ch_binary_response_error(resp));

        ch_binary_response_free(resp);

        /* Prefer consistent interrupt error message when query interrupted */
        CHECK_FOR_INTERRUPTS();
        ereport(
            ERROR,
            errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
            errmsg("pg_clickhouse: %s", error),
            errdetail_internal("Remote Query: %.64000s", query->sql)
        );
    }

    tempcxt = AllocSetContextCreate(
        PortalContext, "pg_clickhouse cursor", ALLOCSET_DEFAULT_SIZES
    );

    oldcxt                 = MemoryContextSwitchTo(tempcxt);
    cursor                 = palloc0(sizeof(ch_cursor));
    cursor->conn           = conn;
    cursor->query_response = resp;
    state                  = (pgch_reader*)palloc0(sizeof(pgch_reader));
    cursor->query          = pstrdup(query->sql);
    cursor->read_state     = state;
    pgch_block_source src  = ch_binary_response_block_source(resp);
    pgch_reader_init(cursor->read_state, &src);
    cursor->columns_count = pgch_reader_columns(state);
    cursor->memcxt        = tempcxt;
    cursor->callback.func = binary_cursor_free;
    cursor->callback.arg  = cursor;
    MemoryContextRegisterResetCallback(tempcxt, &cursor->callback);

    configure_native_cursor(cursor, query);

    MemoryContextSwitchTo(oldcxt);

    if (state->error) {
        native_cursor_raise_error(cursor);
    }

    return cursor;
}

/*
 * Escape characters that would otherwise corrupt the tab/newline framing or
 * collide with the \N null marker. Matches CH's TabSeparated escaping; \0 is
 * unreachable since values arrive as cstrings, so it needs no case.
 */
static void
append_tsv_escaped(StringInfo buf, const char* s) {
    for (; *s != '\0'; s++) {
        switch (*s) {
        case '\\':
            appendStringInfoString(buf, "\\\\");
            break;
        case '\b':
            appendStringInfoString(buf, "\\b");
            break;
        case '\f':
            appendStringInfoString(buf, "\\f");
            break;
        case '\n':
            appendStringInfoString(buf, "\\n");
            break;
        case '\r':
            appendStringInfoString(buf, "\\r");
            break;
        case '\t':
            appendStringInfoString(buf, "\\t");
            break;
        default:
            appendStringInfoChar(buf, *s);
        }
    }
}

/*
 * Drain a binary cursor into tab-separated rows, mirroring the single-text
 * result of the http path. Nulls render as \N, other values escape the
 * control characters CH's TabSeparated format does so they stay unambiguous.
 * Output formatting otherwise differs from the http driver since values pass
 * through PG output functions rather than ClickHouse's wire formatting.
 */
text*
chfdw_binary_fetch_raw_data(ch_cursor* cursor) {
    pgch_reader* state = cursor->read_state;
    size_t ncols       = pgch_reader_columns(state);
    StringInfoData buf;

    if (ncols == 0) {
        return NULL;
    }

    initStringInfo(&buf);

    while (pgch_reader_next(state)) {
        for (size_t i = 0; i < ncols; i++) {
            if (i > 0) {
                appendStringInfoChar(&buf, '\t');
            }

            if (state->nulls[i]) {
                appendStringInfoString(&buf, "\\N");
            } else {
                char* val = pgch_value_to_cstring(state->coltypes[i], state->values[i]);

                append_tsv_escaped(&buf, val);
                pfree(val);
            }
        }
        appendStringInfoChar(&buf, '\n');
        CHECK_FOR_INTERRUPTS();
    }

    if (state->error) {
        ereport(
            ERROR,
            errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
            errmsg("pg_clickhouse: %s", state->error)
        );
    }

    if (buf.len == 0) {
        pfree(buf.data);
        return NULL;
    }

    return cstring_to_text_with_len(buf.data, buf.len);
}

/*
 * Fetch a row from the binary cursor and return its values.
 *
 * If ctx->tupdesc is set, ctx->attinmeta must also be set, and ctx->values
 * and ctx->nulls must already be palloc'd with space for ctx->tupdesc->natts
 * values.
 *
 * Use ctx->tupdesc and ctx->attinmeta to convert the values to the
 * appropriate Datums, and store them and the indication of their NULLness in
 * ctx->values and ctx->nulls, respectively, then return ctx->values.
 *
 * If ctx->tupdesc is not set, treat all values as text and return them as
 * text `Datum`s. This is the use case for `chfdw_construct_create_tables()`,
 * which only cares about text.
 */
static void
binary_fetch_row_errcb(void* arg) {
    const char* sql = (const char*)arg;

    errdetail_internal("Remote Query: %.64000s", sql);
}

/* Conversion state and target attribute per returned column. */
static void
build_conversion(ch_cursor* cursor, const ChFdwScanRowContext* ctx) {
    pgch_reader* state = cursor->read_state;
    MemoryContext old  = MemoryContextSwitchTo(cursor->memcxt);
    size_t ncols       = pgch_reader_columns(state);
    ListCell* lc;
    size_t j = 0;

    cursor->conversion_states = palloc0(ncols * sizeof(void*));
    cursor->fill_dest         = palloc0(ncols * sizeof(int));
    foreach (lc, ctx->retrieved_attrs) {
        int attnum            = lfirst_int(lc);
        Form_pg_attribute att = TupleDescAttr(ctx->tupdesc, attnum - 1);

        cursor->fill_dest[j] = attnum - 1;
        cursor->conversion_states[j] =
            pgch_reader_convert_init(state, j, att->atttypid, att->atttypmod);
        j++;
    }

    MemoryContextSwitchTo(old);
}

static void
configure_native_cursor(ch_cursor* cursor, const ch_query* query) {
    pgch_reader* state = cursor->read_state;

    if (query->tupdesc && query->attr_nums && cursor->columns_count > 0 &&
        (size_t)list_length(query->attr_nums) != cursor->columns_count) {
        ereport(
            ERROR,
            errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg_internal(
                "pg_clickhouse: returned %lu columns, expected %lu",
                (unsigned long)cursor->columns_count,
                (unsigned long)list_length(query->attr_nums)
            ),
            errdetail_internal("Remote Query: %.64000s", query->sql)
        );
    }

    /* Preserve JSON text when PostgreSQL destination uses json, not jsonb. */
    if (query->tupdesc && state->coltypes) {
        ListCell* lc;
        size_t j = 0;

        foreach (lc, query->attr_nums) {
            int i = lfirst_int(lc);

            if (state->coltypes[j] == JSONBOID &&
                TupleDescAttr(query->tupdesc, i - 1)->atttypid == JSONOID) {
                state->coltypes[j] = JSONOID;
            }
            j++;
        }
    }
}

/* Apply PostgreSQL conversions to fetched Native row. */
static Datum*
apply_binary_row(ChFdwScanRowContext* ctx) {
    ch_cursor* cursor  = ctx->cursor;
    List* attrs        = ctx->retrieved_attrs;
    TupleDesc tupdesc  = ctx->tupdesc;
    Datum* values      = ctx->values;
    bool* nulls        = ctx->nulls;
    pgch_reader* state = cursor->read_state;
    size_t attcount    = list_length(attrs);

    if (attcount == 0) {
        if (pgch_reader_columns(state) == 1 && state->nulls[0]) {
            nulls[0] = true;
            return state->values;
        }
        ereport(
            ERROR,
            errcode(ERRCODE_FDW_ERROR),
            errmsg(
                "pg_clickhouse: unexpected state: attributes "
                "count == 0 and haven't got NULL in the response"
            )
        );
    } else if (attcount != pgch_reader_columns(state)) {
        ereport(
            ERROR,
            errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg_internal(
                "pg_clickhouse: returned %lu columns, expected %lu",
                pgch_reader_columns(state),
                attcount
            )
        );
    }

    if (tupdesc) {
        Assert(values && nulls);

        if (cursor->conversion_states == NULL) {
            build_conversion(cursor, ctx);
        }
        pgch_reader_fill_map(
            state, cursor->conversion_states, cursor->fill_dest, values, nulls
        );
    }

    return state->values;
}

/* Raise decoder error; read_error hook may convert to cancellation report. */
static void
native_cursor_raise_error(ch_cursor* cursor) {
    pgch_reader* state = cursor->read_state;

    if (cursor->read_error) {
        cursor->read_error(cursor);
    }
    /* Prefer consistent interrupt error message when fetch interrupted */
    CHECK_FOR_INTERRUPTS();
    ereport(
        ERROR,
        errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
        errmsg("pg_clickhouse: %s", state->error),
        errdetail_internal("Remote Query: %.64000s", cursor->query)
    );
}

static Datum*
native_fetch_row(ChFdwScanRowContext* ctx) {
    ch_cursor* cursor  = ctx->cursor;
    pgch_reader* state = cursor->read_state;
    ErrorContextCallback errcallback;
    bool have_data;
    Datum* result;

    errcallback.callback = binary_fetch_row_errcb;
    errcallback.arg      = (void*)cursor->query;
    errcallback.previous = error_context_stack;
    error_context_stack  = &errcallback;

    have_data = pgch_reader_next(state);

    if (state->error) {
        error_context_stack = errcallback.previous;
        native_cursor_raise_error(cursor);
    }

    result = have_data ? apply_binary_row(ctx) : NULL;

    error_context_stack = errcallback.previous;
    return result;
}

static void
http_native_read_error(ch_cursor* cursor) {
    HttpStream* stream = cursor->query_response;

    if (stream == NULL) {
        return;
    }

    if (ch_http_stream_status(stream) == CH_HTTP_STATUS_CANCELED ||
        QueryCancelPending || ProcDiePending) {
        char qid[CH_HTTP_QUERY_ID_LEN];

        memcpy(qid, ch_http_stream_query_id(stream), sizeof(qid));
        /* Drop the transfer before asking the server to kill the query. */
        ch_http_stream_end(stream);
        cursor->query_response = NULL;
        kill_query(cursor->conn, qid);
        ereport(
            ERROR,
            errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
            errmsg("pg_clickhouse: query was aborted")
        );
    }
}

static void
http_native_cursor_free(void* c) {
    ch_cursor* cursor = c;

    native_cursor_state_free(cursor);
    ch_http_stream_end(cursor->query_response);
    cursor->query_response = NULL;
}

static void
binary_cursor_free(void* c) {
    ch_cursor* cursor = c;

    native_cursor_state_free(cursor);
    ch_binary_response_free(cursor->query_response);
}

/* Conversion states live in the context this callback fires for. */
static void
native_cursor_state_free(void* c) {
    ch_cursor* cursor = c;

    pgch_reader_free(cursor->read_state);
}

static void*
binary_prepare_insert(
    void* conn,
    ResultRelInfo* rri,
    List* target_attrs,
    const ch_query* query,
    char* table_name
) {
    ch_binary_insert_state* state = NULL;
    MemoryContext tempcxt, oldcxt;

    if (table_name == NULL) {
        ereport(ERROR, errcode(ERRCODE_FDW_ERROR), errmsg("expected table name"));
    }

    tempcxt = AllocSetContextCreate(
        CurrentMemoryContext,
        "pg_clickhouse binary insert state",
        ALLOCSET_DEFAULT_SIZES
    );

    /* prepare cleanup */
    oldcxt        = MemoryContextSwitchTo(tempcxt);
    state         = (ch_binary_insert_state*)palloc0(sizeof(ch_binary_insert_state));
    state->memcxt = tempcxt;
    state->callback.func = ch_binary_insert_state_free;
    state->callback.arg  = state;
    state->relid         = RelationGetRelid(rri->ri_RelationDesc);
    MemoryContextRegisterResetCallback(tempcxt, &state->callback);

    /* enter insert mode, take the column list from the server */
    ch_binary_prepare_insert(conn, query, state);
    MemoryContextSwitchTo(oldcxt);

    return state;
}

static void
binary_insert_tuple(void* istate, TupleTableSlot* slot) {
    ch_binary_insert_state* state = istate;

    if (slot) {
        ch_binary_insert_tuple(state, slot);
        ch_binary_insert_autoflush(state);
    } else {
        ch_binary_insert_columns(state);
    }
}

static void
binary_finalize_insert(void* istate) {
    ch_binary_insert_state* state = istate;

    if (state && state->insert_block) {
        ch_binary_finalize_insert(state->insert_block);
    }
}

/*
 * Query to generate table for doc/pg_clickhouse.md. Keep in sync with
 * str_types_map below. On change, re-run and paste the output into
 * doc/pg_clickhouse.md. Perl: https://stackoverflow.com/a/58443028/79202

    psql --no-psqlrc --pset border=2 --pset footer=off -c "
    SELECT * FROM ( VALUES
        ('Bool',     'boolean',          ''),
        ('Int8',     'smallint',         ''),
        ('UInt8',    'smallint',         ''),
        ('Int16',    'smallint',         ''),
        ('UInt16',   'integer',          ''),
        ('Int32',    'integer',          ''),
        ('UInt32',   'bigint',           ''),
        ('Int64',    'bigint',           ''),
        ('UInt64',   'bigint',           'Errors on values > BIGINT max'),
        ('Float32',  'real',             ''),
        ('Float64',  'double precision', ''),
        ('Decimal',  'numeric',          ''),
        ('String',   'text, bytea',      ''),
        ('DateTime', 'timestamptz',      ''),
        ('Date',     'date',             ''),
        ('Date32',   'date',             ''),
        ('UUID',     'uuid',             ''),
        ('IPv4',     'inet',             ''),
        ('IPv6',     'inet',             ''),
        ('JSON',     'jsonb, json',      '')
    ) AS v(\"ClickHouse\", \"PostgreSQL\", \"Notes\")
    ORDER BY \"ClickHouse\";
    " | perl -ne 'my $m = $.%2; print $buf[$m] if defined $buf[$m]; $buf[$m] = s/\+/|/gr
 if $.>1' | pbcopy

*/

static char* str_types_map[][2] = {
    { "Bool",     "BOOLEAN"          },
    { "Int8",     "INT2"             },
    { "UInt8",    "INT2"             },
    { "Int16",    "INT2"             },
    { "UInt16",   "INT4"             },
    { "Int32",    "INT4"             },
    { "UInt32",   "INT8"             },
    { "Int64",    "INT8"             },
    { "UInt64",   "INT8"             },
    { "Float32",  "REAL"             },
    { "Float64",  "DOUBLE PRECISION" },
    { "Decimal",  "NUMERIC"          },
    { "String",   "TEXT"             },
    { "DateTime", "TIMESTAMPTZ"      },
    { "Date",     "DATE"             }, /* must come after other Date types */
    { "Date32",   "DATE"             },
    { "UUID",     "UUID"             },
    { "IPv4",     "inet"             },
    { "IPv6",     "inet"             },
    { "JSON",     "JSONB"            },
    { NULL,       NULL               },
};

static char*
parse_type(
    char* table_name,
    char* colname,
    char* part,
    bool* is_nullable,
    List** options
) {
    char* typepart = part;
    char* pos      = strchr(typepart, '(');

    if (pos != NULL) {
        char* end = strrchr(typepart, ')');
        char* insidebr;

        if (end == NULL || end < pos) {
            ereport(
                ERROR,
                errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                errmsg("pg_clickhouse: malformed ClickHouse type \"%s\"", typepart)
            );
        }
        insidebr = pnstrdup(pos + 1, end - pos - 1);

        if (strncmp(typepart, "Decimal", strlen("Decimal")) == 0) {
            if (strchr(insidebr, ',') == NULL) {
                ereport(
                    ERROR,
                    errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                    errmsg(
                        "pg_clickhouse: could not import Decimal field, "
                        "should be two parameters on definition"
                    )
                );
            }

            return psprintf("NUMERIC(%s)", insidebr);
        } else if (strncmp(typepart, "FixedString", strlen("FixedString")) == 0) {
            return psprintf("VARCHAR(%s)", insidebr);
        } else if (strncmp(typepart, "Enum8", strlen("Enum8")) == 0) {
            return "TEXT";
        } else if (strncmp(typepart, "Enum16", strlen("Enum16")) == 0) {
            return "TEXT";
        } else if (strncmp(typepart, "DateTime64", strlen("DateTime64")) == 0) {
            return "TIMESTAMPTZ";
        } else if (strncmp(typepart, "DateTime", strlen("DateTime")) == 0) {
            return "TIMESTAMPTZ";
        } else if (strncmp(typepart, "Tuple", strlen("Tuple")) == 0) {
            elog(
                NOTICE,
                "pg_clickhouse: ClickHouse <Tuple> type was "
                "translated to <TEXT> type for column \"%s\", please create composite "
                "type and alter the column if needed",
                colname
            );
            return "TEXT";
        } else if (strncmp(typepart, "Array", strlen("Array")) == 0) {
            /* PostgreSQL arrays always allow NULL elements */
            bool elem_nullable = false;
            return psprintf(
                "%s[]",
                parse_type(table_name, colname, insidebr, &elem_nullable, options)
            );
        } else if (strncmp(typepart, "Nullable", strlen("Nullable")) == 0) {
            if (is_nullable == NULL) {
                ereport(
                    ERROR,
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("pg_clickhouse: nested Nullable is not supported")
                );
            }

            *is_nullable = true;
            return parse_type(table_name, colname, insidebr, NULL, options);
        } else if (strncmp(typepart, "LowCardinality", strlen("LowCardinality")) == 0) {
            return parse_type(table_name, colname, insidebr, is_nullable, options);
        } else if (
            strncmp(typepart, "AggregateFunction", strlen("AggregateFunction")) == 0 ||
            strncmp(
                typepart, "SimpleAggregateFunction", strlen("SimpleAggregateFunction")
            ) == 0
        ) {
            char* pos2 = strchr(pos, ',');

            if (pos2 == NULL) {
                /* Detect COUNT with no params. */
                if (strncmp(insidebr, "count", strlen("count")) == 0) {
                    return "BIGINT";
                }
                ereport(
                    ERROR,
                    errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                    errmsg("pg_clickhouse: expected comma in AggregateFunction")
                );
            }

            char* func = pnstrdup(pos + 1, strchr(pos + 1, ',') - pos - 1);

            if (options != NULL) {
                int val = typepart[0] == 'A' ? 1 : 2;

                *options = lappend(*options, makeInteger(val));
                *options = lappend(*options, makeString(func));
            }

            return parse_type(table_name, colname, pos2 + 2, is_nullable, options);
        }

        typepart = pos + 1;
    }

    size_t i = 0;

    while (str_types_map[i][0] != NULL) {
        if (strncmp(str_types_map[i][0], typepart, strlen(str_types_map[i][0])) == 0) {
            return pstrdup(str_types_map[i][1]);
        }
        i++;
    }

    ereport(
        ERROR,
        errmsg(
            "pg_clickhouse: could not map %s.%s type <%s>",
            quote_identifier(table_name),
            quote_identifier(colname),
            part
        )
    );
}

List*
chfdw_construct_create_tables(ImportForeignSchemaStmt* stmt, ForeignServer* server) {
    Oid userid         = GetUserId();
    UserMapping* user  = GetUserMapping(userid, server->serverid);
    ch_connection conn = chfdw_get_connection(user);
    ch_cursor* cursor;
    ch_query query = new_query(NULL, 0, NULL, NULL, NULL);
    List* result   = NIL;
    Datum* row_values;

    query.sql = psprintf(
        "SELECT name, engine, engine_full "
        "FROM system.tables "
        "WHERE name NOT LIKE '.inner%%' "
        "AND database = %s",
        ch_quote_literal(stmt->remote_schema)
    );

    cursor = conn.methods->simple_query(conn.conn, &query);

    ChFdwScanRowContext cols_ctx = {
        NULL, list_make2_int(1, 2), NULL, NULL, NULL, NULL
    };

    ChFdwScanRowContext tables_ctx = { NULL, list_make3_int(1, 2, 3),
                                       NULL, cursor,
                                       NULL, NULL };

    /*
     * Drain the outer query into private strings before opening the per-table
     * column queries: both use the same connection, and binary streaming only
     * permits one in-flight response at a time.
     */
    List* tables = NIL;

    while ((row_values = conn.methods->fetch_row(&tables_ctx)) != NULL) {
        List* triple = list_make3(
            pstrdup(TextDatumGetCString(row_values[0])),
            pstrdup(TextDatumGetCString(row_values[1])),
            pstrdup(TextDatumGetCString(row_values[2]))
        );

        CHECK_FOR_INTERRUPTS();
        tables = lappend(tables, triple);
    }
    MemoryContextDelete(cursor->memcxt);

    ListCell* tlc;

    foreach (tlc, tables) {
        List* triple      = (List*)lfirst(tlc);
        char* table_name  = (char*)linitial(triple);
        char* engine      = (char*)lsecond(triple);
        char* engine_full = (char*)lthird(triple);
        StringInfoData buf;
        Datum* dvalues;
        bool first = true;

        if (table_name == NULL) {
            continue;
        }

        if (list_length(stmt->table_list)) {
            ListCell* lc;
            bool found = false;

            foreach (lc, stmt->table_list) {
                RangeVar* rv = (RangeVar*)lfirst(lc);

                if (strcmp(rv->relname, table_name) == 0) {
                    found = true;
                }
            }

            if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT && found) {
                continue;
            } else if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO && !found) {
                continue;
            }
        }

        initStringInfo(&buf);
        appendStringInfo(
            &buf,
            "CREATE FOREIGN TABLE IF NOT EXISTS %s.%s (\n",
            quote_identifier(stmt->local_schema),
            quote_identifier(table_name)
        );
        query.sql = psprintf(
            "SELECT name, type "
            "FROM system.columns "
            "WHERE database = %s "
            "AND table = %s",
            ch_quote_literal(stmt->remote_schema),
            ch_quote_literal(table_name)
        );

        cols_ctx.cursor = conn.methods->simple_query(conn.conn, &query);
        while ((dvalues = conn.methods->fetch_row(&cols_ctx)) != NULL) {
            List* options     = NIL;
            bool is_nullable  = false;
            char* colname     = TextDatumGetCString(dvalues[0]);
            char* remote_type = parse_type(
                table_name,
                colname,
                TextDatumGetCString(dvalues[1]),
                &is_nullable,
                &options
            );

            if (!first) {
                appendStringInfoString(&buf, ",\n");
            }
            first = false;

            /* name */
            appendStringInfo(&buf, "\t%s ", quote_identifier(colname));

            /* type */
            appendStringInfoString(&buf, remote_type);

            if (options != NIL) {
                bool first_opt = true;
                ListCell* lc;

                appendStringInfoString(&buf, " OPTIONS (");
                foreach (lc, options) {
                    Node* val = lfirst(lc);

                    if (IsA(val, Integer)) {
                        if (!first_opt) {
                            appendStringInfoString(&buf, ", ");
                        }
                        first_opt = false;
                        switch
                            intVal(val) {
                            case 1:
                                appendStringInfoString(&buf, "AggregateFunction");
                                break;
                            case 2:
                                appendStringInfoString(&buf, "SimpleAggregateFunction");
                                break;
                            default:
                                elog(ERROR, "programming error");
                            }
                    } else {
                        appendStringInfoChar(&buf, ' ');
                        appendStringInfoString(&buf, ch_quote_literal(strVal(val)));
                    }
                }
                appendStringInfoString(&buf, ")");
                list_free_deep(options);
            }

            if (!is_nullable) {
                appendStringInfoString(&buf, " NOT NULL");
            }
        }

        appendStringInfo(
            &buf,
            "\n) SERVER %s OPTIONS (database %s, table_name %s",
            quote_identifier(server->servername),
            ch_quote_literal(stmt->remote_schema),
            ch_quote_literal(table_name)
        );

        if (engine && engine_full && strcmp(engine, "CollapsingMergeTree") == 0) {
            char* sub = strchr(engine_full, ')');

            if (sub) {
                sub[1] = '\0';
                appendStringInfo(&buf, ", engine %s", ch_quote_literal(engine_full));
            }
        } else if (engine) {
            appendStringInfo(&buf, ", engine %s", ch_quote_literal(engine));
        }

        appendStringInfoString(&buf, ");\n");
        result = lappend(result, buf.data);
        MemoryContextDelete(cols_ctx.cursor->memcxt);
    }

    return result;
}

/*
 * Escape len bytes from s as an unquoted ClickHouse literal string. Returns a
 * pointer to a palloc'd string.
 *
 * Based on ConvertToSQLString() in src/Client/BuzzHouse/AST/SQLProtoStr.cpp
 * and writeAnyEscapedStringO() in src/IO/WriteHelpers.h in the ClickHouse
 * source code.
 */
static char*
ch_escape_string(const char* from, size_t len) {
    char* result;
    size_t remaining   = len;
    const char* source = from;

    result       = palloc(len * 2 + 1);
    char* target = result;

    while (remaining > 0) {
        char c = *source;

        switch (c) {
        case '\'':
            *target++ = '\\';
            *target++ = c;
            break;
        case '\\':
            *target++ = c;
            *target++ = c;
            break;
        case '\b':
            *target++ = '\\';
            *target++ = 'b';
            break;
        case '\f':
            *target++ = '\\';
            *target++ = 'f';
            break;
        case '\r':
            *target++ = '\\';
            *target++ = 'r';
            break;
        case '\n':
            *target++ = '\\';
            *target++ = 'n';
            break;
        case '\t':
            *target++ = '\\';
            *target++ = 't';
            break;
        case '\0':
            *target++ = '\\';
            *target++ = '0';
            break;
        case '\a':
            *target++ = '\\';
            *target++ = 'a';
            break;
        case '\v':
            *target++ = '\\';
            *target++ = 'v';
            break;
        default:
            *target++ = c;
        }
        source++;
        remaining--;
    }

    *target = '\0';
    return result;
}

/*
 * Convenience function to single-quote a literal SQL string. Differs from
 * PostgreSQL's quote_literal_cstr() by never returning an E-quoted string.
 */
static void
ch_quote_literal_internal(char* dst, const char* src, size_t len) {
    *dst++ = '\'';
    while (*src) {
        if (SQL_STR_DOUBLE(*src, true)) {
            *dst++ = *src;
        }
        *dst++ = *src++;
    }
    *dst++ = '\'';
    *dst++ = '\0';
}

/*
 * Convenience function to escape and return a string as a ClickHouse literal.
 * Returns a palloc'd string.
 */
char*
ch_quote_literal(const char* rawstr) {
    char* result;
    int len;

    len = strlen(rawstr);
    /* We make a worst-case result area; wasting a little space is OK */
    result = palloc(
        (len * 2) /* doubling for every character if each one is
                   * a quote */
        + 2       /* two outer quotes */
        + 1       /* null terminator */
    );

    ch_quote_literal_internal(result, rawstr, len);
    return result;
}

/*
 * Function to quote a ClickHouse identifier. Simply returns `ident` if it's
 * already double-quoted or backtick-quoted. Otherwise quotes it using
 * PostgreSQL's `quote_identifier()`. Raises an error if the identifier length
 * is zero or greater than `NAMEDATALEN` (64) unquoted or
 * `CH_ESCAPED_NAMEDATALEN` quoted.
 */
const char*
ch_quote_ident(const char* ident) {
    /* https://clickhouse.com/docs/sql-reference/syntax#identifiers */
    int len = strlen(ident);

    if (len >= 2 && ((ident[0] == '"' && ident[len - 1] == '"') ||
                     (ident[0] == '`' && ident[len - 1] == '`'))) {
        /*
         * Make sure it has no unescaped quote character. Allowed escapes:
         *
         * ": (""|\\.)
         *
         * `: (``|\\.)
         */
        for (int i = 2; i <= len - 2; i++) {
            /* Skip escaped character. */
            if (ident[i] == '\\') {
                i++;
            }

            /* Disallow unescaped quote character. */
            else if (ident[i] == ident[0] && ident[i + 1] != ident[0]) {
                ereport(
                    ERROR,
                    errcode(ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH),
                    errmsg_internal("pg_clickhouse: invalid identifier")
                );
            }
        }

        /* Allow already quoted identifier. */
        if (len == 2 || len > CH_ESCAPED_NAMEDATALEN - 1) {
            ereport(
                ERROR,
                errcode(ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH),
                errmsg_internal("pg_clickhouse: invalid identifier")
            );
        }
        return ident;
    }

    /* Rely on PostgreSQL 's identifier quoting. */
    if (len == 0 || len > NAMEDATALEN - 1) {
        ereport(
            ERROR,
            errcode(ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH),
            errmsg_internal("pg_clickhouse: invalid identifier")
        );
    }
    return quote_identifier(ident);
}
