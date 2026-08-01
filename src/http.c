#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <http.h>
#include <http_streaming.h>
#include <internal.h>

static long curl_verbose     = 0;
static bool curl_initialized = false;

void
ch_http_init(int verbose) {
    curl_verbose = verbose;

    if (!curl_initialized) {
        curl_initialized = true;
        curl_global_init(CURL_GLOBAL_ALL);
    }
}

long
ch_http_get_verbose(void) {
    return curl_verbose;
}

#define CLICKHOUSE_PORT 8123
#define CLICKHOUSE_TLS_PORT 8443
#define HTTP_TLS_PORT 443

/*
 * Map the min_tls_version option to a CURLOPT_SSLVERSION value, which libcurl
 * treats as the minimum acceptable version. Returns CURL_SSLVERSION_DEFAULT to
 * leave curl's default (no minimum forced).
 */
static long
curl_min_tls_version(tls_version v) {
    switch (v) {
    case CH_TLS_V1_0:
        return CURL_SSLVERSION_TLSv1_0;
    case CH_TLS_V1_1:
        return CURL_SSLVERSION_TLSv1_1;
    case CH_TLS_V1_2:
        return CURL_SSLVERSION_TLSv1_2;
    case CH_TLS_V1_3:
        return CURL_SSLVERSION_TLSv1_3;
    default:
        return CURL_SSLVERSION_DEFAULT;
    }
}

ch_http_connection_t*
ch_http_connection(ch_connection_details* details, const char** error) {
    CURLU* cu  = NULL;
    char* host = details->host;
    int port   = details->port;
    char port_buf[12];

    ch_http_connection_t* conn = calloc(1, sizeof(ch_http_connection_t));

    *error = "out of memory";
    if (!conn) {
        return NULL;
    }

    conn->ssl_version = curl_min_tls_version(details->min_tls_version);

    if (details->dbname) {
        conn->dbname = strdup(details->dbname);
        if (conn->dbname == NULL) {
            goto cleanup;
        }
    }

    if (!host || !*host) {
        host = "localhost";
    }

    bool use_tls;

    switch (details->tls) {
    case CH_TLS_ON:
        if (!port) {
            port = CLICKHOUSE_TLS_PORT;
        }
        use_tls = true;
        break;
    case CH_TLS_OFF:
        if (!port) {
            port = CLICKHOUSE_PORT;
        }
        use_tls = false;
        break;
    default: /* CH_TLS_AUTO */
        if (!port) {
            port = ch_is_cloud_host(host) ? CLICKHOUSE_TLS_PORT : CLICKHOUSE_PORT;
        }
        use_tls = (port == CLICKHOUSE_TLS_PORT || port == HTTP_TLS_PORT);
        break;
    }

    snprintf(port_buf, sizeof(port_buf), "%d", port);

    cu = curl_url();
    if (cu == NULL) {
        goto cleanup;
    }

    /* Credentials go in as components so curl escapes them for us. */
    *error = "could not build ClickHouse URL";
    if (curl_url_set(cu, CURLUPART_SCHEME, use_tls ? "https" : "http", 0) !=
            CURLUE_OK ||
        curl_url_set(cu, CURLUPART_HOST, host, 0) != CURLUE_OK ||
        curl_url_set(cu, CURLUPART_PORT, port_buf, 0) != CURLUE_OK ||
        curl_url_set(cu, CURLUPART_PATH, "/", 0) != CURLUE_OK) {
        goto cleanup;
    }

    if (details->username) {
        if (curl_url_set(cu, CURLUPART_USER, details->username, CURLU_URLENCODE) !=
            CURLUE_OK) {
            goto cleanup;
        }

        if (details->password &&
            curl_url_set(cu, CURLUPART_PASSWORD, details->password, CURLU_URLENCODE) !=
                CURLUE_OK) {
            goto cleanup;
        }
    }

    if (curl_url_get(cu, CURLUPART_URL, &conn->base_url, 0) != CURLUE_OK) {
        goto cleanup;
    }

    curl_url_cleanup(cu);
    return conn;

cleanup:
    curl_url_cleanup(cu);
    free(conn->dbname);
    free(conn);

    return NULL;
}

/*
 * ch_http_simple_query — buffer the full TabSeparated response in memory.
 *
 * Server default format is TabSeparated, so only its dialect needs pinning:
 * ISO timestamps, \N for NULL and LF line ends, as the text parsers expect.
 */
ch_http_response_t*
ch_http_simple_query(
    ch_http_connection_t* conn,
    const ch_query* query,
    ch_cancel_check cancel
) {
    static const ch_setting tsv_overrides[] = {
        { "date_time_output_format",            "iso" },
        { "format_tsv_null_representation",     "\\N" },
        { "output_format_tsv_crlf_end_of_line", "0"   },
    };
    const ch_http_request req = { .query         = query,
                                  .overrides     = tsv_overrides,
                                  .num_overrides = lengthof(tsv_overrides),
                                  .cancel        = cancel };
    HttpStream* stream;
    ch_http_response_t* resp;

    stream = ch_http_stream_begin(conn, &req);
    if (stream == NULL) {
        return NULL;
    }

    resp = calloc(1, sizeof(*resp));
    if (resp == NULL) {
        ch_http_stream_end(stream);
        return NULL;
    }

    resp->http_status      = ch_http_stream_status(stream);
    resp->pretransfer_time = ch_http_stream_request_time(stream) / 1000.0;
    resp->total_time       = ch_http_stream_total_time(stream) / 1000.0;
    memcpy(resp->query_id, ch_http_stream_query_id(stream), CH_HTTP_QUERY_ID_LEN);
    ch_http_stream_take_body(stream, &resp->data, &resp->datasize);

    if (curl_verbose && resp->http_status != CH_HTTP_STATUS_OK && resp->data) {
        fprintf(stderr, "%s", resp->data);
    }

    ch_http_stream_end(stream);
    return resp;
}

/*
 * Fetches and caches the ClickHouse server version via SELECT version().
 * Returns zeros when the version cannot be determined; a failed lookup counts
 * as fetched, so it is not retried and its warning is raised once.
 */
ch_server_version
ch_http_server_version(ch_http_connection_t* conn, ch_cancel_check cancel) {
    ch_server_version none = { 0, 0, 0 };

    if (conn == NULL) {
        return none;
    }

    if (!conn->version_fetched) {
        ch_query query           = { .sql = "SELECT version()" };
        ch_http_response_t* resp = ch_http_simple_query(conn, &query, cancel);

        conn->version_fetched = true;
        if (resp != NULL) {
            if (resp->http_status == CH_HTTP_STATUS_OK && resp->data != NULL) {
                int parsed, v_tweak;
                char buf[32];
                size_t n =
                    resp->datasize < sizeof(buf) - 1 ? resp->datasize : sizeof(buf) - 1;

                memcpy(buf, resp->data, n);
                buf[n] = '\0';

                /* Parse `major.minor.patch.tweak` from `version()` output. */
                parsed = sscanf(
                    buf,
                    "%d.%d.%d.%d",
                    &conn->version.major,
                    &conn->version.minor,
                    &conn->version.patch,
                    &v_tweak
                );
                if (parsed < 4) {
                    elog(
                        WARNING,
                        "pg_clickhouse: unexpected ClickHouse version() output \"%s\"",
                        buf
                    );
                }
                if (parsed < 2) {
                    /* Version string probably trash; zero out. */
                    conn->version = none;
                }
            } else if (resp->http_status != CH_HTTP_STATUS_OK) {
                elog(
                    WARNING,
                    "pg_clickhouse: SELECT version() failed (HTTP status %d): %.*s",
                    (int)resp->http_status,
                    resp->data ? (int)resp->datasize : 0,
                    resp->data ? resp->data : ""
                );
            }
            ch_http_response_free(resp);
        }
    }

    return conn->version;
}

void
ch_http_close(ch_http_connection_t* conn) {
    curl_free(conn->base_url);
    free(conn->dbname);
    free(conn);
}

void
ch_http_response_free(ch_http_response_t* resp) {
    if (resp->data) {
        free(resp->data);
    }

    free(resp);
}
