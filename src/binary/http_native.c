/* Adapt HTTP Native blocks to shared ClickHouse-to-PostgreSQL row decoder. */

#include "postgres.h"

#include <string.h>

#include "miscadmin.h"
#include "utils/palloc.h"

#include "binary_internal.h"
#include "http_native.h"

struct ch_http_native {
    MemoryContext memcxt;
    MemoryContextCallback free_cb;
    HttpStream* stream;

    chc_io io;
    chc_in* in;
    chc_block_opts opts;

    bool eof;
    bool canceled;
    char* error;
};

static int
native_io_read(void* ud, void* buf, size_t len, size_t* out_n, chc_err* err) {
    HttpStream* s = (HttpStream*)ud;

    if (ch_http_stream_read(s, buf, len, out_n) < 0) {
        snprintf(err->msg, sizeof(err->msg), "HTTP transport error");
        return CHC_ERR_IO;
    }
    return CHC_OK;
}

static int
native_io_check_cancel(void* ud pg_attribute_unused()) {
    return (QueryCancelPending || ProcDiePending) ? 1 : 0;
}

static void
native_set_error(ch_http_native* h, const char* msg) {
    if (h->error) {
        return;
    }
    h->error = pstrdup(msg && *msg ? msg : "native block decode failed");
    h->eof   = true;
}

/* Keep decoded block outside transient per-row context. */
static const chc_block*
native_src_next_block(void* ud) {
    ch_http_native* h = (ch_http_native*)ud;
    MemoryContext old;
    chc_block* blk = NULL;
    chc_err err    = {};
    int rc;

    if (h->eof || h->error) {
        return NULL;
    }

    old = MemoryContextSwitchTo(h->memcxt);
    rc  = chc_block_read(h->in, &pgch_alloc, &h->opts, &blk, &err);

    if (rc != CHC_OK) {
        if (rc == CHC_ERR_CANCELLED || QueryCancelPending || ProcDiePending) {
            h->canceled = true;
        }
        native_set_error(h, err.msg);
        blk = NULL;
    } else if (!blk) {
        h->eof = true; /* clean EOF at a block boundary */
    } else {
        for (size_t i = 0; i < chc_block_n_columns(blk); i++) {
            chc_err verr = {};

            if (chc_column_validate(chc_block_column(blk, i), &verr) != CHC_OK) {
                native_set_error(h, verr.msg);
                chc_block_destroy(blk, &pgch_alloc);
                blk = NULL;
                break;
            }
        }
    }

    MemoryContextSwitchTo(old);
    return blk;
}

static const char*
native_src_error(void* ud) {
    return ((ch_http_native*)ud)->error;
}

static void
native_ctx_free_cb(void* arg) {
    ch_http_native_free((ch_http_native*)arg);
}

ch_http_native*
ch_http_native_begin(HttpStream* stream, MemoryContext memcxt) {
    ch_http_native* h = palloc0(sizeof(*h));
    chc_err err       = {};

    h->memcxt          = memcxt;
    h->stream          = stream;
    h->io.ud           = stream;
    h->io.read         = native_io_read;
    h->io.write        = NULL;
    h->io.check_cancel = native_io_check_cancel;
    h->opts =
        (chc_block_opts){ .has_block_info = false, .has_custom_serialization = false };

    /* Close malloc-backed stream on any cursor-context reset. */
    h->free_cb.func = native_ctx_free_cb;
    h->free_cb.arg  = h;
    MemoryContextRegisterResetCallback(memcxt, &h->free_cb);

    h->in = pgch_in_alloc();
    if (chc_in_init(h->in, &h->io, &pgch_alloc, 0, &err) != CHC_OK) {
        native_set_error(h, err.msg[0] ? err.msg : "native reader init failed");
    }
    return h;
}

pgch_block_source
ch_http_native_block_source(ch_http_native* h) {
    return (pgch_block_source){
        .ud         = h,
        .next_block = native_src_next_block,
        .error      = native_src_error,
    };
}

bool
ch_http_native_canceled(const ch_http_native* h) {
    return h->canceled;
}

const char*
ch_http_native_query_id(const ch_http_native* h) {
    return h->stream ? ch_http_stream_query_id(h->stream) : NULL;
}

void
ch_http_native_free(ch_http_native* h) {
    if (!h) {
        return;
    }
    /* Decode allocations belong to memcxt. */
    if (h->stream) {
        ch_http_stream_end(h->stream);
        h->stream = NULL;
    }
}
