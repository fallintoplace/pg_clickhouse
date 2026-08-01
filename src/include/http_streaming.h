#ifndef CLICKHOUSE_HTTP_STREAMING_H
#define CLICKHOUSE_HTTP_STREAMING_H

#include "engine.h"

typedef struct ch_http_connection_t ch_http_connection_t;

/*
 * Opaque handle to a streaming HTTP query. The real type is the HttpStream
 * struct, defined in http_streaming.c.
 */
typedef struct HttpStream HttpStream;

/* lifecycle */
HttpStream*
ch_http_stream_begin(
    ch_http_connection_t* conn,
    const ch_query* query,
    int32 fetch_size,
    bool native
);
void
ch_http_stream_end(HttpStream* stream);

/*
 * pgch_chunk_source next_chunk over the response body. Bytes stay valid until
 * the following call. Sets *len 0 at clean EOF; returns false with *error on
 * transport failure or cancellation. Takes void* so it can be assigned to the
 * callback slot without this header knowing pg-clickhouse-c.
 */
bool
ch_http_stream_next_chunk(void* stream, const void** data, size_t* len, char** error);

/* accessors — let pglink.c read stream state without seeing the struct */
char*
ch_http_stream_buffer(HttpStream* stream);
size_t
ch_http_stream_available(HttpStream* stream);
long
ch_http_stream_status(HttpStream* stream);
const char*
ch_http_stream_query_id(HttpStream* stream);
const char*
ch_http_stream_error(HttpStream* stream);
double
ch_http_stream_request_time(HttpStream* stream);
double
ch_http_stream_total_time(HttpStream* stream);

/*
 * Transfer ownership of the response body to the caller. On return, *out_data
 * is a malloc()'d buffer (or the strdup'd transport error message when status
 * is CH_HTTP_STATUS_TRANSPORT_ERROR) that the caller must free(). Only valid
 * before the first ch_http_stream_next_chunk call, which reuses the buffer.
 * The stream itself is unchanged otherwise and should still be released with
 * ch_http_stream_end().
 */
void
ch_http_stream_take_body(HttpStream* stream, char** out_data, size_t* out_size);

#endif /* CLICKHOUSE_HTTP_STREAMING_H */
