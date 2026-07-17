/* Adapt HTTP Native response to shared block decoder. */

#ifndef CLICKHOUSE_HTTP_NATIVE_H
#define CLICKHOUSE_HTTP_NATIVE_H

#include "postgres.h"

#include "binary.h" /* pgch_block_source */
#include "http_streaming.h"

typedef struct ch_http_native ch_http_native;

/* Take stream ownership, allocate decode state in memcxt. */
extern ch_http_native*
ch_http_native_begin(HttpStream* stream, MemoryContext memcxt);

extern pgch_block_source
ch_http_native_block_source(ch_http_native* h);

extern bool
ch_http_native_canceled(const ch_http_native* h);

/* NULL after stream release. */
extern const char*
ch_http_native_query_id(const ch_http_native* h);

/* Stop transfer, release stream, keep memcxt-owned decode allocations. */
extern void
ch_http_native_free(ch_http_native* h);

#endif /* CLICKHOUSE_HTTP_NATIVE_H */
