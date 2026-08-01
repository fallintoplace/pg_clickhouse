#ifndef CLICKHOUSE_HTTP_H
#define CLICKHOUSE_HTTP_H

#include "postgres.h"

#include "engine.h"
#include "server_version.h"
#include <curl/curl.h>

#define CH_HTTP_QUERY_ID_LEN 37

/*
 * Synthetic statuses used by the HTTP transport to surface local cancellation
 * and libcurl transport failures through the existing response machinery.
 */
#define CH_HTTP_STATUS_OK 200L
#define CH_HTTP_STATUS_CANCELED 418L
#define CH_HTTP_STATUS_TRANSPORT_ERROR 419L

typedef struct ch_http_connection_t ch_http_connection_t;
typedef struct ch_http_response_t {
    char* data;
    size_t datasize;
    long http_status;
    char query_id[CH_HTTP_QUERY_ID_LEN];
    double pretransfer_time;
    double total_time;
} ch_http_response_t;

void
ch_http_init(int verbose);
long
ch_http_get_verbose(void);
/* Returns NULL and sets *error to a static message on failure. */
ch_http_connection_t*
ch_http_connection(ch_connection_details* details, const char** error);
void
ch_http_close(ch_http_connection_t* conn);
ch_http_response_t*
ch_http_simple_query(
    ch_http_connection_t* conn,
    const ch_query* query,
    ch_cancel_check cancel
);
/*
 * Fetch and cache the server version, returning {0, 0, 0} when it cannot be
 * determined. Only the first call issues a query.
 */
ch_server_version
ch_http_server_version(ch_http_connection_t* conn, ch_cancel_check cancel);

void
ch_http_response_free(ch_http_response_t* resp);

#endif /* CLICKHOUSE_HTTP_H */
