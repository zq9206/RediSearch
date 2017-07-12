#ifndef __GEO_INDEX_H__
#define __GEO_INDEX_H__

#include "redisearch.h"
#include "redismodule.h"
#include "index_result.h"

#include "search_ctx.h"

typedef struct geoIndex {
  RedisSearchCtx *ctx;
  FieldSpec *sp;
} GeoIndex;

int GeoIndex_AddStrings(GeoIndex *gi, t_docId docId, char *slon, char *slat);

typedef struct geoFilter {

  const char *property;
  double lat;
  double lon;
  double radius;
  const char *unit;
} GeoFilter;

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0] */
int GeoFilter_Parse(GeoFilter *gf, RedisModuleString **argv, int argc);
void GeoFilter_Free(GeoFilter *gf);

/* Query the index with the filter, and return a sorted list of docIds from the result of the query.
 * If an error occurred, we return NULL */
t_docId *GeoIndex_Query(GeoIndex *gi, GeoFilter *gf, size_t *num);
#endif