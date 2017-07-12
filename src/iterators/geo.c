#include "geo.h"
#include "id_list.h"
#include <rmalloc.h>

IndexIterator *NewGeoRangeIterator(GeoIndex *gi, GeoFilter *gf) {
  size_t sz;
  t_docId *docIds = GeoIndex_Query(gi, gf, &sz);
  if (!docIds) {
    return NULL;
  }

  IndexIterator *ret = NewIdListIterator(docIds, (t_offset)sz);
  rm_free(docIds);
  return ret;
}
