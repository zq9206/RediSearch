#ifndef RS_GEO_ITERATOR_H_
#define RS_GEO_ITERATOR_H_

#include "index_iterator.h"
#include <geo_index.h>

IndexIterator *NewGeoRangeIterator(GeoIndex *gi, GeoFilter *gf);

#endif