#ifndef RS_UNION_ITERATOR_H_
#define RS_UNION_ITERATOR_H_

#include <redisearch.h>
#include <doc_table.h>
#include "index_iterator.h"

/* UnionContext is used during the running of a union iterator */
typedef struct {
  IndexIterator **its;
  t_docId *docIds;
  int num;
  int pos;
  size_t len;
  t_docId minDocId;
  RSIndexResult *current;
  DocTable *docTable;
  int atEnd;
  // If set to 1, we exit skips after the first hit found and not merge further results
  int quickExit;
} UnionContext;

/* Create a new UnionIterator over a list of underlying child iterators.
It will return each document of the underlying iterators, exactly once */
IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *t, int quickExit);


#endif