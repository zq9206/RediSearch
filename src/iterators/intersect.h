#ifndef RS_INTERSECT_ITERATOR_H_
#define RS_INTERSECT_ITERATOR_H_
#include <doc_table.h>
#include <redisearch.h>
#include "index_iterator.h"

/* The context used by the intersection methods during iterating an intersect
 * iterator */
typedef struct {
  IndexIterator **its;
  t_docId *docIds;
  int *rcs;
  RSIndexResult *current;
  int num;
  size_t len;
  int maxSlop;
  int inOrder;
  t_docId lastDocId;

  // RSIndexResult *result;
  DocTable *docTable;
  t_fieldMask fieldMask;
  int atEnd;
} IntersectContext;

/* Create a new intersect iterator over the given list of child iterators. If maxSlop is not a
 * negative number, we will allow at most maxSlop intervening positions between the terms. If
 * maxSlop is set and inOrder is 1, we assert that the terms are in
 * order. I.e anexact match has maxSlop of 0 and inOrder 1.  */
IndexIterator *NewIntersecIterator(IndexIterator **its, int num, DocTable *t, t_fieldMask fieldMask,
                                   int maxSlop, int inOrder);

#endif
