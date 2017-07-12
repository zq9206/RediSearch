#ifndef RS_NOT_ITERATOR_H_
#define RS_NOT_ITERATOR_H_

#include "index_iterator.h"

/* A Not iterator works by wrapping another iterator, and returning OK for misses, and NOTFOUND for
 * hits */
typedef struct {
  IndexIterator *child;
  RSIndexResult *current;
  t_docId lastDocId;
} NotContext;

/* Create an Optional clause iterator by wrapping another index iterator. An optional iterator
 * always returns OK on skips, but a virtual hit with frequency of 0 if there is no hit */
IndexIterator *NewNotIterator(IndexIterator *it);

#endif