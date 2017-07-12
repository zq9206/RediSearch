#ifndef RS_OPTIONAL_ITERATOR_H_
#define RS_OPTIONAL_ITERATOR_H_
#include "index_iterator.h"

typedef struct {
  IndexIterator *child;
  RSIndexResult *virt;
  RSIndexResult *current;
  t_fieldMask fieldMask;
  t_docId lastDocId;
} OptionalMatchContext;

/* Create a NOT iterator by wrapping another index iterator */
IndexIterator *NewOptionalIterator(IndexIterator *it);

#endif