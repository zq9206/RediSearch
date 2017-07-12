#ifndef RS_NUMERIC_ITERATOR_H_
#define RS_NUMERIC_ITERATOR_H_

#include <numeric_index.h>
#include <redisearch.h>
#include "index_iterator.h"

/* NumericRangeIterator is the index iterator responsible for iterating a single numeric range. When
 * we perform a query we union multiple such ranges */
typedef struct {
  NumericRange *rng;
  NumericFilter *nf;
  t_docId lastDocId;
  u_int offset;
  int atEOF;
  RSIndexResult *rec;

} NumericRangeIterator;

/* Read the next entry from the iterator, into hit *e.
  *  Returns INDEXREAD_EOF if at the end */
int NR_Read(void *ctx, RSIndexResult **e);

RSIndexResult *NR_Current(void *ctx);

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int NR_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit);

/* the last docId read */
t_docId NR_LastDocId(void *ctx);

/* can we continue iteration? */
int NR_HasNext(void *ctx);

struct indexIterator;
/* release the iterator's context and free everything needed */
void NR_Free(struct indexIterator *self);

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t NR_Len(void *ctx);

struct indexIterator *NewNumericRangeIterator(NumericRange *nr, NumericFilter *f);

struct indexIterator *NewNumericFilterIterator(NumericRangeTree *t, NumericFilter *f);

#endif