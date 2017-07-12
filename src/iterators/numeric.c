#include "numeric.h"
#include "union.h"
/* Read the next entry from the iterator, into hit *e.
  *  Returns INDEXREAD_EOF if at the end */
int NR_Read(void *ctx, RSIndexResult **r) {

  NumericRangeIterator *it = ctx;

  if (it->atEOF || it->rng->size == 0) {
    goto eof;
  }

  int match = 0;
  double lastValue = 0;
  do {
    if (it->offset == it->rng->size) {
      goto eof;
    }
    it->lastDocId = it->rng->entries[it->offset].docId;
    // lastValue = it->rng->entries[it->offset].value;
    if (it->nf) {
      match = NumericFilter_Match(it->nf, it->rng->entries[it->offset].value);
    } else {
      match = 1;
    }
    it->offset++;

    // printf("nf %s filter doc %d (%f): %d\n", it->nf->fieldName, it->lastDocId, lastValue, match);
  } while (!match);

  if (match) {
    // match must be true here
    it->rec->docId = it->lastDocId;
    *r = it->rec;

    return INDEXREAD_OK;
  }
eof:
  it->atEOF = 1;
  return INDEXREAD_EOF;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int NR_SkipTo(void *ctx, uint32_t docId, RSIndexResult **r) {

  NumericRangeIterator *it = ctx;

  if (it->atEOF || it->rng->size == 0) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  // If we are seeking beyond our last docId - just declare EOF
  if (docId > it->rng->entries[it->rng->size - 1].docId) {
    it->atEOF = 1;
    it->rec->docId = 0;
    return INDEXREAD_EOF;
  }

  // Find the closest entry to the requested docId
  int top = (int)it->rng->size - 1, bottom = (int)it->offset;
  int i = bottom;

  while (bottom <= top) {
    t_docId did = it->rng->entries[i].docId;
    if (did == docId) {
      break;
    }
    if (docId <= did) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }

  it->offset = i;
  it->lastDocId = it->rng->entries[i].docId;
  it->rec->docId = it->lastDocId;

  // Now read the current entry
  int rc = NR_Read(it, r);

  // EOF or not found are returned as is
  if (rc != INDEXREAD_OK) return rc;

  // if we got ok - check if the read document was the one we wanted or not.
  // If the requested document doesn't match the filter, we should return NOTFOUND
  return it->lastDocId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
}
/* the last docId read */
t_docId NR_LastDocId(void *ctx) {
  return ((NumericRangeIterator *)ctx)->lastDocId;
}

/* can we continue iteration? */
int NR_HasNext(void *ctx) {
  return !((NumericRangeIterator *)ctx)->atEOF;
}

/* release the iterator's context and free everything needed */
void NR_Free(IndexIterator *self) {
  NumericRangeIterator *it = self->ctx;
  IndexResult_Free(it->rec);
  free(self->ctx);
  free(self);
}

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t NR_Len(void *ctx) {
  return ((NumericRangeIterator *)ctx)->rng->size;
}

RSIndexResult *NR_Current(void *ctx) {
  return ((NumericRangeIterator *)ctx)->rec;
}

IndexIterator *NewNumericRangeIterator(NumericRange *nr, NumericFilter *f) {
  IndexIterator *ret = malloc(sizeof(IndexIterator));

  NumericRangeIterator *it = malloc(sizeof(NumericRangeIterator));

  it->nf = NULL;
  // if this range is at either end of the filter, we need to check each record
  if (!NumericFilter_Match(f, nr->minVal) || !NumericFilter_Match(f, nr->maxVal)) {
    it->nf = f;
  }

  it->atEOF = 0;
  it->lastDocId = 0;
  it->offset = 0;
  it->rng = nr;
  it->rec = NewVirtualResult();
  it->rec->fieldMask = RS_FIELDMASK_ALL;
  ret->ctx = it;

  ret->Free = NR_Free;
  ret->Len = NR_Len;
  ret->HasNext = NR_HasNext;
  ret->LastDocId = NR_LastDocId;
  ret->Current = NR_Current;
  ret->Read = NR_Read;
  ret->SkipTo = NR_SkipTo;

  return ret;
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
 * the
 * filter */
IndexIterator *NewNumericFilterIterator(NumericRangeTree *t, NumericFilter *f) {

  Vector *v = NumericRangeTree_Find(t, f->min, f->max);
  if (!v || Vector_Size(v) == 0) {
    // printf("Got no filter vector\n");
    return NULL;
  }

  int n = Vector_Size(v);
  // if we only selected one range - we can just iterate it without union or anything
  if (n == 1) {
    NumericRange *rng;
    Vector_Get(v, 0, &rng);
    IndexIterator *it = NewNumericRangeIterator(rng, f);
    Vector_Free(v);
    return it;
  }

  // We create a  union iterator, advancing a union on all the selected range,
  // treating them as one consecutive range
  IndexIterator **its = calloc(n, sizeof(IndexIterator *));

  for (size_t i = 0; i < n; i++) {
    NumericRange *rng;
    Vector_Get(v, i, &rng);
    if (!rng) {
      continue;
    }

    its[i] = NewNumericRangeIterator(rng, f);
  }
  Vector_Free(v);
  return NewUnionIterator(its, n, NULL, 1);
}
