#include "not.h"

void NI_Free(IndexIterator *it) {

  NotContext *nc = it->ctx;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  IndexResult_Free(nc->current);
  free(it->ctx);
  free(it);
}

/* SkipTo for NOT iterator. If we have a match - return NOTFOUND. If we don't or we're at the end -
 * return OK */
int NI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {

  NotContext *nc = ctx;
  // If we don't have a child it means the sub iterator is of a meaningless expression.
  // So negating it means we will always return OK!
  if (!nc->child) {
    goto ok;
  }
  nc->lastDocId = nc->child->LastDocId(nc->child->ctx);

  // if the child's iterator is ahead of the current docId, we can assume the docId is not there and
  // return a pseudo okay
  if (nc->lastDocId > docId) {
    goto ok;
  }

  // if the last read docId is the one we are looking for, it's an anti match!
  if (nc->lastDocId == docId) {
    return INDEXREAD_NOTFOUND;
  }

  // read the next entry
  int rc = nc->child->SkipTo(nc->child->ctx, docId, hit);

  // OK means not found
  if (rc == INDEXREAD_OK) {
    return INDEXREAD_NOTFOUND;
  }

ok:
  // NOT FOUND or end means OK. We need to set the docId on the hit we will bubble up
  nc->current->docId = docId;
  *hit = nc->current;
  return INDEXREAD_OK;
}

/* Read has no meaning in the sense of a NOT iterator, so we just return EOF */
int NI_Read(void *ctx, RSIndexResult **hit) {
  return INDEXREAD_EOF;
}

/* We always have next, in case anyone asks... ;) */
int NI_HasNext(void *ctx) {
  return 1;
}

/* Return the current hit */
RSIndexResult *NI_Current(void *ctx) {
  NotContext *nc = ctx;
  return nc->current;
}

/* Our len is the child's len? TBD it might be better to just return 0 */
size_t NI_Len(void *ctx) {
  NotContext *nc = ctx;
  return nc->child ? nc->child->Len(nc->child->ctx) : 0;
}

/* Last docId */
t_docId NI_LastDocId(void *ctx) {
  NotContext *nc = ctx;

  return nc->lastDocId;
}

IndexIterator *NewNotIterator(IndexIterator *it) {

  NotContext *nc = malloc(sizeof(*nc));
  nc->current = NewVirtualResult();
  nc->current->fieldMask = RS_FIELDMASK_ALL;
  nc->child = it;
  nc->lastDocId = 0;

  IndexIterator *ret = malloc(sizeof(*it));
  ret->ctx = nc;
  ret->Current = NI_Current;
  ret->Free = NI_Free;
  ret->HasNext = NI_HasNext;
  ret->LastDocId = NI_LastDocId;
  ret->Len = NI_Len;
  ret->Read = NI_Read;
  ret->SkipTo = NI_SkipTo;
  return ret;
}
