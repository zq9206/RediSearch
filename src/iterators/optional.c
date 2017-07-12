#include "optional.h"
/**********************************************************
 * Optional clause iterator
 **********************************************************/

void OI_Free(IndexIterator *it) {

  OptionalMatchContext *nc = it->ctx;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  IndexResult_Free(nc->virt);
  free(it->ctx);
  free(it);
}

/* SkipTo for NOT iterator. If we have a match - return NOTFOUND. If we don't or we're at the end -
 * return OK */
int OI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {

  OptionalMatchContext *nc = ctx;
  // If we don't have a child it means the sub iterator is of a meaningless expression.
  // So negating it means we will always return OK!
  if (!nc->child) {
    goto ok;
  }
  RSIndexResult *res = nc->child->Current(nc->child->ctx);
  // if the child's current is already at our docId - just copy it to our current and hit's
  if (docId == (nc->lastDocId = res->docId)) {
    *hit = nc->current = res;
    return INDEXREAD_OK;
  }
  // read the next entry from the child
  int rc = nc->child->SkipTo(nc->child->ctx, docId, &nc->current);

  // OK means ok - pass the entry with the value
  if (rc == INDEXREAD_OK) {
    *hit = nc->current;
    return INDEXREAD_OK;
  }

ok:
  nc->current = nc->virt;
  // NOT FOUND or end means OK. We need to set the docId on the hit we will bubble up
  nc->lastDocId = nc->current->docId = docId;
  *hit = nc->current;
  return INDEXREAD_OK;
}

/* Read has no meaning in the sense of an OPTIONAL iterator, so we just read the next record from
 * our child */
int OI_Read(void *ctx, RSIndexResult **hit) {
  OptionalMatchContext *nc = ctx;
  if (nc->child) {
    if (nc->child->Read(nc->child->ctx, &nc->current) == INDEXREAD_OK) {
      if (hit) {
        *hit = nc->current;
      }
      return INDEXREAD_OK;
    }
  }
  return INDEXREAD_EOF;
}

/* We always have next, in case anyone asks... ;) */
int OI_HasNext(void *ctx) {
  return 1;
}

/* Return the current hit */
RSIndexResult *OI_Current(void *ctx) {
  OptionalMatchContext *nc = ctx;
  return nc->current;
}

/* Our len is the child's len? TBD it might be better to just return 0 */
size_t OI_Len(void *ctx) {
  OptionalMatchContext *nc = ctx;
  return nc->child ? nc->child->Len(nc->child->ctx) : 0;
}

/* Last docId */
t_docId OI_LastDocId(void *ctx) {
  OptionalMatchContext *nc = ctx;

  return nc->lastDocId;
}

IndexIterator *NewOptionalIterator(IndexIterator *it) {

  OptionalMatchContext *nc = malloc(sizeof(*nc));
  nc->virt = NewVirtualResult();
  nc->virt->fieldMask = RS_FIELDMASK_ALL;
  nc->current = nc->virt;
  nc->child = it;
  nc->lastDocId = 0;

  IndexIterator *ret = malloc(sizeof(*it));
  ret->ctx = nc;
  ret->Current = OI_Current;
  ret->Free = OI_Free;
  ret->HasNext = OI_HasNext;
  ret->LastDocId = OI_LastDocId;
  ret->Len = OI_Len;
  ret->Read = OI_Read;
  ret->SkipTo = OI_SkipTo;
  return ret;
}
