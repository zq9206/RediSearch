#include <index_result.h>
#include "index_iterator.h"
#include <rmalloc.h>

#include "id_list.h"

/* Read the next entry from the iterator, into hit *e.
*  Returns INDEXREAD_EOF if at the end */
int IL_Read(void *ctx, RSIndexResult **r) {
  IdListIterator *it = ctx;

  if (it->offset == it->size) {
    goto eof;
  }
  it->lastDocId = it->docIds[it->offset++];

  // match must be true here
  it->res->docId = it->lastDocId;
  *r = it->res;

  return INDEXREAD_OK;

eof:
  it->atEOF = 1;
  return INDEXREAD_EOF;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int IL_SkipTo(void *ctx, uint32_t docId, RSIndexResult **r) {
  IdListIterator *it = ctx;

  if (docId == 0) {
    return IL_Read(it, r);
  }
  if (it->atEOF || it->size == it->offset) {
    goto eof;
  }
  // If we are seeking beyond our last docId - just declare EOF
  if (docId > it->docIds[it->size - 1]) {
    goto eof;
  }

  // Find the closest entry to the requested docId
  int top = (int)it->size - 1, bottom = (int)it->offset;
  int i = bottom;

  while (bottom <= top) {
    t_docId did = it->docIds[i];
    if (did == docId) {
      break;
    }
    if (docId < did) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }
  if (it->docIds[i] < docId && i < it->size) {
    i++;
  }
  if (i == it->size) goto eof;

  it->lastDocId = it->docIds[i];
  it->res->docId = it->lastDocId;
  it->offset = i + 1;
  *r = it->res;
  // if we got ok - check if the read document was the one we wanted or not.
  // If the requested document doesn't match the filter, we should return NOTFOUND
  return it->lastDocId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
eof:
  it->atEOF = 1;
  it->res->docId = 0;
  return INDEXREAD_EOF;
}

/* the last docId read */
t_docId IL_LastDocId(void *ctx) {
  return ((IdListIterator *)ctx)->lastDocId;
}

/* can we continue iteration? */
int IL_HasNext(void *ctx) {
  return !((IdListIterator *)ctx)->atEOF;
}

RSIndexResult *IL_Current(void *ctx) {
  return ((IdListIterator *)ctx)->res;
}

/* release the iterator's context and free everything needed */
void IL_Free(struct indexIterator *self) {
  IdListIterator *it = self->ctx;
  IndexResult_Free(it->res);
  rm_free(it->docIds);
  rm_free(it);
  rm_free(self);
}

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t IL_Len(void *ctx) {
  return (size_t)((IdListIterator *)ctx)->size;
}

static int cmp_docids(const void *p1, const void *p2) {
  const t_docId *d1 = p1, *d2 = p2;

  return (int)(*d1 - *d2);
}

IndexIterator *NewIdListIterator(t_docId *ids, t_offset num) {

  // first sort the ids, so the caller will not have to deal with it
  qsort(ids, (size_t)num, sizeof(t_docId), cmp_docids);
  IdListIterator *it = rm_new(IdListIterator);

  it->size = num;
  it->docIds = rm_calloc(num, sizeof(t_docId));
  if (num > 0) memcpy(it->docIds, ids, num * sizeof(t_docId));

  it->atEOF = 0;
  it->lastDocId = 0;
  it->res = NewVirtualResult();
  it->res->fieldMask = RS_FIELDMASK_ALL;

  it->offset = 0;

  IndexIterator *ret = rm_new(IndexIterator);
  ret->ctx = it;
  ret->Free = IL_Free;
  ret->HasNext = IL_HasNext;
  ret->LastDocId = IL_LastDocId;
  ret->Len = IL_Len;
  ret->Read = IL_Read;
  ret->Current = IL_Current;
  ret->SkipTo = IL_SkipTo;
  return ret;
}
