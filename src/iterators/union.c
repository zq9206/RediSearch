#include "union.h"
#include <sys/param.h>

t_docId UI_LastDocId(void *ctx) {
  return ((UnionContext *)ctx)->minDocId;
}

RSIndexResult *UI_Current(void *ctx) {
  return ((UnionContext *)ctx)->current;
}

int UI_Read(void *ctx, RSIndexResult **hit);
int UI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit);

int UI_Read(void *ctx, RSIndexResult **hit) {
  UnionContext *ui = ctx;
  // nothing to do
  if (ui->num == 0 || ui->atEnd) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  int numActive = 0;
  AggregateResult_Reset(ui->current);

  do {

    // find the minimal iterator
    t_docId minDocId = __UINT32_MAX__;
    int minIdx = -1;
    numActive = 0;
    int rc = INDEXREAD_EOF;
    for (int i = 0; i < ui->num; i++) {
      IndexIterator *it = ui->its[i];
      if (it == NULL || !it->HasNext(it->ctx)) continue;
      RSIndexResult *res = it->Current(it->ctx);

      rc = INDEXREAD_OK;
      // if this hit is behind the min id - read the next entry
      // printf("ui->docIds[%d]: %d, ui->minDocId: %d\n", i, ui->docIds[i], ui->minDocId);
      while (ui->docIds[i] <= ui->minDocId && rc != INDEXREAD_EOF) {
        rc = INDEXREAD_NOTFOUND;
        // read while we're not at the end and perhaps the flags do not match
        while (rc == INDEXREAD_NOTFOUND) {
          rc = it->Read(it->ctx, &res);
          ui->docIds[i] = res->docId;
        }
      }

      if (rc != INDEXREAD_EOF) {
        numActive++;
      } else {
        continue;
      }

      if (rc == INDEXREAD_OK && res->docId <= minDocId) {
        minDocId = res->docId;
        minIdx = i;
      }
    }

    // take the minimum entry and collect all results matching to it
    if (minIdx != -1) {

      UI_SkipTo(ui, ui->docIds[minIdx], hit);
      // return INDEXREAD_OK;
      ui->minDocId = ui->docIds[minIdx];
      ui->len++;
      return INDEXREAD_OK;
    }

  } while (numActive > 0);
  ui->atEnd = 1;

  return INDEXREAD_EOF;
}

int UI_Next(void *ctx) {
  // RSIndexResult h = NewIndexResult();
  return UI_Read(ctx, NULL);
}

// return 1 if at least one sub iterator has next
int UI_HasNext(void *ctx) {

  UnionContext *u = ctx;
  return !u->atEnd;
}

/**
Skip to the given docId, or one place after it
@param ctx IndexReader context
@param docId docId to seek to
@param hit an index hit we put our reads into
@return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF
if
at EOF
*/
int UI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {
  UnionContext *ui = ctx;

  // printf("UI %p skipto %d\n", ui, docId);

  if (docId == 0) {
    return UI_Read(ctx, hit);
  }

  if (ui->atEnd) {
    return INDEXREAD_EOF;
  }

  AggregateResult_Reset(ui->current);
  if (docId < ui->minDocId) {
    AggregateResult_Reset((*hit));
    (*hit)->docId = ui->minDocId;
    return INDEXREAD_NOTFOUND;
  }

  int numActive = 0;
  int found = 0;
  int rc = INDEXREAD_EOF;
  const int num = ui->num;
  const int quickExit = ui->quickExit;
  t_docId minDocId = __UINT32_MAX__;
  IndexIterator *it;
  RSIndexResult *res;
  // skip all iterators to docId
  for (int i = 0; i < num; i++) {
    // this happens for non existent words
    if (NULL == (it = ui->its[i])) continue;
    if (!it->HasNext(it->ctx)) continue;

    res = NULL;

    // If the requested docId is larger than the last read id from the iterator,
    // we need to read an entry from the iterator, seeking to this docId
    if (ui->docIds[i] < docId) {
      if ((rc = it->SkipTo(it->ctx, docId, &res)) == INDEXREAD_EOF) {
        continue;
      }
      ui->docIds[i] = res->docId;

    } else {
      // if the iterator is at an end - we avoid reading the entry
      // in this case, we are either past or at the requested docId, no need to actually read
      rc = (ui->docIds[i] == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
    }

    // if we've read successfully, update the minimal docId we've found
    if (ui->docIds[i] && rc != INDEXREAD_EOF) {
      minDocId = MIN(ui->docIds[i], minDocId);
    }

    // we found a hit - continue to all results matching the same docId
    if (rc == INDEXREAD_OK) {

      // add the result to the aggregate result we are holding
      if (hit) {
        AggregateResult_AddChild(ui->current, res ? res : it->Current(it->ctx));
      }
      ui->minDocId = ui->docIds[i];
      ++found;
    }
    ++numActive;
    // If we've found a single entry and we are iterating in quick exit mode - exit now
    if (found && quickExit) break;
  }

  // all iterators are at the end
  if (numActive == 0) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  // copy our aggregate to the upstream hit

  // if we only have one record, we cane just push it upstream not wrapped in our own record,
  // this will speed up evaluating offsets
  if (found == 1 && ui->current->agg.numChildren == 1) {
    *hit = ui->current->agg.children[0];
  } else {
    *hit = ui->current;
  }
  if (found > 0) {
    return INDEXREAD_OK;
  }

  // not found...
  ui->minDocId = minDocId;
  AggregateResult_Reset((*hit));
  (*hit)->docId = ui->minDocId;
  // printf("UI %p skipped to docId %d NOT FOUND, minDocId now %d\n", ui, docId, ui->minDocId);
  return INDEXREAD_NOTFOUND;
}

void UI_Free(IndexIterator *it) {
  if (it == NULL) return;

  UnionContext *ui = it->ctx;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i]) {
      ui->its[i]->Free(ui->its[i]);
    }
  }

  free(ui->docIds);
  IndexResult_Free(ui->current);
  free(ui->its);
  free(ui);
  free(it);
}

size_t UI_Len(void *ctx) {
  return ((UnionContext *)ctx)->len;
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *dt, int quickExit) {
  // create union context
  UnionContext *ctx = calloc(1, sizeof(UnionContext));
  ctx->its = its;
  ctx->num = num;
  ctx->docTable = dt;
  ctx->atEnd = 0;
  ctx->docIds = calloc(num, sizeof(t_docId));
  ctx->current = NewUnionResult(num);
  ctx->len = 0;
  ctx->quickExit = quickExit;
  // bind the union iterator calls
  IndexIterator *it = malloc(sizeof(IndexIterator));
  it->ctx = ctx;
  it->LastDocId = UI_LastDocId;
  it->Current = UI_Current;
  it->Read = UI_Read;
  it->SkipTo = UI_SkipTo;
  it->HasNext = UI_HasNext;
  it->Free = UI_Free;
  it->Len = UI_Len;
  return it;
}
