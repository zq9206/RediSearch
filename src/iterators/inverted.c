#include "inverted.h"
#include <rmalloc.h>
#include <math.h>

#define IR_CURRENT_BLOCK(ir) (ir->idx->blocks[ir->currentBlock])

void IR_advanceBlock(IndexReader *ir) {
  ir->currentBlock++;
  ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
  ir->lastId = 0;  // IR_CURRENT_BLOCK(ir).firstId;
}

/** Return 1 if we can read further from the index */
int IR_HasNext(void *ctx) {
  IndexReader *ir = ctx;
  return !ir->atEnd;
}

/* Read an entry from an inverted index into RSIndexResult */
int IR_Read(void *ctx, RSIndexResult **e) {

  IndexReader *ir = ctx;

  int rc;
  BufferReader *br = &ir->br;

  do {
    if (BufferReader_AtEnd(br)) {
      // We're at the end of the last block...
      if (ir->currentBlock + 1 == ir->idx->size) {
        goto eof;
      }
      IR_advanceBlock(ir);
      br = &ir->br;
    }

    InvertedIndex_ReadEntry(br, ir->readFlags, ir->record, ir->singleWordMode);
    ir->lastId = ir->record->docId += ir->lastId;

    // The record doesn't match the field filter. Continue to the next one
    if (!(ir->record->fieldMask & ir->fieldMask)) {
      continue;
    }

    ++ir->len;
    *e = ir->record;
    return INDEXREAD_OK;

  } while (1);
eof:

  // Mark the reader as at EOF and return EOF
  ir->atEnd = 1;

  return INDEXREAD_EOF;
}

RSIndexResult *IR_Current(void *ctx) {
  return ((IndexReader *)ctx)->record;
}

int _isPos(InvertedIndex *idx, uint32_t i, t_docId docId) {
  if (idx->blocks[i].firstId <= docId &&
      (i == idx->size - 1 || idx->blocks[i + 1].firstId > docId)) {
    return 1;
  }
  return 0;
}

int IR_skipToBlock(IndexReader *ir, t_docId docId) {

  InvertedIndex *idx = ir->idx;
  if (idx->size == 0 || docId < idx->blocks[0].firstId) {
    return 0;
  }

  uint32_t top = idx->size, bottom = ir->currentBlock;
  uint32_t i = bottom;
  uint32_t newi;

  while (bottom <= top) {
    if (_isPos(idx, i, docId)) {
      ir->currentBlock = i;
      goto found;
    }

    if (docId < idx->blocks[i].firstId) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }
  ir->currentBlock = i;

found:
  ir->lastId = 0;
  ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
  return 1;
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
int IR_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {
  IndexReader *ir = ctx;

  // printf("IR %s skipTo %d\n", ir->term->str, docId);
  /* If we are skipping to 0, it's just like a normal read */
  if (docId == 0) {
    return IR_Read(ctx, hit);
  }

  /* check if the id is out of range */
  if (docId > ir->idx->lastId) {
    ir->atEnd = 1;
    return INDEXREAD_EOF;
  }
  // try to skip to the current block
  if (!IR_skipToBlock(ir, docId)) {
    if (IR_Read(ir, hit) == INDEXREAD_EOF) {
      return INDEXREAD_EOF;
    }
    return INDEXREAD_NOTFOUND;
  }

  int rc;
  t_docId rid;
  while (INDEXREAD_EOF != (rc = IR_Read(ir, hit))) {
    rid = (*hit)->docId;
    if (ir->lastId < docId) continue;
    if (rid == docId) return INDEXREAD_OK;
    return INDEXREAD_NOTFOUND;
  }
  ir->atEnd = 1;
  return INDEXREAD_EOF;
}

/* The number of docs in an inverted index entry */
size_t IR_NumDocs(void *ctx) {
  IndexReader *ir = ctx;

  // in single word optimized mode we only know the size of the record from
  // the header.
  if (ir->singleWordMode) {
    return ir->idx->numDocs;
  }

  // otherwise we use our counter
  return ir->len;
}

/* Create a new index reader */
IndexReader *NewIndexReader(InvertedIndex *idx, DocTable *docTable, t_fieldMask fieldMask,
                            IndexFlags flags, RSQueryTerm *term, int singleWordMode) {
  IndexReader *ret = rm_malloc(sizeof(IndexReader));
  ret->currentBlock = 0;
  ret->idx = idx;
  ret->term = term;

  if (term) {
    // compute IDF based on num of docs in the header
    ret->term->idf = logb(1.0F + docTable->size / (idx->numDocs ? idx->numDocs : (double)1));
  }

  ret->record = NewTokenRecord(term);
  ret->lastId = 0;
  ret->docTable = docTable;
  ret->len = 0;
  ret->singleWordMode = singleWordMode;
  ret->atEnd = 0;

  ret->fieldMask = fieldMask;
  ret->flags = flags;
  ret->readFlags = (uint32_t)flags & (Index_StoreFieldFlags | Index_StoreTermOffsets);
  ret->br = NewBufferReader(IR_CURRENT_BLOCK(ret).data);
  return ret;
}

void IR_Free(IndexIterator *it) {
  if (it == NULL) {
    return;
  }

  IndexReader *ir = it->ctx;

  IndexResult_Free(ir->record);

  Term_Free(ir->term);
  rm_free(ir);
  rm_free(it);
}

t_docId IR_LastDocId(void *ctx) {
  return ((IndexReader *)ctx)->lastId;
}

IndexIterator *NewReadIterator(IndexReader *ir) {
  IndexIterator *ri = rm_malloc(sizeof(IndexIterator));
  ri->ctx = ir;
  ri->Read = IR_Read;
  ri->SkipTo = IR_SkipTo;
  ri->LastDocId = IR_LastDocId;
  ri->HasNext = IR_HasNext;
  ri->Free = IR_Free;
  ri->Len = IR_NumDocs;
  ri->Current = IR_Current;
  return ri;
}
