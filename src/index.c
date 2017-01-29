#include "forward_index.h"
#include "index.h"
#include "varint.h"
#include "spec.h"
#include <math.h>
#include <sys/param.h>

IndexIterator *NewIndexIterator(void *ctx) {
  IndexIterator *ret = calloc(1, sizeof(IndexIterator));
  ret->ctx = ctx;
  return ret;
}

inline int IR_HasNext(void *ctx) {
  IndexReader *ir = ctx;
  // LG_DEBUG("ir %p size %d, offset %d. has next? %d\n", ir, ir->header.size,
  // ir->buf->offset,
  // ir->header.size > ir->buf->offset);
  return ir->header.size > ir->buf->offset;
}

inline int IR_GenericRead(IndexReader *ir, t_docId *docId, float *freq, u_char *flags,
                          VarintVector *offsets) {
  if (!IR_HasNext(ir)) {
    return INDEXREAD_EOF;
  }

  *docId = ReadVarint(ir->buf) + ir->lastId;

  int quantizedScore = ReadVarint(ir->buf);
  if (freq != NULL) {
    *freq = (float)(quantizedScore ? quantizedScore : 1) / FREQ_QUANTIZE_FACTOR;
    // printf("READ Quantized score %d, freq %f\n", quantizedScore, *freq);
  }

  if (ir->flags & Index_StoreFieldFlags) {
    BufferReadByte(ir->buf, (char *)flags);
  } else {
    *flags = 0xFF;
  }

  if (ir->flags & Index_StoreTermOffsets) {

    size_t offsetsLen = ReadVarint(ir->buf);

    // If needed - read offset vectors
    if (offsets != NULL && !ir->singleWordMode) {
      offsets->cap = offsetsLen;
      offsets->data = ir->buf->pos;
      offsets->pos = offsets->data;
      offsets->ctx = NULL;
      offsets->offset = 0;
      offsets->type = BUFFER_READ;
    }
    BufferSkip(ir->buf, offsetsLen);
  }
  ir->lastId = *docId;
  return INDEXREAD_OK;
}

inline int IR_TryRead(IndexReader *ir, t_docId *docId, t_docId expectedDocId) {
  if (!IR_HasNext(ir)) {
    return INDEXREAD_EOF;
  }

  *docId = ReadVarint(ir->buf) + ir->lastId;
  ReadVarint(ir->buf);  // read quantized score
  // pseudo-read flags
  if (ir->flags & Index_StoreFieldFlags) {
    BufferSkip(ir->buf, 1);
  }

  // pseudo read offsets
  if (ir->flags & Index_StoreTermOffsets) {
    size_t len = ReadVarint(ir->buf);
    BufferSkip(ir->buf, len);
  }

  ir->lastId = *docId;

  if (*docId != expectedDocId && expectedDocId != 0) {
    return INDEXREAD_NOTFOUND;
  }

  return INDEXREAD_OK;
}

// inline double tfidf(float freq, u_int32_t docFreq) {
//   double idf = logb(
//       1.0F + TOTALDOCS_PLACEHOLDER / (docFreq ? docFreq : (double)1)); // IDF
//   // LG_DEBUG("FREQ: %f  IDF: %.04f, TFIDF: %f",freq, idf, freq*idf);
//   return freq * idf;
// }

int IR_ReadCurrent(void *ctx, IndexResult *e) {
  IndexReader *ir = ctx;

  if (ir->lastId == 0) {
    return INDEXREAD_NOTFOUND;
  }
  IndexResult_PutRecord(e, &ir->currentRecord);
  return INDEXREAD_OK;
}

size_t IR_EstimateCardinality(void *ctx) {
  IndexReader *ir = ctx;
  return (size_t)ir->header.numDocs;
}

int IR_ReadNext(void *ctx, IndexResult *e) {
  float freq;
  IndexReader *ir = ctx;

  if (ir->useScoreIndex && ir->scoreIndex) {
    ScoreIndexEntry *ent = ScoreIndex_Next(ir->scoreIndex);
    if (ent == NULL) {
      return INDEXREAD_EOF;
    }

    IR_Seek(ir, ent->offset, ent->docId);
  }

  VarintVector *offsets = NULL;
  if (!ir->singleWordMode) {
    offsets = &ir->currentRecord.offsets;
  }

  int rc = IR_GenericRead(ir, &ir->currentRecord.docId, &ir->currentRecord.tf,
                          &ir->currentRecord.flags, offsets);

  // add the record to the current result
  if (rc == INDEXREAD_OK) {
    if (!(ir->currentRecord.flags & ir->fieldMask)) {
      return INDEXREAD_NOTFOUND;
    }

    ++ir->len;

    IndexResult_PutRecord(e, &ir->currentRecord);
  }

  // printf("IR %s Read docId %d, rc %d\n", ir->term->str, e->docId, rc);
  return rc;
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
  // LG_DEBUG("Seeking to %d, lastId %d", offset, docId);
  BufferSeek(ir->buf, offset);
  ir->lastId = docId;
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
int IR_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit) {

  /* If we are skipping to 0, it's just like a normal read */
  if (docId == 0) {
    return IR_ReadNext(ctx, hit);
  }

  IndexReader *ir = ctx;
  /* check if the id is out of range */
  if (docId > ir->header.lastId) {
    return INDEXREAD_EOF;
  }
  if (docId == ir->lastId) {
    return IR_ReadCurrent(ctx, hit);
  }

  /* try to find an entry in the skip index if possible */
  SkipEntry *ent = SkipIndex_Find(ir->skipIdx, docId, &ir->skipIdxPos);
  // printf("ent for %d: %d\n", docId, ent ? ent->docId : -1);
  /* Seek to the correct location if we found a skip index entry */
  if (ent != NULL && ent->offset > BufferOffset(ir->buf)) {
    IR_Seek(ir, ent->offset, ent->docId);
  }
  // if (!ent && ir->lastId > docId) {
  //   return INDEXREAD_NOTFOUND;
  // }

  int rc;
  t_docId lastId = ir->lastId, readId = 0;
  t_offset offset = ir->buf->offset;

  do {

    // do a quick-read until we hit or pass the desired document
    if ((rc = IR_TryRead(ir, &readId, docId)) == INDEXREAD_EOF) {
      return rc;
    }
    // rewind 1 document and re-read it...
    if (rc == INDEXREAD_OK || readId > docId) {
      IR_Seek(ir, offset, lastId);
      IR_ReadNext(ir, hit);
      return rc;
    }
    lastId = readId;
    offset = ir->buf->offset;
  } while (rc != INDEXREAD_EOF);

  return INDEXREAD_EOF;
}

size_t IR_NumDocs(void *ctx) {
  IndexReader *ir = ctx;

  // in single word optimized mode we only know the size of the record from
  // the header.
  if (ir->singleWordMode) {
    return ir->header.numDocs;
  }

  // otherwise we use our counter
  return ir->len;
}

IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si, DocTable *dt,
                            int singleWordMode, u_char fieldMask, IndexFlags flags) {
  return NewIndexReaderBuf(NewBuffer(data, datalen, BUFFER_READ), si, dt, singleWordMode, NULL,
                           fieldMask, flags, NULL);
}

IndexReader *NewIndexReaderBuf(Buffer *buf, SkipIndex *si, DocTable *dt, int singleWordMode,
                               ScoreIndex *sci, u_char fieldMask, IndexFlags flags, Term *term) {
  IndexReader *ret = malloc(sizeof(IndexReader));
  ret->buf = buf;
  indexReadHeader(buf, &ret->header);
  ret->term = term;

  if (term) {
    // compute IDF based on num of docs in the header
    ret->term->idf = logb(
        1.0F + TOTALDOCS_PLACEHOLDER / (ret->header.numDocs ? ret->header.numDocs : (double)1));
  }

  ret->currentRecord = (IndexRecord){.term = ret->term};
  ret->lastId = 0;
  ret->skipIdxPos = 0;
  ret->skipIdx = NULL;
  ret->docTable = dt;
  ret->len = 0;
  ret->singleWordMode = singleWordMode;
  // only use score index on single words, no field filter and large entries
  ret->useScoreIndex = 0;
  ret->scoreIndex = NULL;
  if (flags & Index_StoreScoreIndexes) {
    ret->useScoreIndex = sci != NULL && singleWordMode && fieldMask == 0xff &&
                         ret->header.numDocs > SCOREINDEX_DELETE_THRESHOLD;
    ret->scoreIndex = sci;
  }

  // LG_DEBUG("Load offsets %d, si: %p", singleWordMode, si);
  ret->skipIdx = si;
  ret->fieldMask = fieldMask;
  ret->flags = flags;

  return ret;
}

void IR_Free(IndexReader *ir) {
  membufferRelease(ir->buf);
  if (ir->scoreIndex) {
    ScoreIndex_Free(ir->scoreIndex);
  }
  SkipIndex_Free(ir->skipIdx);
  Term_Free(ir->term);
  free(ir);
}

IndexIterator *NewReadIterator(IndexReader *ir) {
  IndexIterator *ri = malloc(sizeof(IndexIterator));
  ri->ctx = ir;
  ri->ReadNext = IR_ReadNext;
  ri->EstimateCardinality = IR_EstimateCardinality;
  ri->ReadCurrent = IR_ReadCurrent;
  ri->SkipTo = IR_SkipTo;
  ri->LastDocId = IR_LastDocId;
  ri->HasNext = IR_HasNext;
  ri->Free = ReadIterator_Free;
  ri->Len = IR_NumDocs;
  return ri;
}

size_t IW_Len(IndexWriter *w) {
  return BufferLen(w->bw.buf);
}

void writeIndexHeader(IndexWriter *w) {
  size_t offset = w->bw.buf->offset;
  BufferSeek(w->bw.buf, 0);
  IndexHeader h = {offset, w->lastId, w->ndocs};
  LG_DEBUG(
      "Writing index header. offest %d , lastId %d, ndocs %d, will seek "
      "to %zd",
      h.size, h.lastId, w->ndocs, offset);
  w->bw.Write(w->bw.buf, &h, sizeof(IndexHeader));
  BufferSeek(w->bw.buf, offset);
}

IndexWriter *NewIndexWriter(size_t cap, IndexFlags flags) {
  return NewIndexWriterBuf(NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE)),
                           NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE)),
                           NewScoreIndexWriter(NewBufferWriter(NewMemoryBuffer(2, BUFFER_WRITE))),
                           flags);
}

IndexWriter *NewIndexWriterBuf(BufferWriter bw, BufferWriter skipIdnexWriter, ScoreIndexWriter siw,
                               IndexFlags flags) {
  IndexWriter *w = malloc(sizeof(IndexWriter));
  w->bw = bw;
  w->skipIndexWriter = skipIdnexWriter;
  w->ndocs = 0;
  w->lastId = 0;
  w->scoreWriter = siw;
  w->flags = flags;

  IndexHeader h = {0, 0, 0};
  if (indexReadHeader(w->bw.buf, &h)) {
    if (h.size > 0) {
      w->lastId = h.lastId;
      w->ndocs = h.numDocs;
      BufferSeek(w->bw.buf, h.size);

      return w;
    }
  }

  writeIndexHeader(w);
  BufferSeek(w->bw.buf, sizeof(IndexHeader));

  return w;
}

int indexReadHeader(Buffer *b, IndexHeader *h) {
  if (b->cap > sizeof(IndexHeader)) {
    BufferSeek(b, 0);
    return BufferRead(b, h, sizeof(IndexHeader));
  }
  return 0;
}

void IW_WriteSkipIndexEntry(IndexWriter *w) {
  SkipEntry se = {w->lastId, BufferOffset(w->bw.buf)};
  Buffer *b = w->skipIndexWriter.buf;

  u_int32_t num = (w->ndocs / SKIPINDEX_STEP);
  size_t off = b->offset;

  BufferSeek(b, 0);
  w->skipIndexWriter.Write(b, &num, sizeof(u_int32_t));

  if (off > 0) {
    BufferSeek(b, off);
  }
  w->skipIndexWriter.Write(b, &se, sizeof(SkipEntry));
}

/* Write a forward-index entry to an index writer */
size_t IW_WriteEntry(IndexWriter *w, ForwardIndexEntry *ent) {
  // VVW_Truncate(ent->vw);
  size_t ret = 0;
  VarintVector *offsets = ent->vw->bw.buf;

  if (w->flags & Index_StoreScoreIndexes) {
    ScoreIndexWriter_AddEntry(&w->scoreWriter, ent->freq, BufferOffset(w->bw.buf), w->lastId);
  }
  // quantize the score to compress it to max 4 bytes
  // freq is between 0 and 1
  int quantizedScore = floorl(ent->freq * ent->docScore * (double)FREQ_QUANTIZE_FACTOR);

  size_t offsetsSz = offsets->offset;
  // // // calculate the overall len
  // size_t len = varintSize(quantizedScore) + 1 + offsetsSz;

  // Write docId
  ret += WriteVarint(ent->docId - w->lastId, &w->bw);
  // encode len

  // ret += WriteVarint(len, &w->bw);
  // encode freq
  ret += WriteVarint(quantizedScore, &w->bw);

  if (w->flags & Index_StoreFieldFlags) {
    // encode flags
    ret += w->bw.Write(w->bw.buf, &ent->flags, 1);
  }

  if (w->flags & Index_StoreTermOffsets) {
    ret += WriteVarint(offsetsSz, &w->bw);
    // write offsets size
    // ret += WriteVarint(offsetsSz, &w->bw);
    ret += w->bw.Write(w->bw.buf, offsets->data, offsetsSz);
  }
  w->lastId = ent->docId;
  if (w->ndocs && w->ndocs % SKIPINDEX_STEP == 0) {
    IW_WriteSkipIndexEntry(w);
  }

  w->ndocs++;
  return ret;
}

size_t IW_Close(IndexWriter *w) {
  // w->bw.Truncate(w->bw.buf, 0);

  // write the header at the beginning
  writeIndexHeader(w);

  return w->bw.buf->cap;
}

void IW_Free(IndexWriter *w) {
  w->skipIndexWriter.Release(w->skipIndexWriter.buf);
  w->scoreWriter.bw.Release(w->scoreWriter.bw.buf);

  w->bw.Release(w->bw.buf);
  free(w);
}

inline t_docId IR_LastDocId(void *ctx) {
  return ((IndexReader *)ctx)->lastId;
}

inline t_docId UI_LastDocId(void *ctx) {
  return ((UnionContext *)ctx)->lastDocId;
}

static int cmpIndexIterators(const void *e1, const void *e2, const void *udata) {
  const IndexIterator *i2 = e1, *i1 = e2;
  // printf("cmp %p<>%p: %d\n", i1, i2, (int)(i1->LastDocId(i1->ctx) - i2->LastDocId(i2->ctx)));
  return (int)(i1->LastDocId(i1->ctx) - i2->LastDocId(i2->ctx));
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *dt) {
  // create union context
  UnionContext *ctx = calloc(1, sizeof(UnionContext));
  ctx->iters = malloc(heap_sizeof(num));
  heap_init(ctx->iters, cmpIndexIterators, NULL, num);
  for (int i = 0; i < num; i++) {
    if (its[i]) {
      heap_offerx(ctx->iters, its[i]);
      ctx->num++;
    }
  }

  ctx->docTable = dt;
  ctx->atEnd = 0;
  ctx->current = NewIndexResult();
  ctx->tmp = NewIndexResult();
  ctx->lastDocId = 0;
  ctx->pos = 0;
  ctx->len = 0;
  // bind the union iterator calls
  IndexIterator *it = NewIndexIterator(ctx);

  it->LastDocId = UI_LastDocId;
  it->ReadNext = UI_ReadNext;
  it->ReadCurrent = UI_ReadCurrent;
  it->EstimateCardinality = UI_EstimateCardinality;
  it->SkipTo = UI_SkipTo;
  it->HasNext = UI_HasNext;
  it->Free = UnionIterator_Free;
  it->Len = UI_Len;
  return it;
}

int UI_ReadCurrent(void *ctx, IndexResult *hit) {
  UnionContext *ui = ctx;

  if (ui->atEnd || heap_count(ui->iters) == 0) {
    return INDEXREAD_EOF;
  }
  if (ui->lastDocId == 0) {
    return INDEXREAD_NOTFOUND;
  }

  IndexResult_Add(hit, &ui->current);
  return INDEXREAD_OK;
}

size_t UI_EstimateCardinality(void *ctx) {
  printf("TODO:UI_EstimateCardinality\n");
  return 0;
}

int UI_ReadNext(void *ctx, IndexResult *hit) {
  UnionContext *ui = ctx;
  // nothing to do
  if (ui->num == 0 || ui->atEnd) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  int n = 0;
  t_docId docId = 0;

  ui->current.numRecords = 0;
  do {
    // read the smallest id iterator
    IndexIterator *it = heap_poll(ui->iters);

    // if the smallest iter is equal or larger than the last read,
    // we're done
    // printf("docId now %d, selected it %p, lastDocId: %d\n", docId, it,
    //        it ? it->LastDocId(it->ctx) : -1);

    if (!it || (it->LastDocId(it->ctx) > docId && docId)) {
      if (it) heap_offerx(ui->iters, it);
      break;
    }

    ui->tmp.numRecords = 0;
    int rc;
    if (!docId && ui->lastDocId < it->LastDocId(it->ctx)) {
      rc = it->ReadCurrent(it->ctx, &ui->tmp);
    } else
      rc = it->ReadNext(it->ctx, &ui->tmp);

    // printf("Read from it %p %d: %d\n", it, ui->tmp.docId, rc);
    if (rc == INDEXREAD_OK) {
      // read one more record to the same docId already read
      if (docId == 0 || ui->tmp.docId == docId) {
        IndexResult_Add(&ui->current, &ui->tmp);
        docId = ui->tmp.docId;
        // we have a docId lower than current - we need to switch current
      } else if (ui->tmp.docId < docId) {
        ui->current.numRecords = 0;
        IndexResult_Add(&ui->current, &ui->tmp);
        docId = ui->tmp.docId;
      }
    }
    if (rc != INDEXREAD_EOF) {
      heap_offerx(ui->iters, it);
    }

  } while (heap_count(ui->iters) > 0);

  if (ui->current.numRecords > 0) {
    ui->len++;
    ui->lastDocId = ui->current.docId;
    if (hit) IndexResult_Add(hit, &ui->current);
    // printf("returning OK docId %d!\n", ui->lastDocId);
    return INDEXREAD_OK;
  } else {  // couldn't read a single record
    // printf("Returning EOF\n");
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }
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
int UI_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit) {
  if (docId == 0) {
    return UI_ReadNext(ctx, hit);
  }
  UnionContext *ui = ctx;
  // printf("UI %p skipto %d\n", ui, docId);

  int sz = heap_count(ui->iters);
  if (sz == 0 || ui->atEnd) {
    return INDEXREAD_EOF;
  }

  IndexIterator *its[sz];
  int itx = 0;
  int n = 0;
  int rc = INDEXREAD_EOF;

  ui->current.numRecords = 0;
  // skip all iterators to docId
  for (int i = 0; i < sz; i++) {
    IndexIterator *it = heap_poll(ui->iters);

    if (it->LastDocId(it->ctx) == docId) {
      if (it->ReadCurrent(it->ctx, &ui->current) == INDEXREAD_OK) {
        n++;
      }
    } else if (it->LastDocId(it->ctx) < docId) {

      ui->tmp.numRecords = 0;
      rc = it->SkipTo(it->ctx, docId, &ui->tmp);
      // printf("read %d, rc %d\n", ui->tmp.docId, rc);
      if (rc == INDEXREAD_EOF) continue;

      if (rc == INDEXREAD_OK) {
        IndexResult_Add(&ui->current, &ui->tmp);
        n++;
      }
    }

    // only if not at EOF - we return it to the heap
    its[itx++] = it;
  }

  // all iterators are at the end
  if (itx == 0) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  // return the iterators to the heap
  for (int i = 0; i < itx; i++) {
    heap_offerx(ui->iters, its[i]);
  }

  IndexIterator *minIt = heap_peek(ui->iters);
  ui->lastDocId = minIt->LastDocId(minIt->ctx);

  if (n > 0) {
    IndexResult_Add(hit, &ui->current);
    return INDEXREAD_OK;
  }

  IndexResult_Add(&ui->current, &ui->tmp);

  // printf("UI %p skipped to docId %d NOT FOUND, minDocId now %d\n", ui, docId, ui->minDocId);
  return INDEXREAD_NOTFOUND;
}

void UnionIterator_Free(IndexIterator *it) {
  if (it == NULL) return;

  UnionContext *ui = it->ctx;
  IndexIterator *cit = NULL;
  while (heap_count(ui->iters) && NULL != (cit = heap_poll(ui->iters))) {
    cit->Free(cit);
  }
  heap_free(ui->iters);
  IndexResult_Free(&ui->current);
  IndexResult_Free(&ui->tmp);
  free(ui);
  free(it);
}

size_t UI_Len(void *ctx) {
  return ((UnionContext *)ctx)->len;
}

void ReadIterator_Free(IndexIterator *it) {
  if (it == NULL) {
    return;
  }

  IR_Free(it->ctx);
  free(it);
}

void IntersectIterator_Free(IndexIterator *it) {
  if (it == NULL) return;
  IntersectContext *ui = it->ctx;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i] != NULL) {
      ui->its[i]->Free(ui->its[i]);
    }
  }

  IndexResult_Free(&ui->current);
  IndexResult_Free(&ui->tmp);
  free(ui->its);
  free(it->ctx);
  free(it);
}

IndexIterator *NewIntersecIterator(IndexIterator **its, int num, int exact, DocTable *dt,
                                   u_char fieldMask) {
  // create context
  IntersectContext *ctx = calloc(1, sizeof(IntersectContext));
  ctx->its = its;
  ctx->num = num;
  ctx->lastDocId = 0;
  ctx->len = 0;
  ctx->exact = exact;
  ctx->fieldMask = fieldMask;
  ctx->atEnd = 0;
  ctx->current = NewIndexResult();
  ctx->tmp = NewIndexResult();
  ctx->docTable = dt;

  // bind the iterator calls
  IndexIterator *it = malloc(sizeof(IndexIterator));
  it->ctx = ctx;
  it->LastDocId = II_LastDocId;
  it->ReadNext = II_Read;

  it->ReadCurrent = II_ReadCurrent;
  it->SkipTo = II_SkipTo;
  it->HasNext = II_HasNext;
  it->Len = II_Len;
  it->Free = IntersectIterator_Free;
  return it;
}

int II_ReadCurrent(void *ctx, IndexResult *hit) {
  IntersectContext *ii = ctx;

  if (ii->atEnd || ii->num == 0) {
    ii->atEnd = 1;
    return INDEXREAD_EOF;
  }
  if (ii->lastDocId == 0) {
    return INDEXREAD_NOTFOUND;
  }

  IndexResult_Add(hit, &ii->current);
  return INDEXREAD_OK;
}
int II_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit) {

  /* A seek with docId 0 is equivalent to a read */
  if (docId == 0) {
    return II_Read(ctx, hit);
  }
  IntersectContext *ic = ctx;

  int nfound = 0;

  int rc = INDEXREAD_EOF;

  ic->current.numRecords = 0;
  // skip all iterators to docId
  for (int i = 0; i < ic->num; i++) {
    IndexIterator *it = ic->its[i];
    rc = INDEXREAD_OK;
    // printf("II: it %d, last docId %d, our own last docId %d\n", i, it->LastDocId(it->ctx),
    //        ic->lastDocId);

    // only read if we're not already at the final position
    if (it->LastDocId(it->ctx) != ic->lastDocId || ic->lastDocId == 0) {
      ic->tmp.numRecords = 0;
      rc = it->SkipTo(it->ctx, docId, &ic->tmp);
    } else {
      // printf("reading current...\n");
      rc = it->ReadCurrent(it->ctx, &ic->tmp);
    }

    if (rc == INDEXREAD_EOF) {
      // we are at the end!
      ic->atEnd = 1;
      return rc;
    } else if (rc == INDEXREAD_OK) {
      // YAY! found!
      ic->lastDocId = docId;
      IndexResult_Add(&ic->current, &ic->tmp);
      ++nfound;
    } else if (it->LastDocId(it->ctx) > ic->lastDocId) {
      ic->lastDocId = it->LastDocId(it->ctx);
      break;
    }
  }

  if (nfound == ic->num) {
    IndexResult_Add(hit, &ic->current);
    return INDEXREAD_OK;
  }

  return INDEXREAD_NOTFOUND;
}

int II_Next(void *ctx) {
  return II_Read(ctx, NULL);
}

int II_Read(void *ctx, IndexResult *hit) {
  IntersectContext *ic = (IntersectContext *)ctx;

  if (ic->num == 0) return INDEXREAD_EOF;

  int nh = 0;
  int i = 0;
  int tr = 0;

  do {
    //    tr++;
    nh = 0;
    for (i = 0; i < ic->num; i++) {

      IndexIterator *it = ic->its[i];
      if (!it) goto eof;

      // printf("Try %d II READ: it %d, last docId %d, our own last docId %d\n", tr, i,
      //        it->LastDocId(it->ctx), ic->lastDocId);

      int rc = INDEXREAD_OK;
      ic->tmp.numRecords = 0;
      if (i == 0) {
        rc = it->ReadNext(it->ctx, &ic->tmp);
      } else {
        // printf("II %d skipping to %d\n", i, ic->lastDocId);
        rc = it->SkipTo(it->ctx, ic->lastDocId, &ic->tmp);
      }
      // printf("read docId %d, rc %d\n", it->LastDocId(it->ctx), rc);

      if (rc == INDEXREAD_EOF) goto eof;
      if (rc == INDEXREAD_NOTFOUND) break;

      ic->lastDocId = it->LastDocId(it->ctx);
      IndexResult_Add(&ic->current, &ic->tmp);
      ++nh;
    }

    if (nh == ic->num) {
      // printf("II %p HIT @ %d\n", ic, ic->lastDocId);
      // sum up all hits

      // hit->numRecords = 0;
      // ic->current.numRecords = 0;
      IndexResult_Add(hit, &ic->current);
    }

    if ((hit->flags & ic->fieldMask) == 0) {
      // printf("Skipping %d\n", hit->docId);
      continue;
    }

    // In exact mode, make sure the minimal distance is the number of words
    if (ic->exact) {
      int md = IndexResult_MinOffsetDelta(hit);

      if (md > ic->num - 1) {
        continue;
      }
    }

    ic->len++;
    return INDEXREAD_OK;
  } while (1);
eof:
  ic->atEnd = 1;
  return INDEXREAD_EOF;
}

int II_HasNext(void *ctx) {
  IntersectContext *ic = ctx;
  // printf("%p %d\n", ic, ic->atEnd);
  return ic->atEnd;
}

t_docId II_LastDocId(void *ctx) {
  return ((IntersectContext *)ctx)->lastDocId;
}

size_t II_Len(void *ctx) {
  return ((IntersectContext *)ctx)->len;
}
