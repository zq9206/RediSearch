#include "inverted_index.h"
#include "math.h"
#include "varint.h"
#include <stdio.h>
#include "rmalloc.h"
#include "qint.h"

#define INDEX_BLOCK_SIZE 100
#define INDEX_BLOCK_INITIAL_CAP 2

#define INDEX_LAST_BLOCK(idx) (idx->blocks[idx->size - 1])

size_t readEntry(BufferReader *__restrict__ br, IndexFlags idxflags, RSIndexResult *res,
                 int singleWordMode);

size_t writeEntry(BufferWriter *bw, IndexFlags idxflags, t_docId docId, t_fieldMask fieldMask,
                  uint32_t freq, uint32_t offsetsSz, RSOffsetVector *offsets);

void InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId) {

  idx->size++;
  idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  idx->blocks[idx->size - 1] = (IndexBlock){.firstId = firstId, .lastId = 0, .numDocs = 0};
  INDEX_LAST_BLOCK(idx).data = NewBuffer(INDEX_BLOCK_INITIAL_CAP);
}

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock) {
  InvertedIndex *idx = rm_malloc(sizeof(InvertedIndex));
  idx->blocks = NULL;
  idx->size = 0;
  idx->lastId = 0;
  idx->flags = flags;
  idx->numDocs = 0;
  if (initBlock) {
    InvertedIndex_AddBlock(idx, 0);
  }
  return idx;
}

void indexBlock_Free(IndexBlock *blk) {
  Buffer_Free(blk->data);
  free(blk->data);
}

void InvertedIndex_Free(void *ctx) {
  InvertedIndex *idx = ctx;
  for (uint32_t i = 0; i < idx->size; i++) {
    indexBlock_Free(&idx->blocks[i]);
  }
  rm_free(idx->blocks);
  rm_free(idx);
}

size_t writeEntry(BufferWriter *bw, IndexFlags idxflags, t_docId docId, t_fieldMask fieldMask,
                  uint32_t freq, uint32_t offsetsSz, RSOffsetVector *offsets) {
  size_t sz = 0;
  switch (idxflags & (Index_StoreFieldFlags | Index_StoreTermOffsets)) {
    // Full encoding - docId, freq, flags, offset
    case Index_StoreTermOffsets | Index_StoreFieldFlags:
      sz = qint_encode4(bw, docId, (uint32_t)freq, (uint32_t)fieldMask, (uint32_t)offsetsSz);
      sz += Buffer_Write(bw, offsets->data, offsetsSz);
      break;

    // Store term offsets but not field flags
    case Index_StoreTermOffsets:
      sz = qint_encode3(bw, docId, (uint32_t)freq, (uint32_t)offsetsSz);
      sz += Buffer_Write(bw, offsets->data, offsetsSz);
      break;

    // Store field mask but not term offsets
    case Index_StoreFieldFlags:
      sz = qint_encode3(bw, docId, (uint32_t)freq, (uint32_t)fieldMask);
      break;
    // Store neither -we store just freq and docId
    default:
      sz = qint_encode2(bw, docId, (uint32_t)freq);
      break;
  }

  return sz;
}

/* Write a forward-index entry to an index writer */
size_t InvertedIndex_WriteEntry(InvertedIndex *idx,
                                ForwardIndexEntry *ent) {  // VVW_Truncate(ent->vw);

  // printf("writing %s docId %d, lastDocId %d\n", ent->term, ent->docId, idx->lastId);
  IndexBlock *blk = &INDEX_LAST_BLOCK(idx);

  // see if we need to grow the current block
  if (blk->numDocs >= INDEX_BLOCK_SIZE) {
    InvertedIndex_AddBlock(idx, ent->docId);
    blk = &INDEX_LAST_BLOCK(idx);
  }
  // // this is needed on the first block
  if (blk->firstId == 0) {
    blk->firstId = ent->docId;
  }
  size_t ret = 0;

  RSOffsetVector offsets = (RSOffsetVector){ent->vw->bw.buf->data, ent->vw->bw.buf->offset};

  BufferWriter bw = NewBufferWriter(blk->data);

  ret = writeEntry(&bw, idx->flags, ent->docId - blk->lastId, ent->fieldMask, ent->freq,
                   offsets.len, &offsets);

  idx->lastId = ent->docId;
  blk->lastId = ent->docId;
  ++blk->numDocs;
  ++idx->numDocs;

  return ret;
}



inline size_t InvertedIndex_ReadEntry(BufferReader *restrict br, IndexFlags idxflags,
                                      RSIndexResult *res, int singleWordMode) {

  size_t startPos = BufferReader_Offset(br);

  switch ((uint32_t)idxflags) {
    // Full encoding - load docId, freq, flags, offset
    case Index_StoreTermOffsets | Index_StoreFieldFlags:

      // size_t sz =
      //     streamvbyte_decode((const uint8_t *)BufferReader_Current(br), arr, 4);
      qint_decode(br, (uint32_t *)res, 4);
      res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
      Buffer_Skip(br, res->offsetsSz);

      break;

    // load term offsets but not field flags
    case Index_StoreTermOffsets:
      qint_decode3(br, &res->docId, &res->freq, &res->offsetsSz);
      res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
      Buffer_Skip(br, res->offsetsSz);
      break;

    // Load field mask but not term offsets
    case Index_StoreFieldFlags:
      qint_decode(br, (uint32_t *)res, 3);
      break;

    // Load neither -we load just freq and docId
    default:
      qint_decode(br, (uint32_t *)res, 2);
      break;
  }

  return BufferReader_Offset(br) - startPos;
}

typedef struct {
  InvertedIndex *idx;
  uint32_t currentBlock;
  DocTable *docs;
  int numRepaired;

} RepairContext;

int IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags) {
  t_docId lastReadId = 0;
  blk->lastId = 0;
  Buffer repair = *blk->data;
  repair.offset = 0;

  BufferReader br = NewBufferReader(blk->data);
  BufferWriter bw = NewBufferWriter(&repair);

  RSIndexResult *res = NewTokenRecord(NULL);
  int frags = 0;

  while (!BufferReader_AtEnd(&br)) {
    size_t sz = readEntry(&br, flags & (Index_StoreFieldFlags | Index_StoreTermOffsets), res, 0);
    lastReadId = res->docId += lastReadId;
    RSDocumentMetadata *md = DocTable_Get(dt, res->docId);

    if (md->flags & Document_Deleted) {
      frags += 1;
      // printf("ignoring hole in doc %d, frags now %d\n", docId, frags);
    } else {

      if (frags) {
        // printf("Writing entry %d, last read id %d, last blk id %d\n", docId, lastReadId,
        //        blk->lastId);
        writeEntry(&bw, flags, res->docId - blk->lastId, res->fieldMask, res->freq,
                   res->term.offsets.len, &res->term.offsets);
      } else {
        bw.buf->offset += sz;
        bw.pos += sz;
      }
      blk->lastId = res->docId;
    }
  }
  if (frags) {
    blk->numDocs -= frags;
    *blk->data = repair;
    Buffer_Truncate(blk->data, 0);
  }
  // IndexReader *ir = NewIndexReader()
  return frags;
}

int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock, int num) {
  int n = 0;
  while (startBlock < idx->size && (num <= 0 || n < num)) {
    int rep = IndexBlock_Repair(&idx->blocks[startBlock], dt, idx->flags);
    if (rep) {
      // printf("Repaired %d holes in block %d\n", rep, startBlock);
    }
    n++;
    startBlock++;
  }

  return startBlock < idx->size ? startBlock : 0;
}