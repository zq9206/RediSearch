#ifndef __INVERTED_INDEX_H__
#define __INVERTED_INDEX_H__

#include "redisearch.h"
#include "buffer.h"
#include "doc_table.h"
#include "forward_index.h"
#include "index_result.h"
#include "spec.h"

#include <stdint.h>

/* A single block of data in the index. The index is basically a list of blocks we iterate */
typedef struct {
  t_docId firstId;
  t_docId lastId;
  uint16_t numDocs;

  Buffer *data;
} IndexBlock;

typedef struct {
  IndexBlock *blocks;
  uint32_t size;
  IndexFlags flags;
  t_docId lastId;
  uint32_t numDocs;
} InvertedIndex;

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock);
void InvertedIndex_Free(void *idx);
int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock, int num);

/* Write a ForwardIndexEntry into an indexWriter, updating its score and skip
 * indexes if needed.
 * Returns the number of bytes written to the index */
size_t InvertedIndex_WriteEntry(InvertedIndex *idx, ForwardIndexEntry *ent);

extern inline size_t InvertedIndex_ReadEntry(BufferReader *__restrict__ br, IndexFlags idxflags,
                                             RSIndexResult *res, int singleWordMode);
#endif