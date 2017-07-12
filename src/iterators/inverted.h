#ifndef RS_INVERTED_ITERATOR_H_
#define RS_INVERTED_ITERATOR_H_
#include <redisearch.h>
#include <inverted_index.h>
#include "index_iterator.h"

/* An IndexReader wraps an inverted index record for reading and iteration */
typedef struct indexReadCtx {
  // the underlying data buffer
  BufferReader br;

  InvertedIndex *idx;
  // last docId, used for delta encoding/decoding
  t_docId lastId;
  uint32_t currentBlock;
  // // skip index. If not null and is needed, will be used for intersects
  // SkipIndex *skipIdx;
  // u_int skipIdxPos;
  DocTable *docTable;

  t_fieldMask fieldMask;

  IndexFlags flags;
  // processed version of the "interesting" part of the flags
  IndexFlags readFlags;

  int singleWordMode;

  size_t len;
  RSIndexResult *record;
  RSQueryTerm *term;

  int atEnd;
} IndexReader;

/* Create a new index reader on an inverted index buffer,
* optionally with a skip index, docTable and scoreIndex.
* If singleWordMode is set to 1, we ignore the skip index and use the score
* index.
*/
IndexReader *NewIndexReader(InvertedIndex *idx, DocTable *docTable, t_fieldMask fieldMask,
                            IndexFlags flags, RSQueryTerm *term, int singleWordMode);

/* Create a reader iterator that iterates an inverted index record */
IndexIterator *NewReadIterator(IndexReader *ir);

#endif