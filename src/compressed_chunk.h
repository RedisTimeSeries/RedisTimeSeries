#ifndef COMPRESSED_CHUNK_H
#define COMPRESSED_CHUNK_H

#include <sys/types.h>      // u_int_t
#include <stdbool.h>        // bool

typedef struct CChunk_Iterator CChunk_Iterator;
typedef struct CompressedChunk CompressedChunk;

typedef struct Sample {
    u_int64_t timestamp;
    double data;
} Sample;

enum result {
  CC_OK = 0,    // RM_OK
  CC_ERR = 1,   // RM_ERR
  CC_END = 2  
};

// Initialize compressed chunk
CompressedChunk *CChunk_NewChunk(u_int64_t size);
void CChunk_FreeChunk(CompressedChunk *chunk);

// Append a sample to a compressed chunk
int CChunk_AddSample(CompressedChunk *chunk, Sample sample);          // From RTS
int CChunk_Append(CompressedChunk *chunk, u_int64_t timestamp, double value);

int       CChunk_IsChunkFull      (CompressedChunk *chunk);
int       CChunk_ChunkNumOfSample (CompressedChunk *chunk);
u_int64_t CChunk_GetFirstTimestamp(CompressedChunk *chunk);
u_int64_t CChunk_GetLastTimestamp (CompressedChunk *chunk);

// Read from compressed chunk using an iterator
CChunk_Iterator *CChunk_GetIterator(CompressedChunk *chunk);
int  CChunk_ReadNext(CChunk_Iterator *iter, u_int64_t *timestamp, double *value);
void CChunk_FreeIter(CChunk_Iterator *iter);

CChunk_Iterator *NewChunkIterator(CompressedChunk *chunk);                  // From RTS
int ChunkIteratorGetNext(CChunk_Iterator *iter, Sample* sample);           // From RTS

u_int64_t getIterIdx(CChunk_Iterator *iter);

#endif // COMPRESSED_CHUNK_H