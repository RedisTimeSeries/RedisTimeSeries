#include <assert.h>         // assert
#include <limits.h>
#include <stdbool.h>        // bool
#include <stdlib.h>         // malloc
#include <stdio.h>          // printf
#include <math.h>           // rand

#include "compressed_chunk.h"
#include "gorilla.h"

void testIter() {
  CompressedChunk *chunk = CChunk_NewChunk(4096);
  CChunk_Append(chunk, 10, 76.54);
  CChunk_Append(chunk, 11, 76.60);
  CChunk_Append(chunk, 12, 80.00);
  CChunk_Append(chunk, 13, 81.00);
  CChunk_Append(chunk, 14, 81.10);
  CChunk_Append(chunk, 15, 81.25);
  CChunk_Append(chunk, 30, 24.876);
  CChunk_Append(chunk, 30, 24.877);
  CChunk_Append(chunk, 30, 24.878);
  CChunk_Append(chunk, 30, 24.877);
  CChunk_Append(chunk, 30, 24.876);
  CChunk_Append(chunk, 130, 2);
  CChunk_Append(chunk, 132, 22);
  CChunk_Append(chunk, 135, 0);
  
  double value;
  u_int64_t timestamp;
  CChunk_Iterator *iter = CChunk_GetIterator(chunk);
  printf("\n");
  for(int i = 0; i < 14; ++i) {
    printf("i %d\t idx %lu\t", i, getIterIdx(iter));
    CChunk_ReadNext(iter, &timestamp, &value);
    printf("timestamp %lu\t value %3.3lf\n", timestamp, value);
  }
}

void testIterLoop() {
  printf("** start testIterLoop **\n");
  srand(0);
  CompressedChunk *chunk = CChunk_NewChunk(4096);
  for(int i = 1; i; ++i) {
    int success = CChunk_Append(chunk, i, ((i + rand()) % 100)/* * 1.123*/);
    if (success != CC_OK) break;
  }
  
  double value;
  u_int64_t timestamp;
  CChunk_Iterator *iter = CChunk_GetIterator(chunk);
  printf("\n");
  for(int i = 1; i; ++i) {
    printf("i %d\t idx %lu\t", i, getIterIdx(iter));
    int success = CChunk_ReadNext(iter, &timestamp, &value);
    if (success != CC_OK) {
      printf("finished\n");
      break;
    }
    printf("timestamp %lu\t value %3.3lf\n", timestamp, value);
  }
}

int main() {
  //testIter();
  testIterLoop();
  return 0;
}