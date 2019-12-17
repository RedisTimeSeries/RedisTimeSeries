/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 * 
 * This file is available under the Redis Labs Source Available License Agreement
 * 
 ******************************************************************************
 *
 * Compression algorithm based on a paper by Facebook, Inc. 
 * "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
 * Section 4.1 "Time series compression"
 * Link: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 * 
 * Implementation by Ariel Shtul
 * 
 ******************************************************************************
 * 
 * DoubleDelta compression algorithm is a combinattion of two separete
 * algorithms : 
 * * Compression of Delta of Deltas between integers
 * * Compression of doubles.
 * 
 ******************************************************************************
 * Compression of Delta of Deltas (DoubleDelta) between integers
 * 
 * The DoubleDelta value is calculated using the stored values of the previous
 * value and the previous delta.
 * 
 * Writing:
 * If DoubleDelta equal 0, one bit is set to 0 and we are done.
 * Else, the are preset buckets of size 7, 10, 13 and 16 bits. We test for the
 * minimal bucket which can hold DoubleDelta. Since DoubleDelta can be negative,
 * the ranges are [-2^(size - 1), 2^(size - 1) - 1]. For each `size` of bucket
 * we set one bit to 1 and an additional bit to 0. We will then use the next 13
 * bits to store the value. If DoubleDelta does not fit in any of the buckets,
 * we will set five bits to 1 and use the following 64 bits.
 * Example, 999 fits at the bucket-size of 13 and therefore we will use four
 * bits and set them to `0111`. Then set the following 13 bits to
 * `0001111100111`. Setting total of 17 bits to '00011111001110111'.
 * 
 * Reading:
 * The reverse process, if the first bit is set to 0, the double delta is 0 and
 * we return lastValue + lastDelta.
 * Else, we count consecutive bits set to 1 up to 5 and will use the appropriate
 * `size` of bucket to read the value (7, 10, 13, 16, 64). For example, 
 * `00011111001110111` has three consecutive bits set to 1 and therefore the 
 * bucket-size is 13. The next 13 bits are decoded into DoubleDelta. The
 * function returns DoubleDelta + lastDelta + lastValue.
 ************************************************************************************
 *           final          *       binary       *  bits *      range     * example *
 ************************************************************************************
 *                        0 *                  0 *       *                *       0 *
 *                000010101 *            0000101 *    01 *       [-64,63] *       5 *
 *            1111100111011 *         1111100111 *   011 *     [-512,511] *    -487 *
 *        11000010001000111 *      1100001000100 *  0111 *   [-4096,4095] *   -1980 *
 *    000101100110110001111 *   0001011001101100 * 01111 * [-32768,32767] *    5740 * 
 * 0x00000000000186A0 11111 * 0x00000000000186A0 * 11111 *  [Min64,Max64] *  100000 *
 ************************************************************************************
 * Compression of (XOR of) doubles
 * 
 * Writing:
 * A XOR value is calculated using last double value.
 * If XOR equal 0, one bit is set to 0 and we are done.
 * Else, we calculate the number of leading and trailing 0s (MASK).
 * For optimization, if using the last value's MASK will overall save storage
 * space. If it does, one bit is set to 0 else, one bit is set to 1, the next 5
 * bits hold the number of leading 0's and the following 6 bits hold the number
 * of trailing 0s.
 * At last, XOR is shifted to the right(>>) by `leading` bit and is store in 
 * (64 - leading - trailing) bits.
 * 
 * Reading:
 * The reverse process, if the first bit is set to 0, lastValue is returned.
 * Else, if the following bit is set to 0, last `leading` and `trailing` are
 * read, otherwise, the 5 then 6 bits are read for `leading` and `trailing`
 * respectively.
 * Next, (64 - `leading` - `trailing`) bits are read and shifted left (<<) by
 * `leading` and the function returns this number^prevresult (XOR) and returned.
 * 
 ********************************************************************************* 
 *      final               *   binary           * t  * l  * p * 0 * value * prev*
 *********************************************************************************                                                                          
 *                        0 *                    *    *    *   * 0 *   2.2 * 2.2 *
 * (using prev params)  101 *                  1 *    *    * 0 * 1 *     2 *   3 *
 *        1 110011 01100 11 *                  1 * 51 * 12 * 1 * 1 *     2 *   3 *
 *    0x0024b33333333333 01 * 0x0024b33333333333 *    *    * 0 * 1 *  18.7 * 5.5 * 
 * 0x0024b33333333333 01011 * 0x0024b33333333333 *  0 * 10 * 1 * 1 *  18.7 * 5.5 * 
 *********************************************************************************
 * t=trailing, l=leading, p=use of previous params, 0=xor equal zero 
 */

#include <assert.h>

#include "gorilla.h"

#define BIN_NUM_VALUES 64
#define BINW BIN_NUM_VALUES

#define DOUBLE_LEADING 5
#define DOUBLE_BLOCK_SIZE 6
#define DOUBLE_BLOCK_ADJUST 1

#define CHECKSPACE(chunk, x) \
    do { \
        if (!isSpaceAvailable((chunk), (x))) return CR_ERR; \
    } while (0)

#define LeadingZeros64(x)   __builtin_clzll(x)
#define TrailingZeros64(x)  __builtin_ctzll(x)

// 2^bit
static inline u_int64_t BIT(u_int64_t bit) {
    return 1ULL << bit;
}

// the LSB `bits` turned on
static inline u_int64_t MASK(u_int64_t bits) {
    return (1ULL << bits) - 1;
}

// Clear most significant bits from position `bits`
static inline u_int64_t LSB(u_int64_t x, u_int64_t bits) {
    return x & ((1ULL << bits) - 1);
}

/* 
 * int2bin and bin2int functions mirror each other.
 * int2bin is used to encode int64 into smaller representation to conserve space.
 * bin2int is used to decode input bits into an int64.
 * Example 1: int2bin(7, 10) = 7. Bit representation 0000000111
 *            bin2int(7, 10) = 7
 * Example 2: int2bin(-7, 10) = 1017. Bit representation 1111111001
 *            bin2int(1017, 10) = -7
 */

/*
 The binary_t type is a 2's-complement integer represented in `l` bits,
 with the remaining significant bits set to 0.
 Thus, the representation of a positive int64 7 as binary_t(10)
 will be the same (0-0000000111), while a negative int64 -7 will transform
 from 1-11111111001 to 0-01111111001.
 Thus the sign bit of a binary_t(10) is bit 9.
 */
 
// Converts `x`, an int64, to binary representation with length `l` bits
static binary_t int2bin(int64_t x, u_int8_t l) {
#if 1
    binary_t bin = LSB(x, l);
	return bin;
#else
    binary_t bin = LSB(x, l - 1);
    if (x >= 0) return bin;
    binary_t sign = 1 << (l - 1);
    return bin | sign;
#endif
}

// Converts `bin`, a binary of length `l` bits, into an int64
static int64_t bin2int(binary_t bin, u_int8_t l) {
    bool pos = !(bin & BIT(l - 1));
	if (pos) return bin;
    //return (int64_t) (bin | ~MASK(l)); // sign extend `bin`
    return (int64_t) bin - BIT(l); // same but cheaper
}

// note that return value is a signed int
static inline int64_t Bin_MaxVal(u_int8_t nbits) {
    return BIT(nbits - 1) - 1;
}

// note that return value is a signed int
static inline int64_t Bin_MinVal(u_int8_t nbits) {
    return -BIT(nbits - 1);
}

// `bit` is a global bit (can be out of scope of a single binary_t)

static inline u_int8_t localbit(globalbit_t bit) {
    return bit % BINW;
}

// return `true` if `x` is in [-(2^(n-1)), 2^(n-1)-1]
// e.g. for n=6, range is [-32, 31]

static bool Bin_InRange(int64_t x, u_int8_t nbits) {
    return x >= Bin_MinVal(nbits) && x <= Bin_MaxVal(nbits);
}

static inline binary_t *Bins_bitbin(u_int64_t *bins, globalbit_t bit) {
    return &bins[bit / BINW];
}

static inline bool Bins_bitoff(u_int64_t *bins, globalbit_t bit) {
    return !(bins[bit / BINW] & BIT(localbit(bit)));
}

static inline bool Bins_biton(u_int64_t *bins, globalbit_t bit) {
    return !Bins_bitoff(bins, bit);
}

// Append `dataLen` bits from `data` into `bins` at bit position `bit`
static void appendBits(binary_t *bins, globalbit_t *bit, binary_t data, u_int8_t dataLen) {
    binary_t *bin_it = Bins_bitbin(bins, *bit);
    localbit_t lbit = localbit(*bit);
    localbit_t available = BINW - lbit;

    if (available >= dataLen) {
        *bin_it |= LSB(data, dataLen) << lbit;
    } else {
        *bin_it |= data << lbit;
        *(++bin_it) |= LSB(data, dataLen) >> available;
    }
    *bit += dataLen;
}

// Read `dataLen` bits from `bins` at position `bit`
static binary_t readBits(binary_t *bins, globalbit_t *bit, u_int8_t dataLen) {
    binary_t *bin_it = Bins_bitbin(bins, *bit);
    localbit_t lbit = localbit(*bit);
    localbit_t available = BINW - lbit;

    binary_t bin = 0;
    if (available >= dataLen) {
        bin = LSB(*bin_it >> lbit, dataLen);
    } else {
        u_int8_t left = dataLen - available;
        bin = LSB(*bin_it >> lbit, available);
        bin |= LSB(*++bin_it, left) << available;
    }
    *bit += dataLen;
    return bin;
}

static bool isSpaceAvailable(CompressedChunk *chunk, u_int8_t size) {
    u_int64_t available = (chunk->size * 8) - chunk->idx;
    return size <= available;
}

/***************************** APPEND ********************************/
static ChunkResult appendTS(CompressedChunk *chunk, timestamp_t timestamp) {
    assert(timestamp >= chunk->prevTimestamp);
    timestamp_t curDelta = timestamp - chunk->prevTimestamp;
    
    union64bits doubleDelta;
    doubleDelta.i = curDelta - chunk->prevTimestampDelta;
/* 
 * Before any insertion the code `CHECKSPACE` ensures there is enough space to
 * encode timestamp and one additional bit which the minimum to encode the value.
 * This is why we have `+ 1` in `CHECKSPACE`.
 * 
 * If doubleDelta == 0, 1 bit of value 0 is inserted.
 * 
 * Else, `Bin_InRange` checks for the minimal number of bits required to represent
 * `doubleDelta`, the delta of deltas between current and previous timestamps.
 * Then two values are being inserted. 
   * The first value is, encoding for the lowest number of bits for which
     `Bin_InRange` returns `true`.
   * The second value is a compressed representation of the value with the `length`
     encoded by the first value. Compression is done using `int2bin`.
 */
    u_int64_t *bins = chunk->data;
    u_int64_t *bit = &chunk->idx;
    if (doubleDelta.i == 0) {
        CHECKSPACE(chunk, 1 + 1); // CHECKSPACE adds 1 as minimum for double space
        appendBits(bins, bit, 0x00, 1);
    } else if (Bin_InRange(doubleDelta.i, 7)) {
        CHECKSPACE(chunk, 2 + 7 + 1);
        appendBits(bins, bit, 0x01, 2);
        appendBits(bins, bit, int2bin(doubleDelta.i, 7), 7);
    } else if (Bin_InRange(doubleDelta.i, 10)) {
        CHECKSPACE(chunk, 3 + 10 + 1);
        appendBits(bins, bit, 0x03, 3);
        appendBits(bins, bit, int2bin(doubleDelta.i, 10), 10);
    } else if (Bin_InRange(doubleDelta.i, 13)) {
        CHECKSPACE(chunk, 4 + 13 + 1);
        appendBits(bins, bit, 0x07, 4);
        appendBits(bins, bit, int2bin(doubleDelta.i, 13), 13);
    } else if (Bin_InRange(doubleDelta.i, 16)) {
        CHECKSPACE(chunk, 5 + 16 + 1);
        appendBits(bins, bit, 0x0f, 5);
        appendBits(bins, bit, int2bin(doubleDelta.i, 16), 16);
    } else {
        CHECKSPACE(chunk, 5 + 64 + 1);
        appendBits(bins, bit, 0x1f, 5);
        appendBits(bins, bit, doubleDelta.u, 64);
    }
    chunk->prevTimestampDelta = curDelta;
    chunk->prevTimestamp = timestamp;
    return CR_OK;
}

static ChunkResult appendV(CompressedChunk *chunk, double value) {
    union64bits val;
    val.d = value;
    u_int64_t xorWithPrevious = val.u ^ chunk->prevValue.u;

    binary_t *bins = chunk->data;
    globalbit_t *bit = &chunk->idx;
    
    // CHECKSPACE already checked for 1 extra bit availability in appendTS.
    // Current value is identical to previous value. 1 bit used to encode.
    if (xorWithPrevious == 0) {
        appendBits(bins, bit, 0, 1);
        return CR_OK;   
    }
    appendBits(bins, bit, 1, 1);

    u_int64_t leading  = LeadingZeros64(xorWithPrevious);
    u_int64_t trailing = TrailingZeros64(xorWithPrevious);
  
    // Prevent over flow of DOUBLE_LEADING
    if (leading > 31) { 
        leading = 31; 
    }

    localbit_t prevLeading = chunk->prevLeading;
    localbit_t prevTrailing = chunk->prevTrailing;  
    assert(leading + trailing <= BINW);
    localbit_t blockSize = BINW - leading - trailing;
    u_int32_t expectedSize = DOUBLE_LEADING + DOUBLE_BLOCK_SIZE + blockSize;
    assert(prevLeading + prevTrailing <= BINW);
    localbit_t prevBlockInfoSize = BINW - prevLeading - prevTrailing;
/*
 * First bit encodes whether previous block parameters can be used since
 * encoding block-size requires 5 + 6 bits.
 * 
 * If previous block size is used and the block itself is being appended.
 * 
 * Else, number of leading zeros in inserted followed by trailing zeros.
 * Then the value is the block is being appended. 
 */
    if (leading >= chunk->prevLeading && 
        trailing >= chunk->prevTrailing &&
        expectedSize > prevBlockInfoSize) 
    {
        CHECKSPACE(chunk, prevBlockInfoSize + 1);
        appendBits(bins, bit, 0, 1);    
        appendBits(bins, bit, xorWithPrevious >> prevTrailing, prevBlockInfoSize);
    } else {
        CHECKSPACE(chunk, expectedSize + 1);
        appendBits(bins, bit, 1, 1);
        appendBits(bins, bit, leading, DOUBLE_LEADING);
        appendBits(bins, bit, blockSize - DOUBLE_BLOCK_ADJUST, DOUBLE_BLOCK_SIZE);            
        appendBits(bins, bit, xorWithPrevious >> trailing, blockSize);  
        chunk->prevLeading = leading;
        chunk->prevTrailing = trailing;
    }
    chunk->prevValue.d = value;
    return CR_OK;
}

ChunkResult CChunk_Append(CompressedChunk *chunk, timestamp_t timestamp, double value) {
    assert(chunk);

    if (chunk->count == 0) {
        chunk->baseValue.d   = chunk->prevValue.d = value;
        chunk->baseTimestamp = chunk->prevTimestamp = timestamp;
        chunk->prevTimestampDelta = 0;
    } else {
        if (appendTS(chunk, timestamp) != CR_OK) return CR_END;
        if (appendV (chunk, value) != CR_OK) return CR_END;
    }
    chunk->count++;
    return CR_OK;
}

/********************************** READ *********************************/
/* 
 * This function decodes timestamps inserted by appendTS.
 * 
 * It checks for an OFF bit to decode the doubleDelta with the right size,
 * then decodes the value back to an int64 and calculate the original value
 * using `prevTS` and `prevDelta`.
 */ 
static u_int64_t readTS(CChunk_Iterator *iter) {
    binary_t *bins = iter->chunk->data;
    globalbit_t *bit = &iter->idx;

    int64_t dd = 0;
    // Read stored double delta value
    if (Bins_bitoff(bins, (*bit)++)) {
        dd = 0;
    } else if (Bins_bitoff(bins, (*bit)++)) {
        dd = bin2int(readBits(bins, bit, 7), 7);
    } else if (Bins_bitoff(bins, (*bit)++)) {
        dd = bin2int(readBits(bins, bit, 10), 10);
    } else if (Bins_bitoff(bins, (*bit)++)) {
        dd = bin2int(readBits(bins, bit, 13), 13);
    } else if (Bins_bitoff(bins, (*bit)++)) {
        dd = bin2int(readBits(bins, bit, 16), 16);
    } else {
        dd = bin2int(readBits(bins, bit, 64), 64);
    }

    // Update iterator
    iter->prevDelta += dd;
    return iter->prevTS = iter->prevTS + iter->prevDelta;  
}

/* 
 * This function decodes values inserted by appendV.
 * 
 * If first bit if OFF, the value hasn't changed from previous sample.
 * 
 * If Next bit is OFF, previous `block size` can be used, otherwise, the
 * next 5 then 6 bits maintain number of leading and trailing zeros.
 * 
 * Finally, the compressed representation of the value is decoded.
 */ 
static double readV(CChunk_Iterator *iter) {
    binary_t xorValue;
    union64bits rv;

    // Check if value was changed
    if (Bins_bitoff(iter->chunk->data, iter->idx++)) {
        return iter->prevValue.d;
    }

  // Check if previous block information was used
    bool usePreviousBlockInfo = Bins_bitoff(iter->chunk->data, iter->idx++);
    if (usePreviousBlockInfo) {
        assert(iter->prevLeading + iter->prevTrailing <= BINW);
        u_int8_t prevBlockInfo = BINW - iter->prevLeading - iter->prevTrailing;
        xorValue = readBits(iter->chunk->data, &iter->idx, prevBlockInfo);
        xorValue <<= iter->prevTrailing;
    } else {
        binary_t leading = readBits(iter->chunk->data, &iter->idx, DOUBLE_LEADING);
        binary_t blocksize = readBits(iter->chunk->data, &iter->idx, DOUBLE_BLOCK_SIZE) + DOUBLE_BLOCK_ADJUST;
        assert(leading + blocksize <= BINW);
        binary_t trailing = BINW - leading - blocksize;
        xorValue = readBits(iter->chunk->data, &iter->idx, blocksize) << trailing;
        iter->prevLeading = leading;
        iter->prevTrailing = trailing;    
    }

    rv.u = xorValue ^ iter->prevValue.u;
    return iter->prevValue.d = rv.d;
}

ChunkResult CChunk_ReadNext(CChunk_Iterator *iter, timestamp_t *timestamp, double *value) {
    assert(iter);
    *timestamp = iter->prevTS;
    *value     = iter->prevValue.d;

    assert(iter->chunk);
    if (iter->count >= iter->chunk->count) return CR_END;

    iter->prevTS      = readTS(iter);
    iter->prevValue.d = readV (iter);
    iter->count++;
    return CR_OK;
}