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

#include "gorilla.h"

#include <assert.h>

#define BIN_NUM_VALUES 64
#define BINW BIN_NUM_VALUES

#define DOUBLE_LEADING 5
#define DOUBLE_BLOCK_SIZE 6
#define DOUBLE_BLOCK_ADJUST 1

#define CHECKSPACE(chunk, x)                                                                       \
    if (!isSpaceAvailable((chunk), (x)))                                                           \
        return CR_ERR;

#define LeadingZeros64(x) __builtin_clzll(x)
#define TrailingZeros64(x) __builtin_ctzll(x)

// Define compression steps for integer compression
// 1 bit used for positive/negative sign. Rest give 10^i. (4,7,10,14)
#define CMPR_L1 5
#define CMPR_L2 8
#define CMPR_L3 11
#define CMPR_L4 15
#define CMPR_L5 32

// The powers of 2 from 0 to 63
static u_int64_t bittt[] = {
    1ULL << 0,  1ULL << 1,  1ULL << 2,  1ULL << 3,  1ULL << 4,  1ULL << 5,  1ULL << 6,  1ULL << 7,
    1ULL << 8,  1ULL << 9,  1ULL << 10, 1ULL << 11, 1ULL << 12, 1ULL << 13, 1ULL << 14, 1ULL << 15,
    1ULL << 16, 1ULL << 17, 1ULL << 18, 1ULL << 19, 1ULL << 20, 1ULL << 21, 1ULL << 22, 1ULL << 23,
    1ULL << 24, 1ULL << 25, 1ULL << 26, 1ULL << 27, 1ULL << 28, 1ULL << 29, 1ULL << 30, 1ULL << 31,
    1ULL << 32, 1ULL << 33, 1ULL << 34, 1ULL << 35, 1ULL << 36, 1ULL << 37, 1ULL << 38, 1ULL << 39,
    1ULL << 40, 1ULL << 41, 1ULL << 42, 1ULL << 43, 1ULL << 44, 1ULL << 45, 1ULL << 46, 1ULL << 47,
    1ULL << 48, 1ULL << 49, 1ULL << 50, 1ULL << 51, 1ULL << 52, 1ULL << 53, 1ULL << 54, 1ULL << 55,
    1ULL << 56, 1ULL << 57, 1ULL << 58, 1ULL << 59, 1ULL << 60, 1ULL << 61, 1ULL << 62, 1ULL << 63
};

static uint64_t bitmask[] = {
    (1ULL << 0) - 1,  (1ULL << 1) - 1,  (1ULL << 2) - 1,  (1ULL << 3) - 1,  (1ULL << 4) - 1,
    (1ULL << 5) - 1,  (1ULL << 6) - 1,  (1ULL << 7) - 1,  (1ULL << 8) - 1,  (1ULL << 9) - 1,
    (1ULL << 10) - 1, (1ULL << 11) - 1, (1ULL << 12) - 1, (1ULL << 13) - 1, (1ULL << 14) - 1,
    (1ULL << 15) - 1, (1ULL << 16) - 1, (1ULL << 17) - 1, (1ULL << 18) - 1, (1ULL << 19) - 1,
    (1ULL << 20) - 1, (1ULL << 21) - 1, (1ULL << 22) - 1, (1ULL << 23) - 1, (1ULL << 24) - 1,
    (1ULL << 25) - 1, (1ULL << 26) - 1, (1ULL << 27) - 1, (1ULL << 28) - 1, (1ULL << 29) - 1,
    (1ULL << 30) - 1, (1ULL << 31) - 1, (1ULL << 32) - 1, (1ULL << 33) - 1, (1ULL << 34) - 1,
    (1ULL << 35) - 1, (1ULL << 36) - 1, (1ULL << 37) - 1, (1ULL << 38) - 1, (1ULL << 39) - 1,
    (1ULL << 40) - 1, (1ULL << 41) - 1, (1ULL << 42) - 1, (1ULL << 43) - 1, (1ULL << 44) - 1,
    (1ULL << 45) - 1, (1ULL << 46) - 1, (1ULL << 47) - 1, (1ULL << 48) - 1, (1ULL << 49) - 1,
    (1ULL << 50) - 1, (1ULL << 51) - 1, (1ULL << 52) - 1, (1ULL << 53) - 1, (1ULL << 54) - 1,
    (1ULL << 55) - 1, (1ULL << 56) - 1, (1ULL << 57) - 1, (1ULL << 58) - 1, (1ULL << 59) - 1,
    (1ULL << 60) - 1, (1ULL << 61) - 1, (1ULL << 62) - 1, (1ULL << 63) - 1, (0ULL - 1)

};

// 2^bit
static inline u_int64_t BIT(u_int64_t bit) {
    return bittt[bit];
}

// Logic to check Least Significant Bit (LSB) of a number
// Clear most significant bits from position `bits`
static inline u_int64_t LSB(u_int64_t x, u_int64_t bits) {
    return x & bitmask[bits];
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
// The commented out code is the full implementation, left for readability.
// Final code is an optimization.
static inline binary_t int2bin(int64_t x, u_int8_t l) {
    /*  binary_t bin = LSB(x, l - 1);
     *  if (x >= 0) return bin;
     *  binary_t sign = 1 << (l - 1);
     *  return bin | sign;*/

    binary_t bin = LSB(x, l);
    return bin;
}

// Converts `bin`, a binary of length `l` bits, into an int64
static int64_t bin2int(binary_t bin, u_int8_t l) {
    if (!(bin & BIT(l - 1)))
        return bin;
    // return (int64_t) (bin | ~MASK(l)); // sign extend `bin`
    return (int64_t)bin - BIT(l); // same but cheaper
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

static inline u_int8_t localbit(const globalbit_t bit) {
    return bit % BINW;
}

// return `true` if `x` is in [-(2^(n-1)), 2^(n-1)-1]
// e.g. for n=6, range is [-32, 31]

static inline bool Bin_InRange(int64_t x, u_int8_t nbits) {
    return x >= Bin_MinVal(nbits) && x <= Bin_MaxVal(nbits);
}

static inline bool Bins_bitoff(const u_int64_t *bins, globalbit_t bit) {
    return !(bins[bit / BINW] & BIT(localbit(bit)));
}

// unused:
// static inline bool Bins_biton(const u_int64_t *bins, globalbit_t bit) {
//    return !Bins_bitoff(bins, bit);
//}

// Append `dataLen` bits from `data` into `bins` at bit position `bit`
static inline void appendBits(binary_t *bins, globalbit_t *bit, binary_t data, u_int8_t dataLen) {
    binary_t *bin_it = &bins[(*bit) >> 6];
    localbit_t lbit = localbit(*bit);
    localbit_t available = BINW - lbit;

    if (available >= dataLen) {
        *bin_it |= LSB(data, dataLen) << lbit;
    } else {
        *bin_it |= data << lbit;
        *(++bin_it) |= LSB(data >> available, lbit);
    }
    *bit += dataLen;
}

// Read `dataLen` bits from `bins` at position `bit`
static inline binary_t readBits(const binary_t *bins,
                                globalbit_t start_pos,
                                const u_int8_t dataLen) {
    const localbit_t lbit = localbit(start_pos);
    const localbit_t available = BINW - lbit;
    if (available >= dataLen) {
        return LSB(bins[start_pos / BINW] >> lbit, dataLen);
    } else {
        binary_t bin = LSB(bins[start_pos / BINW] >> lbit, available);
        bin |= LSB(bins[(start_pos / BINW) + 1], dataLen - available) << available;
        return bin;
    }
}

static inline bool isSpaceAvailable(CompressedChunk *chunk, u_int8_t size) {
    u_int64_t available = (chunk->size * 8) - chunk->idx;
    return size <= available;
}

/***************************** APPEND ********************************/
static ChunkResult appendInteger(CompressedChunk *chunk, timestamp_t timestamp) {
#ifdef DEBUG
    assert(timestamp >= chunk->prevTimestamp);
#endif
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
    binary_t *bins = chunk->data;
    globalbit_t *bit = &chunk->idx;
    if (doubleDelta.i == 0) {
        CHECKSPACE(chunk, 1 + 1); // CHECKSPACE adds 1 as minimum for double space
        appendBits(bins, bit, 0x00, 1);
    } else if (Bin_InRange(doubleDelta.i, CMPR_L1)) {
        CHECKSPACE(chunk, 2 + CMPR_L1 + 1);
        appendBits(bins, bit, 0x01, 2);
        appendBits(bins, bit, int2bin(doubleDelta.i, CMPR_L1), CMPR_L1);
    } else if (Bin_InRange(doubleDelta.i, CMPR_L2)) {
        CHECKSPACE(chunk, 3 + CMPR_L2 + 1);
        appendBits(bins, bit, 0x03, 3);
        appendBits(bins, bit, int2bin(doubleDelta.i, CMPR_L2), CMPR_L2);
    } else if (Bin_InRange(doubleDelta.i, CMPR_L3)) {
        CHECKSPACE(chunk, 4 + CMPR_L3 + 1);
        appendBits(bins, bit, 0x07, 4);
        appendBits(bins, bit, int2bin(doubleDelta.i, CMPR_L3), CMPR_L3);
    } else if (Bin_InRange(doubleDelta.i, CMPR_L4)) {
        CHECKSPACE(chunk, 5 + CMPR_L4 + 1);
        appendBits(bins, bit, 0x0f, 5);
        appendBits(bins, bit, int2bin(doubleDelta.i, CMPR_L4), CMPR_L4);
    } else if (Bin_InRange(doubleDelta.i, CMPR_L5)) {
        CHECKSPACE(chunk, 6 + CMPR_L5 + 1);
        appendBits(bins, bit, 0x1f, 6);
        appendBits(bins, bit, int2bin(doubleDelta.i, CMPR_L5), CMPR_L5);
    } else {
        CHECKSPACE(chunk, 6 + 64 + 1);
        appendBits(bins, bit, 0x3f, 6);
        appendBits(bins, bit, doubleDelta.u, 64);
    }
    chunk->prevTimestampDelta = curDelta;
    chunk->prevTimestamp = timestamp;
    return CR_OK;
}

static ChunkResult appendFloat(CompressedChunk *chunk, double value) {
    union64bits val;
    val.d = value;
    u_int64_t xorWithPrevious = val.u ^ chunk->prevValue.u;

    binary_t *bins = chunk->data;
    globalbit_t *bit = &chunk->idx;

    // CHECKSPACE already checked for 1 extra bit availability in appendInteger.
    // Current value is identical to previous value. 1 bit used to encode.
    if (xorWithPrevious == 0) {
        appendBits(bins, bit, 0, 1);
        return CR_OK;
    }
    appendBits(bins, bit, 1, 1);

    u_int64_t leading = LeadingZeros64(xorWithPrevious);
    u_int64_t trailing = TrailingZeros64(xorWithPrevious);

    // Prevent over flow of DOUBLE_LEADING
    if (leading > 31)
        leading = 31;

    localbit_t prevLeading = chunk->prevLeading;
    localbit_t prevTrailing = chunk->prevTrailing;
#ifdef DEBUG
    assert(leading + trailing <= BINW);
#endif
    localbit_t blockSize = BINW - leading - trailing;
    u_int32_t expectedSize = DOUBLE_LEADING + DOUBLE_BLOCK_SIZE + blockSize;
#ifdef DEBUG
    assert(leading + trailing <= BINW);
#endif
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
    if (leading >= chunk->prevLeading && trailing >= chunk->prevTrailing &&
        expectedSize > prevBlockInfoSize) {
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

ChunkResult Compressed_Append(CompressedChunk *chunk, timestamp_t timestamp, double value) {
#ifdef DEBUG
    assert(chunk);
#endif

    if (chunk->count == 0) {
        chunk->baseValue.d = chunk->prevValue.d = value;
        chunk->baseTimestamp = chunk->prevTimestamp = timestamp;
        chunk->prevTimestampDelta = 0;
    } else {
        u_int64_t idx = chunk->idx;
        u_int64_t prevTimestamp = chunk->prevTimestamp;
        int64_t prevTimestampDelta = chunk->prevTimestampDelta;
        if (appendInteger(chunk, timestamp) != CR_OK || appendFloat(chunk, value) != CR_OK) {
            chunk->idx = idx;
            chunk->prevTimestamp = prevTimestamp;
            chunk->prevTimestampDelta = prevTimestampDelta;
            return CR_END;
        }
    }
    chunk->count++;
    return CR_OK;
}

/********************************** READ *********************************/
/*
 * This function decodes timestamps inserted by appendInteger.
 *
 * It checks for an OFF bit to decode the doubleDelta with the right size,
 * then decodes the value back to an int64 and calculate the original value
 * using `prevTS` and `prevDelta`.
 */
static inline u_int64_t readInteger(Compressed_Iterator *iter, const uint64_t *bins) {
    if (Bins_bitoff(bins, iter->idx++)) {
        iter->prevDelta += bin2int(readBits(bins, iter->idx, CMPR_L1), CMPR_L1);
        iter->idx += CMPR_L1;
    } else if (Bins_bitoff(bins, iter->idx++)) {
        iter->prevDelta += bin2int(readBits(bins, iter->idx, CMPR_L2), CMPR_L2);
        iter->idx += CMPR_L2;
    } else if (Bins_bitoff(bins, iter->idx++)) {
        iter->prevDelta += bin2int(readBits(bins, iter->idx, CMPR_L3), CMPR_L3);
        iter->idx += CMPR_L3;
    } else if (Bins_bitoff(bins, iter->idx++)) {
        iter->prevDelta += bin2int(readBits(bins, iter->idx, CMPR_L4), CMPR_L4);
        iter->idx += CMPR_L4;
    } else if (Bins_bitoff(bins, iter->idx++)) {
        iter->prevDelta += bin2int(readBits(bins, iter->idx, CMPR_L5), CMPR_L5);
        iter->idx += CMPR_L5;
    } else {
        iter->prevDelta += readBits(bins, iter->idx, 64);
        iter->idx += 64;
    }
    return iter->prevDelta;
}

/*
 * This function decodes values inserted by appendFloat.
 *
 * If first bit if OFF, the value hasn't changed from previous sample.
 *
 * If Next bit is OFF, previous `block size` can be used, otherwise, the
 * next 5 then 6 bits maintain number of leading and trailing zeros.
 *
 * Finally, the compressed representation of the value is decoded.
 */
static inline double readFloat(Compressed_Iterator *iter, const uint64_t *data) {
    binary_t xorValue;

    // Check if we can use the previous block info
    // meaning control bit number 2 is 1. i.e. control bits are ‘10’ (case b)
    // there are at least as many leading zeros and as
    // many trailing zeros as with the previous value
    // use  the previous block  information and
    // just read the meaningful XORed value
    if (Bins_bitoff(data, iter->idx++)) {
#ifdef DEBUG
        assert(iter->leading + iter->trailing <= BINW);
#endif
        xorValue = readBits(data, iter->idx, iter->blocksize);
        iter->idx += iter->blocksize;
        xorValue <<= iter->trailing;
    } else {
        // Read the length of the number of leading zeros in the next 5 bits,
        // then read the length of the meaningful XORed value in the next 6 bits.
        // Finally read the meaningful bits of theXORed value
        iter->leading = readBits(data, iter->idx, DOUBLE_LEADING);
        iter->idx += DOUBLE_LEADING;
        iter->blocksize = readBits(data, iter->idx, DOUBLE_BLOCK_SIZE) + DOUBLE_BLOCK_ADJUST;
        iter->idx += DOUBLE_BLOCK_SIZE;
#ifdef DEBUG
        assert(iter->leading + iter->blocksize <= BINW);
#endif
        iter->trailing = BINW - iter->leading - iter->blocksize;
        xorValue = readBits(data, iter->idx, iter->blocksize) << iter->trailing;
        iter->idx += iter->blocksize;
    }
    union64bits rv;
    rv.u = xorValue ^ iter->prevValue.u;
    return iter->prevValue.d = rv.d;
}

ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *abstractIter, Sample *sample) {
    Compressed_Iterator *iter = (Compressed_Iterator *)abstractIter;
#ifdef DEBUG
    assert(iter);
    assert(iter->chunk);
#endif
    if (unlikely(iter->count >= iter->chunk->count))
        return CR_END;
    // First sample
    if (unlikely(iter->count == 0)) {
        sample->timestamp = iter->chunk->baseTimestamp;
        sample->value = iter->chunk->baseValue.d;
        iter->count++;
        return CR_OK;
    }
    const u_int64_t *bins = iter->chunk->data;
    // We're fast checking the control bits for the cases in which the delta is 0
    // This avoids the call to expensive readInteger and readFloat functions
    //
    // control bit ‘0’
    // Read stored double delta value
    sample->timestamp = iter->prevTS +=
        Bins_bitoff(bins, iter->idx++) ? iter->prevDelta : readInteger(iter, bins);
    // Check if value was changed
    // control bit ‘0’ (case a)
    sample->value = Bins_bitoff(bins, iter->idx++) ? iter->prevValue.d : readFloat(iter, bins);
    iter->count++;
    return CR_OK;
}
