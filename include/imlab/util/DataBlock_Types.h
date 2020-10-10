// #define DEBUG_COUNT
// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_UTIL_DATABLOCK_TYPES_H_
#define INCLUDE_IMLAB_UTIL_DATABLOCK_TYPES_H_
// ---------------------------------------------------------------------------------------------------
#include <stdint.h>
#include <assert.h>
#include <type_traits>
#include <cstdint>
#include <functional>
#include <sstream>
#include <iostream>
#include <array>
#include <cstddef>
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace util {
// ---------------------------------------------------------------------------------------------------
/// Size of uint8_t
constexpr size_t UB1_SIZE = sizeof(uint8_t);

constexpr size_t UB1_SIZE_LOG2 = 0;

/// Size of uint16_t
constexpr size_t UB2_SIZE = sizeof(uint16_t);

constexpr size_t UB2_SIZE_LOG2 = 1;

/// Size of uint32_t
constexpr size_t UB4_SIZE = sizeof(uint32_t);

constexpr size_t UB4_SIZE_LOG2 = 2;

/// Size of uint64_t
constexpr size_t UB8_SIZE = sizeof(uint64_t);

constexpr size_t UB8_SIZE_LOG2 = 3;
// ---------------------------------------------------------------------------------------------------
struct free_delete {
    void operator()(void* x) { free(x); }
};

/**
 * The maximal tuple number for a single DataBlock
 * 65536 = 2 ^ 16 from the Paper
 * 65536 = 2 ^ 16 > UINT32_MAX = 65535
 */
constexpr uint32_t TUPLES_PER_DATABLOCK = 1 << 16;

/**
 * The Data Type of the Table Schema
 * Should be given, before the Datablock is built
 * HERE: I distinguish only Int and Str (decimal are already encoding as Int in the Dataset and the Querys)
 */
enum SchemaType {
    Interger,
    Char
};

/**
 * Byte1, Byte2, Byte4, Byte8: different integer in DataBlock (int8_t, int16_t, int32_t, int64_t)
 * Single Value: is the compression way, when the data all the same is
 * String: Dictionary Encoding applied for String
 */
enum EncodingType {
  Byte1,
  Byte2,
  Byte4,
  Byte8,
  Singlevalue
};

/// Reason for these static assert: I use some bit shifting according to EncodingType
static_assert(Byte1 == 0, "2 ^ 0 == 1");

static_assert(Byte2 == 1, "2 ^ 1 == 2");

static_assert(Byte4 == 2, "2 ^ 2 == 4");

static_assert(Byte8 == 3, "2 ^ 3 == 8");

static_assert(Singlevalue == 4, "Singlevalue == 4");

/**
 * So far these Compare Type are implemented
 * EQUAL, GREATER_THAN, LESS_THAN, BETWEEN
 */
enum CompareType {
  EQUAL,
  GREATER_THAN,
  LESS_THAN,
  BETWEEN /* as [x, y], double sides included*/
};

class DataBlock_Offsets {
public:
  DataBlock_Offsets(uint32_t smaOffset,
                    uint32_t dictOffset,
                    uint32_t stringOffset,
                    uint32_t dataOffset)
                    : sma_offset(smaOffset),
                      dict_offset(dictOffset),
                      string_offset(stringOffset),
                      data_offset(dataOffset) {}

  uint32_t getSmaOffset() const { return sma_offset; }

  void setSmaOffset(uint32_t smaOffset) { sma_offset = smaOffset; }

  uint32_t getDictOffset() const { return dict_offset; }

  void setDictOffset(uint32_t dictOffset) { dict_offset = dictOffset; }

  uint32_t getStringOffset() const { return string_offset; }

  void setStringOffset(uint32_t stringOffset) { string_offset = stringOffset; }

  uint32_t getDataOffset() const { return data_offset; }

  void setDataOffset(uint32_t dataOffset) { data_offset = dataOffset; }

private:
  /**
   * The offset to SMA
   */
  uint32_t sma_offset;

  /**
   * The offset to data = sma_offset + 32 for SMA + 32 for padding
   */
  uint32_t data_offset;

  /**
   * The offset to string_offset: only for string column at the last of data block
   */
  uint32_t string_offset;

  /**
   * The offset to dict_offset: only for string column at the last of data block, and after string_offset
   */
  uint32_t dict_offset;
};
//__attribute__((__packed__));

template<size_t COLUMN_COUNT>
class DataBlock_Header {
 public:
  /**
   * Number of records, contained by this DataBlock.
   * "Typically, we store up to 2^16 records in a DataBlock." => uint32_t
   */
  uint32_t tuple_count;

  /**
   * The Offset to stop
   * The Datablock Header and 32 aligned padding considered
   */
  uint64_t stop_offset;

  std::array<DataBlock_Offsets, COLUMN_COUNT> offsets;

  /**
   * The Encoding Type of data in each Column
   */
  std::array<EncodingType, COLUMN_COUNT> encoding_types;

  /**
   *  Constructor
   */
  DataBlock_Header(uint32_t tuple_count,
                   std::array<DataBlock_Offsets, COLUMN_COUNT> offsets,
                   uint64_t stop_offset,
                   std::array<EncodingType, COLUMN_COUNT> encoding_type)
      : tuple_count(tuple_count),
        offsets(offsets),
        stop_offset(stop_offset),
        encoding_types(encoding_type) {}

  /**
    *  Copy Constructor
    */
  explicit DataBlock_Header(const DataBlock_Header *header) {
    tuple_count = header->tuple_count;
    offsets = header->offsets;
    stop_offset = header->stop_offset;
    encoding_types = header->encoding_types;
  }
}
__attribute__((__packed__));
// ---------------------------------------------------------------------------------------------------
}  // namespace util
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_UTIL_DATABLOCK_TYPES_H_
// ---------------------------------------------------------------------------------------------------

