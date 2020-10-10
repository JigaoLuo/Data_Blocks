// #define DEBUG
// #define DEBUG_BUILD
// #define DEBUG_BUILD_HEADER
// #define DEBUG_SCAN
// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_DATABLOCK_DATABLOCK_H_
#define INCLUDE_IMLAB_DATABLOCK_DATABLOCK_H_
// ---------------------------------------------------------------------------------------------------
#include <cassert>
#include <algorithm>
#include <thread>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>
#include <set>
#include <immintrin.h>
#include "imlab/continuous_memory/continuous_memory.h"
#include "imlab/datablock/SMA.h"
#include "imlab/util/Count_byte1.h"
#include "imlab/util/Count_byte2.h"
#include "imlab/util/Count_byte4.h"
#include "imlab/util/Count_byte8.h"
#include "imlab/util/Count_singlevalue.h"
#include "imlab/util/DataBlock_Types.h"
#include "imlab/util/MatchTable.h"
// ---------------------------------------------------------------------------------------------------
namespace imlab {
// ---------------------------------------------------------------------------------------------------
using util::EncodingType;
using util::EncodingType::Byte1;
using util::EncodingType::Byte2;
using util::EncodingType::Byte4;
using util::EncodingType::Byte8;
using util::EncodingType::Singlevalue;
using util::Predicate;
using util::CompareType;
using util::CompareType::BETWEEN;
using util::CompareType::EQUAL;
using util::CompareType::GREATER_THAN;
using util::CompareType::LESS_THAN;
using util::TUPLES_PER_DATABLOCK;
using util::SchemaType;
using util::FirstScan_MatchTable;
// ---------------------------------------------------------------------------------------------------
template<size_t COLUMN_COUNT>
class DataBlock {
  friend class Continuous_Memory_Segment<COLUMN_COUNT>;

 private:
  /**
   * Header
   */
  DataBlock_Header<COLUMN_COUNT> header;

  /**
   * Get the length of the data exclude the 16 bytes for SMA
   */
  inline uint64_t get_column_data_length(const size_t slotId);

  /**
    * Get the size of data in Bytes
    * @param table_portion Table containing the columns
    * @param encodings Encodings to the columns
    */
  inline static uint64_t get_data_bytes(const std::vector<std::vector<uint64_t>> &table_portion,
                                        const std::vector<EncodingType>          &encodings);

  /**
    * Get the size of dictionary in Bytes
    * FORMAT:
    * uint32: col_id | uint32: # bytes for this col | uint32: first string length | first string | uint32: second string length | second string ....
    *
    * @param str_set_vec all string columns, in the set representation
    */
  inline static uint64_t get_dictionary_bytes(const std::vector<std::set<std::string>>& str_set_vec);

  /**
    * Helper function to figure out the encoding and the SMA
    * @param encodings Encodings to the columns
    * @param sma_vector SMA vectors for the column
    * @param int_table_portion Table containing the columns
    */
  static void build_Encoding_SMA(std::vector<EncodingType>& encodings, std::vector<SMA>& sma_vector, const std::vector<std::vector<uint64_t>> &int_table_portion);

 public:
  typedef void (*DataBlock_deleter) (DataBlock<COLUMN_COUNT> *);

  /**
   *  Constructor
   *  Template parameter COLUMN_COUNT: # Columns
   */
  DataBlock<COLUMN_COUNT>() { header.tuple_count = 0; }

  /**
   *  Copy Constructor
   *  Template parameter COLUMN_COUNT: # Columns
   */
  DataBlock<COLUMN_COUNT>(const DataBlock *db) {
    header.tuple_count = db->tuple_count;
    header.data_offsets = db->data_offsets;
    header.stop_offset = db->stop_offset;
    header.encoding_types = db->encoding_type;
  }

  /**
   *  Destructor
   */
  ~DataBlock<COLUMN_COUNT>() {}

  /**
   * Get the data
   */
  inline uint32_t get_tupel_count() const noexcept { return header.tuple_count; }

  /**
   * Return, if this DataBlock is empty
   */
  inline bool is_empty() { return header.tuple_count == 0; }

  /**
   * Return, if this DataBlock is full
   */
  inline bool is_full() { return header.tuple_count == TUPLES_PER_DATABLOCK; }

  /**
   * Return the column count
   */
  inline size_t get_column_count() { return COLUMN_COUNT; }

  /**
   * Return the all encodings in a std::array
   */
  inline std::array<EncodingType, COLUMN_COUNT> getEncodings() { return header.encoding_types; }

  /**
   * Return the DataBlock size in Byte, dictionary included
   */
  inline uint64_t getDataBlockSize() {
    return header.stop_offset;
  }

  /**
   * Return the Header length
   */
  inline uint64_t getHeadSize() {
    return header.offsets[0].getSmaOffset();
  }

  /**
   * Return the offset to a column FROM Header
   */
    inline uint64_t get_column_offset(const int column_id) {
      return header.offsets[column_id].getDataOffset();
    }

  /**
  * Return the offset to a column SMA FROM Header
  */
    inline uint64_t get_SMA_offset(const int column_id) {
      return header.offsets[column_id].getSmaOffset();
    }

  /**
   * Build a DataBlock with allocated memory
   * The encoding will be figured out then data will be byte addressable
   * 
   * @param int_table_portion Table containing the columns (col_id sorted), which are ints
   * @param str_table_portion Table containing the columns (col_id sorted), which are string
   * @param schematypes each data types for each column
   * @return the DataBlock as unique pointer
   */
  static std::unique_ptr<DataBlock<COLUMN_COUNT>, DataBlock_deleter>
         build(const std::vector<std::vector<uint64_t>>& int_table_portion,
               const std::vector<std::vector<std::string>>& str_table_portion,
               const std::vector<SchemaType> &schematypes);

  /**
   * Scan using some predicates to find the number of tuple, which can satisfy all the predicates
   * @param vector of predicate
   * @param index_vector compile time allocated array representing index vector
   * @return the number of tuple, which can satisfy all the predicates
   *         It should be uint32_t: because 1 << 16 = 65536 > UINT32_MAX = 65535
   *         From Discussion wih Alex
   */
  uint32_t scan(std::vector<Predicate> &predicates, std::array<uint32_t, TUPLES_PER_DATABLOCK>& index_vector);

  /**
   * Decode to get all the string set
   * Should be called for Query on String
   */
  template<std::size_t SIZE>
  std::vector<std::set<std::string>> decode_dictionary(const std::array<uint8_t , SIZE>& str_index_arr);

  /**
   * Decode a single string
   * Should be called for Query on String
   */
  size_t decode_string(const std::string& str, uint8_t column_id);
};

template <size_t COLUMN_COUNT>
uint64_t DataBlock<COLUMN_COUNT>::get_column_data_length(const size_t slotId) {
  assert(slotId < COLUMN_COUNT);
  assert(header.encoding_types[slotId] != EncodingType::Singlevalue);
  return header.tuple_count * (1 << header.encoding_types[slotId]);
}

template <size_t COLUMN_COUNT>
uint64_t DataBlock<COLUMN_COUNT>::get_data_bytes(const std::vector<std::vector<uint64_t>> &table_portion,
                                                 const std::vector<EncodingType>          &encodings) {
  uint64_t data_bytes = 0;
  for (size_t i = 0; i < encodings.size(); ++i) {
    switch (encodings[i]) {
      case Byte1:
        while ((data_bytes) % 32 != 0) ++data_bytes;
        data_bytes += sizeof(uint8_t) * table_portion[i].size() + 32;   /// 2 * 8 for min max, 16 for padding
        break;
      case Byte2:
        while ((data_bytes) % 32 != 0) ++data_bytes;
        data_bytes += sizeof(uint16_t) * table_portion[i].size() + 32;  /// 2 * 8 for min max, 16 for padding
        break;
      case Byte4:
        while ((data_bytes) % 32 != 0) ++data_bytes;
        data_bytes += sizeof(uint32_t) * table_portion[i].size() + 32;  /// 2 * 8 for min max, 16 for padding
        break;
      case Byte8:
        while ((data_bytes) % 32 != 0) ++data_bytes;
        data_bytes += sizeof(uint64_t) * table_portion[i].size() + 32;  /// 2 * 8 for min max, 16 for padding
        break;
      case Singlevalue:
        while ((data_bytes) % 32 != 0) ++data_bytes;
        data_bytes += 32;                                               /// 2 * 8 for min max, 16 for padding
    }
  }
  return data_bytes;
}

/**
* FORMAT:
 * For each char column
 * uint32 : # string
 *
* 0 | uint32 | uint32 | uint32 | uint32 | ...: string end offsets (# string end offsets = # string)
*
* string | string | string | string |...: strings (# strings = # strings, which is super clear)
*/
template <size_t COLUMN_COUNT>
uint64_t DataBlock<COLUMN_COUNT>::get_dictionary_bytes(const std::vector<std::set<std::string>>& str_set_vec) {
  uint64_t dictionary_bytes = 0;
  for (const auto& str_set : str_set_vec) {
    dictionary_bytes += 4;  /// # string

    dictionary_bytes += 4;  /// 0 |
    for (const auto& str : str_set) {
      /// uint32: offset
      dictionary_bytes += 4;

      /// string
      dictionary_bytes += str.length();
    }
  }
  return dictionary_bytes;
}

template <size_t COLUMN_COUNT>
void DataBlock<COLUMN_COUNT>::build_Encoding_SMA(std::vector<EncodingType>& encodings,
                                                 std::vector<SMA>& sma_vector,
                                                 const std::vector<std::vector<uint64_t>> &int_table_portion) {
  /// Loop all the tables
  for (auto& col : int_table_portion) {
    auto min_max_pair = std::minmax_element(col.begin(), col.end());
    sma_vector.emplace_back(*min_max_pair.first, *min_max_pair.second);

    /// differ decides which Encoding we use here
    uint64_t differ = *min_max_pair.second - *min_max_pair.first;
    if (differ == 0) {
      encodings.push_back(Singlevalue);
    } else if (differ < 1 << 8) {
      encodings.push_back(Byte1);
    } else if (differ < 1 << 16) {
      encodings.push_back(Byte2);
    } else if (differ < ((uint64_t)1) << 32) {
      encodings.push_back(Byte4);
    } else {
      encodings.push_back(Byte8);
    }
  }
}


template <size_t COLUMN_COUNT>
std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter>
      DataBlock<COLUMN_COUNT>::build(const std::vector<std::vector<uint64_t>>&    int_table_portion,
                                     const std::vector<std::vector<std::string>>& str_table_portion,
                                     const std::vector<SchemaType>                &schematypes) {
  /// ----------------------------------------------------------------
  /// ----1. Loop table portion to get all the SMA + Encoding---------
  /// ----------------------------------------------------------------

  /// # columns == # schema types
  assert(COLUMN_COUNT == schematypes.size());

  /// # columns == # int columns + # str columns
  assert(COLUMN_COUNT == int_table_portion[0].size() + str_table_portion[0].size());

  /// Vector for all the encodings
  std::vector<EncodingType> encodings;
  encodings.reserve(COLUMN_COUNT);

  /// Vector for all the SMAs
  std::vector<SMA> sma_vector;
  sma_vector.reserve(COLUMN_COUNT);

  /// tuple_count: depends on the int_table_portion and str_table_portion
  /// , because one of them can be empty, when all columns all int or string
  const size_t tuple_count = (int_table_portion.size() == 0) ? str_table_portion[0].size() : int_table_portion[0].size();

  /// # String Columns
  const size_t STR_COL_NUM = str_table_portion.size();

  /// table_portion for string converting into int
  std::vector<std::vector<uint64_t>> int_str_table_portion(STR_COL_NUM, vector<uint64_t>(tuple_count));
  int_str_table_portion.reserve(STR_COL_NUM);

  /// vector of string set: # string sets == # string columns
  /// for calculation the size of for dictionary needed memory
  std::vector<std::set<std::string>> str_set_vec(STR_COL_NUM);

  for (size_t str_table_in = 0; str_table_in < STR_COL_NUM; str_table_in++) {
    const auto& str_col = str_table_portion[str_table_in];

    /// 1.1 String Set to get all the strings / Sort the String Set
    std::set<std::string> string_set(str_col.begin(), str_col.end());

    str_set_vec[str_table_in] = string_set;

    /// 1.2 Map Sorted string with a increasing number
    ///     uint64_t: because of int_table_portion
    size_t set_counter = 0;
    std::unordered_map<std::string, uint64_t> str_int_map;
    for (const auto& str : string_set) {
        str_int_map[str] = set_counter;
        set_counter++;
    }
    assert(set_counter == string_set.size());

    /// 1.3 Encoding string vector to uint32 vector
    auto& str_int_col = int_str_table_portion[str_table_in];
    for (size_t str_in = 0; str_in < str_col.size(); str_in++) {
      str_int_col[str_in] = str_int_map[str_col[str_in] /* the string*/] /* the mapped int to the string */;
    }
  }

  /// 1.4 To build a whole table_portion containing all the columns: int + string
  std::vector<std::vector<uint64_t>> all_table_portion(COLUMN_COUNT, vector<uint64_t>(tuple_count));

  for (size_t in = 0, int_in = 0, str_in = 0; in < COLUMN_COUNT; in++) {
    const auto& schematype = schematypes[in];

    if (schematype == SchemaType::Interger) {
      /// Copy Int Vector into all_table_portion
      all_table_portion[in] = int_table_portion[int_in];
      int_in++;
    } else {
      /// Copy Str_int Vector into all_table_portion
      all_table_portion[in] = int_str_table_portion[str_in];
      str_in++;
    }
  }

  /// 1.5 The whole table_portion containing all the columns: int + string built
  ///     The Ordering matches with the schematype: the same ordering as in the SCHEMA
  ///     But all_table_portion is just a int vector, that we can apply our Int Encoding and SMA


  /// Helper function to figure out the encoding and the SMA
  build_Encoding_SMA(encodings, sma_vector, all_table_portion);

  /// ----------------------------------------------------------------
  /// ---2. Calculate the size of Data　Block and aligned alloc it----
  /// ----------------------------------------------------------------
  /// data_bytes
  const uint64_t data_bytes = get_data_bytes(all_table_portion, encodings);

  /// dictionary_bytes
  const uint64_t dictionary_bytes = get_dictionary_bytes(str_set_vec);

  /// to calculate
  /// 4: uint32_t tuple_count
  /// + 4 * COLUMN_COUNT: uint32_t data_offsets[COLUMN_COUNT]
  /// + 4: uint32_t stop_offset
  /// + sizeof(EncodingType) * COLUMN_COUNT EncodingType encoding_type[COLUMN_COUNT]
  /// + uint8_t*: alignas(32) uint8_t* data
  const auto sizeof_db = sizeof(DataBlock_Header<COLUMN_COUNT>);

  /// With padding to find next 32 aligned size
  /// to ensure data of DataBlock starts with a 32 aligned address
  uint8_t padding = 0;
  while ((sizeof_db + padding) % 32 != 0) ++padding;

  const uint64_t size_of_data_block = sizeof_db + data_bytes + padding + dictionary_bytes;

  /// aligned alloc the DataBlock
  uint8_t* data_malloc = static_cast<uint8_t*>(std::aligned_alloc(32, size_of_data_block));
  if (data_malloc == NULL) {
    perror("Error allocating memory");
    abort();
  }
  std::memset(data_malloc, 0, size_of_data_block);

  DataBlock<COLUMN_COUNT>* datablock = reinterpret_cast<DataBlock<COLUMN_COUNT>*>(data_malloc);

  datablock->header.tuple_count = tuple_count;
  assert(datablock->header.tuple_count <= TUPLES_PER_DATABLOCK);

  /// Check Data Alignment
  uint64_t offset_data = sizeof_db + padding; /// Just before the first sma
  assert((uintptr_t)reinterpret_cast<void*>(reinterpret_cast<uint8_t *>(datablock) + offset_data) % 32 == 0);

  /// ----------------------------------------------------------------
  /// -----------3. Build this DataBlock step by step-----------------
  /// ----------------------------------------------------------------

  size_t i = 0;
  #ifdef DEBUG_BUILD_HEADER
    int DEBUG_OFFSET_i = 0;
    for (auto& min_max_pair : min_max_pairs) {
      std::cout << "================================" << '\n';
      std::cout << "DataBlock.build() index: " << DEBUG_OFFSET_i << '\n';
      std::cout << "encoding type as enum: " << encodings[DEBUG_OFFSET_i] << '\n';
      std::cout << "min element : " << min_max_pair.first << '\n';
      std::cout << "max element : " << min_max_pair.second<< '\n';
      ++DEBUG_OFFSET_i;
    }
    std::cout << "DataBlock.build() size_of_data_block: " << size_of_data_block
    << "\n         \t \t with column first_count: " << COLUMN_COUNT
    << "\n         \t \t with DataBytes: " << getDataBytes(int_table_portion, encodings)
    << "\n         \t \t with differ: " << size_of_data_block - getDataBytes(int_table_portion, encodings) <<std::endl;
    printf("DataBlock.build() pointer of data_malloc: %p\n", data_malloc);
    printf("DataBlock.build() pointer of datablock->tuple_count: %p\n", &(datablock->tuple_count));
    printf("DataBlock.build() value   of datablock->tuple_count: %d\n", (datablock->tuple_count));
    printf("DataBlock.build() pointer of datablock->data_offsets: %p\n", datablock->data_offsets.data());
    printf("DataBlock.build() pointer of datablock->stop_offset: %p\n", &(datablock->stop_offset));
    printf("DataBlock.build() pointer of datablock->encoding_types: %p\n", datablock->encoding_types.data());
    printf("DataBlock.build() pointer of datablock->encoding_types ending: %p\n", datablock->encoding_types.data() + datablock->encoding_types.size());
    printf("DataBlock.build() pointer of datablock->data: %p\n", datablock->data);
  #endif
  for (auto& col : all_table_portion) {
    datablock->header.encoding_types[i] = encodings[i];
    datablock->header.offsets[i].setSmaOffset(offset_data);

    #ifdef DEBUG_BUILD
      std::cout << "================================" << '\n';
      std::cout << "DataBlock.build() row index: " << i
      << "\n         \t \t with encoding_types: " << encodings[i]
      << "\n         \t \t with col_size: " << col.size()
      << "\n         \t \t with offset_data: " << offset_data << std::endl;
    #endif
    uint8_t* base_ptr = reinterpret_cast<uint8_t *>(datablock);

    /// 3.1 Starting with SMA
    /// BEFORE we had build a SMA Vector when we traverse the table first time
    /// Now we just copy this SMA Vector's Entry (Single SMA) into the DataBlock
    std::memcpy(base_ptr + offset_data, &(sma_vector[i]), sizeof(SMA));
    auto ss = sizeof(SMA);

    /// reinterpret_cast this pointer to a SMA object
    SMA* sma = reinterpret_cast<SMA*>(base_ptr + offset_data);
    uint64_t sma_min = sma->getSMA_min();

    /// Update the offset
    /// 16 Byte for SMA, 16 Byte padding
    /// SMA: the first 2 * 64 bits = 2 * 8B = 16B are the min max then come the real data
    /// Then come 16B padding (16B + 16B = 32B) let the real data 32 aligned
    offset_data += sizeof(SMA) + 16 /* padding 16 Byte to let it 32 aligned*/;

    /// With padding to find next 32 aligned size
    /// to ensure data of DataBlock starts with a 32 aligned address
    while ((offset_data) % 32 != 0) ++offset_data;  /// This step is also considered at the get_data_bytes function
    datablock->header.offsets[i].setDataOffset(offset_data);
    assert((uintptr_t)reinterpret_cast<void*>(base_ptr + offset_data) % 32 == 0);

    /// 3.2 Calculate all the (value - SMA.min)
    /// the col is const, so make a local copy of col then substract the min
    auto col_copy = col;
    for (auto &ele : col_copy) ele -= sma_min;

    /// 3.3 Regarding the EncodingType, write all (value - SMA.min) into DataBlock
    switch (encodings[i]) {
      case Byte1: {
        std::copy(col_copy.begin(), col_copy.end(), base_ptr + offset_data);
        offset_data += sizeof(uint8_t) * col.size();
      }
        break;
      case Byte2: {
        uint16_t* ptr = reinterpret_cast<uint16_t*>(base_ptr + offset_data);
        std::copy(col_copy.begin(), col_copy.end(), ptr);
        offset_data += sizeof(uint16_t) * col.size();
      }
        break;
      case Byte4: {
        uint32_t* ptr = reinterpret_cast<uint32_t*>(base_ptr + offset_data);
        std::copy(col_copy.begin(), col_copy.end(), ptr);
        offset_data += sizeof(uint32_t) * col.size();
      }
        break;
      case Byte8: {
        uint64_t* ptr = reinterpret_cast<uint64_t*>(base_ptr + offset_data);
        std::copy(col_copy.begin(), col_copy.end(), ptr);
        offset_data += sizeof(uint64_t) * col.size();
      }
        break;
      case Singlevalue: {
        /// For singlevalue is a exception
        /// min: for the value (singlevalue)
        /// max: the times this singlevalue appears (same as the tuple count or column size)
        sma->setSMA_max(col.size());

        /// Offset is already updated before
      }
    }
    ++i;
  }
  /// base_ptr + offset_data: The first empty address

  #ifdef DEBUG_BUILD
    std::cout << "DataBlock.build() final offsets: " << std::endl;
    for (size_t in = 0; in < COLUMN_COUNT; ++in) std::cout << "i: " << in << "  offset: " << datablock->data_offsets[in] << " | ";
    std::cout << "\nDataBlock.build() final tupel_count: " << datablock->tuple_count << std::endl;
  #endif

  /// ----------------------------------------------------------------
  /// ------------- 4. Write all Dictionaries at end -----------------
  /// ----------------------------------------------------------------
  /**
  * FORMAT:
   * uint32 : # string
   *
  * uint32 | uint32 | uint32 | uint32 | ...: string end offsets (# string end offsets = # string)
  *
  * string | string | string | string |...: strings (# strings = # strings, which is super clear)
  */
  uint8_t* base_ptr = reinterpret_cast<uint8_t *>(datablock);
  for (size_t str_set_vec_in = 0; str_set_vec_in < str_set_vec.size(); str_set_vec_in++) {
    /// find the real index of this columns
    uint32_t column_index = 0;
    uint32_t char_counter = 0;
    for (; column_index < COLUMN_COUNT; column_index++) {
      if (schematypes[column_index] == SchemaType::Char) {
        if (char_counter == str_set_vec_in) {
          break;
        } else {
          char_counter++;
        }
      }
    }

    /// uint32 : # string
    /// uint32: string end offsets
    uint32_t offsets = 0;
    uint32_t *ptr = reinterpret_cast<uint32_t *>(base_ptr + offset_data);
    datablock->header.offsets[column_index].setStringOffset(offset_data);

    /// uint32 : # string
    *ptr = str_set_vec[str_set_vec_in].size();
    ptr += 1;
    offset_data += sizeof(uint32_t);

    /// 0 |
    *ptr = 0;
    ptr += 1;
    offset_data += sizeof(uint32_t);

    /// uint32: string end offsets
    for (const auto& str : str_set_vec[str_set_vec_in]) {
      offsets += str.length();
      *ptr = offsets;
      ptr += 1;
      offset_data += sizeof(uint32_t);
    }

    datablock->header.offsets[column_index].setDictOffset(offset_data);
    /// strings
    for (const auto& str : str_set_vec[str_set_vec_in]) {
      /// first string (only # char count here)
      uint8_t* char_ptr = base_ptr + offset_data;
      memcpy(char_ptr, str.c_str(), str.length());
      offset_data += str.length();
    }
  }

  /// all data written, set the stop offset, dictionary included
  datablock->header.stop_offset = offset_data;

  /// ----------------------------------------------------------------
  /// ----------------5. Build finished and return--------------------
  /// ----------------------------------------------------------------
  return std::unique_ptr<DataBlock<COLUMN_COUNT>, DataBlock_deleter>
         (datablock, [](DataBlock<COLUMN_COUNT> *p){ std::free(p); });
}

template <size_t COLUMN_COUNT>
uint32_t DataBlock<COLUMN_COUNT>::scan(std::vector<Predicate> &predicates,
                                       std::array<uint32_t, TUPLES_PER_DATABLOCK>& index_vector) {
  /// Vector for counting the matched index per column
  /// Because a Datablock can have up to TUPLES_PER_DATABLOCK (1 << 16 = 65536) Tuples

  /// A Indicator for first scan ===> if a scan is full valid, we can delay this first scan Indicator in Multi-predicates-scan case
  bool first_scan = true;
  size_t match_counter = 0;

  /// Scan / Loop all the predicates
  for (size_t index = 0; index < predicates.size(); index++) {
    auto& predicate = predicates[index];
    assert(predicate.col_id < COLUMN_COUNT);
    assert(header.offsets[predicate.col_id].getSmaOffset() >= 0);

    /// Get the Base Pointer for THIS COLUMN
    /// starting with SMA(min and max) and padding, 32 Byte together
    uint8_t* column_base_pointer =
        reinterpret_cast<uint8_t *>(this) + header.offsets[predicate.col_id].getSmaOffset();
    SMA* sma_ptr = reinterpret_cast<SMA*>(column_base_pointer);

    /// 32 B =  16 B(SMA) + 16 B (Padding) + 32 aligned additional Padding
    column_base_pointer = reinterpret_cast<uint8_t *>(this) + header.offsets[predicate.col_id].getDataOffset();

#ifdef DEBUG_SCAN
    std::cout << "DataBlock.scan() col: " << predicates.col_id << "  offset:" << data_offsets[predicates.col_id] << std::endl;
#endif

    /// Scan Cases for:
    /// 5 Encoding Types: byte1, byte2, byte4, byte8, singlevalue
    /// 4 Predicates Types: EQUAL, GREATER_THAN, LESS_THAN, BETWEEN
    ///
    /// Main Work deligated to Count Helper Functions

    /// FOR judging if we can jump this Datablock
    const auto[min, max] = sma_ptr->getSMA_min_max();

    /// Check the type
    uint64_t value = 0;
    if (predicate.str.empty()) {
      value = predicate.val;
    } else {
      value = decode_string(predicate.str, predicate.col_id);
      predicate.val = value;
    }

    switch (predicate.compareType /* Predicate Type */) {
        case EQUAL : {
            switch (header.encoding_types[predicate.col_id] /* Encoding Type for this CLOUMN */) {
              case Singlevalue :
                /// all equal: valid
                if (value == min /* single_value */) {
                  /// if initial then all in, if initialed change nothing
                  if (first_scan) {
                    if (predicates.size() == 1) {
                      /// direct return
                      return max /* appear_time */;
                    } else {
                      /// Delay this first_scan
                      continue;
                    }
                  }
                } else {
                  /// all not equal: invalid
                  return 0;
                }
                break;
              case Byte1 :
                /// Initial match with val not in range => then skip
                if (value < min || value > max) return 0;
                if (first_scan) {
                  match_counter = util::first_count<Byte1, EQUAL>(predicate,
                                                                  this->get_tupel_count(),
                                                                  column_base_pointer,
                                                                  index_vector,
                                                                  sma_ptr,
                                                                  0,
                                                                  0);
                } else {
                  match_counter = util::non_first_count<Byte1, EQUAL>(predicate,
                                                                      match_counter,
                                                                      column_base_pointer,
                                                                      index_vector,
                                                                      sma_ptr,
                                                                      0,
                                                                      0,
                                                                      0);
                }
                break;
              case Byte2 :
                /// Initial match with val not in range => then skip
                if (value < min || value > max) return 0;
                if (first_scan) {
                  match_counter = util::first_count<Byte2, EQUAL>(predicate,
                                                                  get_column_data_length(predicate.col_id),
                                                                  column_base_pointer,
                                                                  index_vector,
                                                                  sma_ptr,
                                                                  0,
                                                                  0);
                } else {
                  match_counter = util::non_first_count<Byte2, EQUAL>(predicate,
                                                                      match_counter,
                                                                      column_base_pointer,
                                                                      index_vector,
                                                                      sma_ptr,
                                                                      0,
                                                                      0,
                                                                      0);
                }
                break;
              case Byte4 :
                /// Initial match with val not in range => then skip
                if (value < min || value > max) return 0;
                if (first_scan) {
                  match_counter = util::first_count<Byte4, EQUAL>(predicate,
                                                                  get_column_data_length(predicate.col_id),
                                                                  column_base_pointer,
                                                                  index_vector,
                                                                  sma_ptr,
                                                                  0,
                                                                  0);
                } else {
                  match_counter = util::non_first_count<Byte4, EQUAL>(predicate,
                                                                      match_counter,
                                                                      column_base_pointer,
                                                                      index_vector,
                                                                      sma_ptr,
                                                                      0,
                                                                      0,
                                                                      0);
                }
                break;
              case Byte8 :
                /// Initial match with val not in range => then skip
                if (value < min || value > max) return 0;
                if (first_scan) {
                  match_counter = util::first_count<Byte8, EQUAL>(predicate,
                                                                  get_column_data_length(predicate.col_id),
                                                                  column_base_pointer,
                                                                  index_vector,
                                                                  sma_ptr,
                                                                  0,
                                                                  0);
                } else {
                  match_counter = util::non_first_count<Byte8, EQUAL>(predicate,
                                                                      match_counter,
                                                                      column_base_pointer,
                                                                      index_vector,
                                                                      sma_ptr,
                                                                      0,
                                                                      0,
                                                                      0);
                }
                break;
            }
        }
        break;

        case GREATER_THAN : {
            switch (header.encoding_types[predicate.col_id] /* Encoding Type for this CLOUMN */) {
              case Singlevalue :
                /// all greater than: valid
                if (value < min /* single_value */) {
                  /// if initial then all in, if initialed change nothing
                  if (first_scan) {
                    if (predicates.size() == 1) {
                      /// direct return
                      return max /* appear_time */;
                    } else {
                      /// Delay this first_scan
                      continue;
                    }
                  }
                } else {
                  /// all not greater than: invalid
                  return 0;
                }
                break;
              case Byte1 :
                /// initial match with val not in range => then skip
                if (value >= max) return 0;
                /// all indexes are valid
                if (first_scan && value < min) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte1, GREATER_THAN>(predicate,
                                                                         get_column_data_length(predicate.col_id),
                                                                         column_base_pointer,
                                                                         index_vector,
                                                                         sma_ptr,
                                                                         0,
                                                                         0);
                } else if (value >= min) {
                  match_counter = util::non_first_count<Byte1, GREATER_THAN>(predicate,
                                                                             match_counter,
                                                                             column_base_pointer,
                                                                             index_vector,
                                                                             sma_ptr,
                                                                             0,
                                                                             0,
                                                                             0);
                }
                break;
              case Byte2 :
                /// initial match with val not in range => then skip
                if (value >= max) return 0;
                /// all indexes are valid
                if (first_scan && value < min) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte2, GREATER_THAN>(predicate,
                                                                         get_column_data_length(predicate.col_id),
                                                                         column_base_pointer,
                                                                         index_vector,
                                                                         sma_ptr,
                                                                         0,
                                                                         0);
                } else if (value >= min) {
                  match_counter = util::non_first_count<Byte2, GREATER_THAN>(predicate,
                                                                             match_counter,
                                                                             column_base_pointer,
                                                                             index_vector,
                                                                             sma_ptr,
                                                                             0,
                                                                             0,
                                                                             0);
                }
                break;
              case Byte4 :
                /// initial match with val not in range => then skip
                if (value >= max) return 0;
                /// all indexes are valid
                if (first_scan && value < min) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte4, GREATER_THAN>(predicate,
                                                                         get_column_data_length(predicate.col_id),
                                                                         column_base_pointer,
                                                                         index_vector,
                                                                         sma_ptr,
                                                                         0,
                                                                         0);
                } else if (value >= min) {
                  match_counter = util::non_first_count<Byte4, GREATER_THAN>(predicate,
                                                                             match_counter,
                                                                             column_base_pointer,
                                                                             index_vector,
                                                                             sma_ptr,
                                                                             0,
                                                                             0,
                                                                             0);
                }
                break;
              case Byte8 :
                /// initial match with val not in range => then skip
                if (value >= max) return 0;
                /// all indexes are valid
                if (first_scan && value < min) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte8, GREATER_THAN>(predicate,
                                                                         get_column_data_length(predicate.col_id),
                                                                         column_base_pointer,
                                                                         index_vector,
                                                                         sma_ptr,
                                                                         0,
                                                                         0);
                } else if (value >= min) {
                  match_counter = util::non_first_count<Byte8, GREATER_THAN>(predicate,
                                                                             match_counter,
                                                                             column_base_pointer,
                                                                             index_vector,
                                                                             sma_ptr,
                                                                             0,
                                                                             0,
                                                                             0);
                }
                break;
            }
        }
        break;

        case LESS_THAN : {
            switch (header.encoding_types[predicate.col_id] /* Encoding Type for this CLOUMN */) {
            case Singlevalue :
              /// all less than : valid
              if (value > min /* single_value */) {
                /// if initial then all in, if initialed change nothing
                if (first_scan) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return max /* appear_time */;
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                }
              } else {
                /// all not less than: invalid
                return 0;
              }
              break;
            case Byte1 :
              /// initial match with val not in range => then skip
              if (value <= min) return 0;
              /// all indexes are valid
              if (first_scan && max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return this->get_tupel_count();
                } else {
                  /// Delay this first_scan
                  continue;
                }
              } else if (first_scan) {
                match_counter = util::first_count<Byte1, LESS_THAN>(predicate,
                                                                    this->get_tupel_count(),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
              } else if (max >= value) {
                match_counter = util::non_first_count<Byte1, LESS_THAN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
              }
              break;
            case Byte2 :
              /// initial match with val not in range => then skip
              if (value <= min) return 0;
              /// all indexes are valid
              if (first_scan && max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return this->get_tupel_count();
                } else {
                  /// Delay this first_scan
                  continue;
                }
              } else if (first_scan) {
                match_counter = util::first_count<Byte2, LESS_THAN>(predicate,
                                                                    get_column_data_length(predicate.col_id),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
              } else if (max >= value) {
                match_counter = util::non_first_count<Byte2, LESS_THAN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
              }
              break;
            case Byte4 :
              /// initial match with val not in range => then skip
              if (value <= min) return 0;
              /// all indexes are valid
              if (first_scan && max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return this->get_tupel_count();
                } else {
                  /// Delay this first_scan
                  continue;
                }
              } else if (first_scan) {
                match_counter = util::first_count<Byte4, LESS_THAN>(predicate,
                                                                    get_column_data_length(predicate.col_id),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
              } else if (max >= value) {
                match_counter = util::non_first_count<Byte4, LESS_THAN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
              }
              break;
            case Byte8 :
              /// initial match with val not in range => then skip
              if (value <= min) return 0;
              /// all indexes are valid
              if (first_scan && max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return this->get_tupel_count();
                } else {
                  /// Delay this first_scan
                  continue;
                }
              } else if (first_scan) {
                match_counter = util::first_count<Byte8, LESS_THAN>(predicate,
                                                                    get_column_data_length(predicate.col_id),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
              } else if (max >= value) {
                match_counter = util::non_first_count<Byte8, LESS_THAN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
              }
              break;
            }
        }
        break;

        case BETWEEN : {
          /// Check the prediate type
          uint64_t right_value = 0;
          if (predicate.str.empty() && predicate.right_str.empty()) {
            right_value = predicate.right_value;
          } else {
            right_value = decode_string(predicate.right_str, predicate.col_id);
            predicate.right_value = right_value;
          }

            switch (header.encoding_types[predicate.col_id] /* Encoding Type for this CLOUMN */) {
            case Singlevalue :
              /// all between: valid
              if (value < min /* single_value */ && min /* single_value */ < predicate.right_value) {
                /// if initial then all in, if initialed change nothing
                if (first_scan) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return max /* appear_time */;
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                }
              } else {
                /// all not equal: invalid
                return 0;
              }
              break;
              case Byte1 :
                /// initial match with val not in range => then all invalid
                if (right_value < min || value /*left value*/ > max) return 0;
                /// all indexes are valid
                if (first_scan && value /*left value*/ <= min && right_value >= max) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte1, BETWEEN>(predicate,
                                                                    this->get_tupel_count(),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
                } else if (value /*left value*/ > min || right_value < max) {
                  match_counter = util::non_first_count<Byte1, BETWEEN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
                }
                break;
              case Byte2 :
                /// initial match with val not in range => then all invalid
                if (right_value < min || value /*left value*/ > max) return 0;
                /// all indexes are valid
                if (first_scan && value /*left value*/ <= min && right_value >= max) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte2, BETWEEN>(predicate,
                                                                    get_column_data_length(predicate.col_id),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
                } else if (value /*left value*/ > min || right_value < max) {
                  match_counter = util::non_first_count<Byte2, BETWEEN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
                }
                break;
              case Byte4 :
                /// initial match with val not in range => then all invalid
                if (right_value < min || value /*left value*/ > max) return 0;
                /// all indexes are valid
                if (first_scan && value /*left value*/ <= min && right_value >= max) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte4, BETWEEN>(predicate,
                                                                    get_column_data_length(predicate.col_id),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
                } else if (value /*left value*/ > min || right_value < max) {
                  match_counter = util::non_first_count<Byte4, BETWEEN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
                }
                break;
              case Byte8 :
                /// initial match with val not in range => then all invalid
                if (right_value < min || value /*left value*/ > max) return 0;
                /// all indexes are valid
                if (first_scan && value /*left value*/ <= min && right_value >= max) {
                  if (predicates.size() == 1) {
                    /// direct return
                    return this->get_tupel_count();
                  } else {
                    /// Delay this first_scan
                    continue;
                  }
                } else if (first_scan) {
                  match_counter = util::first_count<Byte8, BETWEEN>(predicate,
                                                                    get_column_data_length(predicate.col_id),
                                                                    column_base_pointer,
                                                                    index_vector,
                                                                    sma_ptr,
                                                                    0,
                                                                    0);
                } else if (value /*left value*/ > min || right_value < max) {
                  match_counter = util::non_first_count<Byte8, BETWEEN>(predicate,
                                                                        match_counter,
                                                                        column_base_pointer,
                                                                        index_vector,
                                                                        sma_ptr,
                                                                        0,
                                                                        0,
                                                                        0);
                }
                break;
            }
        }
        break;
    }

    /// Until here the predicate without any match
    /// -> none matched possible at all (Because Scan with predicates are LOGIC AND connected)
    if (match_counter == 0) return 0;
    first_scan = false;
  }
  return match_counter;
}

template <size_t COLUMN_COUNT>
template<std::size_t SIZE>
std::vector<std::set<std::string>> DataBlock<COLUMN_COUNT>::decode_dictionary(const std::array<uint8_t , SIZE>& str_index_arr) {
  /**
  * FORMAT:
  * uint32 : # string
  *
  * 0 | uint32 | uint32 | uint32 | uint32 | ...: string end offsets (# string end offsets = # string)
  *
  * string | string | string | string |...: strings (# strings = # strings, which is super clear)
  */
  /// 0. Preparation
  std::vector<std::set<std::string>> str_set_vec(str_index_arr.size());

  uint8_t* base_ptr = reinterpret_cast<uint8_t *>(this);

  /// ----------------------------------------------------------------
  /// ------------- 1. Get all Dictionaries at end -----------------
  /// ----------------------------------------------------------------
  size_t str_set_vec_in = 0;
  for (const auto& str_index : str_index_arr) {
    /// 1.1 Init a string vector
    std::vector<std::string> str_vec(0);

    /// 1.2 Get string_offset
    const auto string_offset = this->header.offsets[str_index].getStringOffset();
    uint32_t* string_offset_ptr = reinterpret_cast<uint32_t*>(base_ptr + string_offset);

    /// 1.3 Get dict_offset
    const auto dict_offset = this->header.offsets[str_index].getDictOffset();
    const uint8_t* dict_offset_ptr = reinterpret_cast<uint8_t*>(base_ptr + dict_offset);

    /// 1.4 Get uint32 : # string
    const uint32_t str_num = *string_offset_ptr;
    string_offset_ptr++;

    /// 0 |
    string_offset_ptr++;

    for (size_t str_counter = 0; str_counter < str_num; str_counter++) {
      uint32_t offset_l = (str_counter == 0)? 0 : *(string_offset_ptr - 1);
      uint32_t offset_r = *string_offset_ptr;
      std::string str(dict_offset_ptr + offset_l, dict_offset_ptr + offset_r);
      str_vec.push_back(str);
      string_offset_ptr++;
    }

    /// 1.5 string vector -> string set
    std::set<std::string> str_set(str_vec.begin(), str_vec.end());
    str_set_vec[str_set_vec_in] = str_set;
    str_set_vec_in++;
  }
  return str_set_vec;
}

template <size_t COLUMN_COUNT>
size_t DataBlock<COLUMN_COUNT>::decode_string(const std::string& str, uint8_t column_id) {
  uint8_t *base_ptr = reinterpret_cast<uint8_t *>(this);

  /// 1.1 Get string_offset
  const auto string_offset = this->header.offsets[column_id].getStringOffset();
  uint32_t *string_offset_ptr =
      reinterpret_cast<uint32_t *>(base_ptr + string_offset);

  /// 1.2 Get dict_offset
  const auto dict_offset = this->header.offsets[column_id].getDictOffset();
  const uint8_t *dict_offset_ptr =
      reinterpret_cast<uint8_t *>(base_ptr + dict_offset);

  /// 1.3 Get uint32 : # string
  const uint32_t str_num = *string_offset_ptr;
  /// number of iteration with binary search
  const uint32_t iter_num = ((str_num) >> 1);
  size_t str_id = iter_num;
  string_offset_ptr++;

  /// pointing at the string in the middle
  string_offset_ptr += iter_num;

  for (size_t i = 0; i <= iter_num; i++) {
  label1:

    uint32_t offset_l = *(string_offset_ptr);
    uint32_t offset_r = *(string_offset_ptr + 1);
    for (size_t it = 0; it < offset_r - offset_l; it++) {
      const char str_dic = *(dict_offset_ptr + offset_l + it);
      if (str[it] > str_dic) {
        string_offset_ptr++;
        str_id++;
        i++;
        goto label1;
      } else if (str[it] < str_dic) {
        string_offset_ptr--;
        str_id--;
        i++;
        goto label1;
      }
    }
    return str_id;

    /// NO SUCH A String
    throw "NO SUCH A String";
  }
}
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_DATABLOCK_DATABLOCK_H_
// ---------------------------------------------------------------------------------------------------

