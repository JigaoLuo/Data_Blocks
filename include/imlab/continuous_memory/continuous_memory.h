// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_CONTINUOUS_MEMORY_CONTINUOUS_MEMORY_H_
#define INCLUDE_IMLAB_CONTINUOUS_MEMORY_CONTINUOUS_MEMORY_H_
// ---------------------------------------------------------------------------------------------------
#include <iostream>
#include <memory>
#include <array>
#include <vector>
#include <algorithm>
#include "imlab/buffer_manage/Segment.h"
#include "imlab/buffer_manage/buffer_manager.h"
#include "imlab/datablock/SMA.h"
#include "imlab/util/DataBlock_Types.h"
#include "imlab/util/Predicate.h"
#include "imlab/util/Count_byte1.h"
#include "imlab/util/Count_byte2.h"
#include "imlab/util/Count_byte4.h"
#include "imlab/util/Count_byte8.h"
#include "imlab/util/Count_singlevalue.h"
#include "imlab/util/Operator.h"
// ---------------------------------------------------------------------------------------------------
//#define DEBUG
// ---------------------------------------------------------------------------------------------------
namespace imlab {
// ---------------------------------------------------------------------------------------------------
using util::equal_to;
using util::greater;
using util::less;
using util::between;
// ---------------------------------------------------------------------------------------------------
using util::EncodingType;
using util::EncodingType::Byte1;
using util::EncodingType::Byte2;
using util::EncodingType::Byte4;
using util::EncodingType::Byte8;
using util::EncodingType::Singlevalue;
using util::DataBlock_Header;
using util::Predicate;
using util::TUPLES_PER_DATABLOCK;
using util::CompareType;
using util::CompareType::BETWEEN;
using util::CompareType::EQUAL;
using util::CompareType::GREATER_THAN;
using util::CompareType::LESS_THAN;
using util::SchemaType;
// ---------------------------------------------------------------------------------------------------
/**
 * A table can store any number tuples
 */
template<size_t COLUMN_COUNT>
class DataBlock;

class Column_Info {
 public:
  SMA sma;

  uint32_t start_page_id = 0;

  uint32_t end_page_id = 0;

  /// Containing the SMA when that it's first page for this Column
  uint64_t start_offset_in_startpage = 0;

  uint64_t end_offset_in_endpage = 0;

  /// Always at the last page
  uint64_t string_offset = 0;

  uint64_t dict_offset = 0;

  /// FOR NOW: only work with Byte8 Encoding
  uint64_t padding_before_SMA = 0;

  EncodingType encoding_type = Byte1;

  SchemaType schematype;

  Column_Info() : sma() {}
};

/**
 *
 * The Segment can manage the buffer, to get part of a Datablock
 * Also be able to cache the Header Information (A bit more than Header) for efficient fixing page
 * Also be able to do scan in different pages like in a continuous memory Datablock
 *
 */
template<size_t COLUMN_COUNT>
class Continuous_Memory_Segment : public Segment {
 private:
  /**
   * Number of records, from DataBlock Header.
   */
  uint32_t tuple_count = 0;

  /**
   * Number of pages
   */
  uint32_t page_number = 0;

  /**
 * Decode from Datablock
 * Dictionary are at the last of the Datablock
 */
  std::vector<std::set<std::string>> dictionaries;

  /**
   * In Datablock we have # COLUMN_COUNT columns.
   * This array is built for FAST getting the information that we possible need.
   * In such way, we can reduce MANY page miss.
   * e.g. We don't have to ALWAYS fix the first page to get the Datablock Header.
   */
  std::array<Column_Info, COLUMN_COUNT> column_infos;

  /**
   * Get ALL the need number of pages for this datablock
   * @param data_block_ptr the Datablock in unique pointer for building the pages
   */
  uint32_t get_total_page_number(size_t datablock_size);

 public:
  /**
   * Constructor
   * @param segment_id Id of the segment that the Continuous_Memory (Datablock) are stored in.
   * @param buffer_manager The buffer manager that should be used by the Continuous_Memory (Datablock) segment.
   */
  Continuous_Memory_Segment(uint16_t segment_id, BufferManager &buffer_manager) : Segment(segment_id, buffer_manager), column_infos() {}

  /**
   * Function for build Bufferframes (Pages) for the Datablock in unique pointer
   * Idea: simple memory copy into the data of the buffer manager
   *
   * @param data_block_ptr the Datablock in unique pointer for building the pages
   */
  template<std::size_t SIZE>
  void build_conti_memory_segment(std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> data_block_ptr,
                                  const std::vector<SchemaType> &schematypes,
                                  const std::array<uint8_t , SIZE>& str_index_arr);

  /**
   * Get ALL the need number of pages for this datablock
   * all padding_before_SMA included
   */
  inline uint32_t get_page_number() { return page_number; }

  /**
   * Get a copy of column_infos
   */
  inline std::array<Column_Info, COLUMN_COUNT> get_column_infos() { auto copy = column_infos; return copy; }

  /**
   * Get a copy of dictionaries
   */
  inline std::vector<std::set<std::string>> get_dictionaries() { auto copy = dictionaries; return copy; }

  /**
   * Scan using some predicates to find the number of tuple, which can satisfy all the predicates
   * @param vector of predicate
   * @param index_vector compile time allocated array representing index vector
   * @return the number of tuple, which can satisfy all the predicates
   *         It should be uint32_t: because 1 << 16 = 65536 > UINT32_MAX = 65535
   *         From Discussion wih Alex
   */
  uint32_t cm_scan(std::vector<Predicate> &predicate, std::array<uint32_t, TUPLES_PER_DATABLOCK>& index_vector);

  /**
   *
   * @tparam ENCODING_SHIFT
   * @tparam CallBack
   * @tparam Operator
   * @param predicate
   * @param index_vector
   * @param scan_function
   * @param first_scan
   * @param op
   * @param first_scan_match_counter the size of col_count after first scan (useful for non first scan)
   * @return first scan # matches
   */
  template<size_t ENCODING_SHIFT, class CallBack, class Operator>
  uint32_t cm_scan_col(const Predicate &predicate,
                       std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                       CallBack scan_function,
                       bool first_scan,
                       Operator op,
                       uint32_t first_scan_match_counter) const;


  template<size_t ENCODING_SHIFT, class CallBack, class Operator>
  uint32_t cm_scan_col_reduce(const Predicate &predicate,
                              std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                              CallBack scan_function,
                              bool first_scan,
                              Operator op,
                              uint32_t first_scan_match_counter) const;

  /**
   * Decode a single string
   * Should be called for Query on String
   */
  size_t decode_string(const std::string& str, uint8_t column_id);

};

template<size_t COLUMN_COUNT>
template<std::size_t SIZE>
void Continuous_Memory_Segment<COLUMN_COUNT>::build_conti_memory_segment(std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> data_block_ptr,
                                                                         const std::vector<SchemaType> &schematypes,
                                                                         const std::array<uint8_t , SIZE>& str_index_arr) {
  /// 0. Calculate how many Bufferframes (pages) do we need for this Datablock

  /// Return the DataBlock size in Byte, dictionary included
  const size_t datablock_size = data_block_ptr.get()->getDataBlockSize();

  this->page_number = 0;   /// still have to add the Padding for each column
  size_t last_page_size = 0;  /// still have to add the Padding for each column

  /// 1. Prepare for Extra Information in Continuous_Memory_Segment

  /// tuple_count
  this->tuple_count = data_block_ptr->header.tuple_count;

  /// pages offset in buffer_manager
  /// In this case we get the next clean page for this NEW datablock
  const size_t used_page_offset = buffer_manager.get_used_page_count();

  /// Dictionary from Datablock
  this->dictionaries = data_block_ptr->decode_dictionary(str_index_arr);

  /// Get the Datablock Header
  DataBlock_Header<COLUMN_COUNT>* header = reinterpret_cast<DataBlock_Header<COLUMN_COUNT>*>(data_block_ptr.get());

  size_t padding_before_SMA_sum = 0;  /// padding_before_SMA_sum so far: for calculation the end page id

  for (size_t counter = 0; counter < COLUMN_COUNT; counter++) {
    auto& column_info = column_infos[counter];

    /// encoding_type
    column_info.encoding_type = header->encoding_types[counter];

    /// schema type
    column_info.schematype = schematypes[counter];

    /// start_page_id
    /// absolute page id in this buffer manager, starting from SMA
    column_info.start_page_id = used_page_offset;
    column_info.start_page_id += ((header->offsets[counter].getSmaOffset() + padding_before_SMA_sum) >> PAGE_SIZE_LOG2) /* relative page id */;

    /// start_offset_in_startpage
    column_info.start_offset_in_startpage = header->offsets[counter].getSmaOffset()
                                            + padding_before_SMA_sum
                                            - ((column_info.start_page_id - used_page_offset) /* relative page id */ << PAGE_SIZE_LOG2);

    /// A small fix for Encoding 8byte:
    /// When SIMD 64byte at page accept 8 * uint64_t (so two SIMD iterations)
    /// but there are some cases, that we just leave 4 Number at the end of page, which not enough to do a SIMD iteration.
    /// SO: for 8byte Encoding must have satisfy this condition: (start_offset_in_startpage + 32Byte(SMA+16Padding)) % 64 = 0
    if (column_info.encoding_type == EncodingType::Byte8) {
      while ((column_info.start_offset_in_startpage + 32) % 64 != 0) {
        column_info.start_offset_in_startpage++;
        if (column_info.start_offset_in_startpage == PAGE_SIZE) {
          column_info.start_offset_in_startpage = 0;
          column_info.start_page_id++;
        }
        column_info.padding_before_SMA++;
      }
    }
    padding_before_SMA_sum += column_info.padding_before_SMA;

    if (counter == COLUMN_COUNT - 1) {
      /// Case for last Column

      /// We consider only the offset of the last data
      /// Without any string_offset and the dictionary
      size_t last_data_offset = 0;
      if (SIZE == 0) {
        /// all int columns, not string columns, so that no string_offset and dictionary
        last_data_offset = header->stop_offset;
      } else {

        uint32_t first_char_column_index = 0;
        for (; first_char_column_index < COLUMN_COUNT; first_char_column_index++) {
          if (schematypes[first_char_column_index] == SchemaType::Char) {
            break;
          }
        }

        /// with string columns, so that with string_offset and dictionary
        last_data_offset = header->offsets[first_char_column_index].getStringOffset();
      }

      /// end_page_id
      /// absolute page id in this buffer manager
      column_info.end_page_id = used_page_offset;
      column_info.end_page_id += (last_data_offset + padding_before_SMA_sum) >> PAGE_SIZE_LOG2 /* relative page id */;

      /// at last column: padding_before_SMA_sum are fixed
      /// Update the page_number and the last_page_size
      /// Check if we are at the last page
      page_number = get_total_page_number(datablock_size + padding_before_SMA_sum);

      last_page_size = datablock_size + padding_before_SMA_sum - ((page_number - 1) << PAGE_SIZE_LOG2);

      assert(column_info.end_page_id == used_page_offset + page_number - 1);

      /// end_offset_in_endpage
      column_info.end_offset_in_endpage = (last_data_offset + padding_before_SMA_sum) - ((column_info.end_page_id - used_page_offset) /* relative page id */ << PAGE_SIZE_LOG2);
    } else {
      /// Case for NOT last Column

      /// end_page_id
      /// absolute page id in this buffer manager
      column_info.end_page_id = used_page_offset;
      column_info.end_page_id += (header->offsets[counter + 1].getSmaOffset() + padding_before_SMA_sum) >> PAGE_SIZE_LOG2 /* relative page id */;

      /// end_offset_in_endpage
      column_info.end_offset_in_endpage = (header->offsets[counter + 1].getSmaOffset()+ padding_before_SMA_sum) - ((column_info.end_page_id - used_page_offset) /* relative page id */ << PAGE_SIZE_LOG2);
    }
  }

  uint8_t* base_pointer = reinterpret_cast<uint8_t *>(data_block_ptr.get());

  /// All sma (Get from Datablock)
  for (size_t counter = 0; counter < COLUMN_COUNT; counter++) {
    auto offset = header->offsets[counter].getSmaOffset();
    SMA* sma_ptr = reinterpret_cast<SMA*>(base_pointer + offset);
    column_infos[counter].sma = *sma_ptr;
  }

  /// 2. Loop page_number times
  /// Each loop fix a page, save this page_size bytes from Datablock into the fixed page, then unfix

  /// Copy Datablock header

  /// Aboulute page id for starting this datablock
  /// This page id should we fix to copy datablock header info into
  const size_t db_start_page_id = column_infos[0].start_page_id;

  auto& bufferframe = buffer_manager.fix_page(db_start_page_id, true);

  std::memcpy(bufferframe.get_data(), base_pointer, header->offsets[0].getSmaOffset());
  base_pointer += (header->offsets[0].getSmaOffset());

  buffer_manager.unfix_page(bufferframe, true);

  /// Copy each Columns
  for (auto& column_info : column_infos) {
    const uint32_t start_page_id = column_info.start_page_id;

    const uint32_t end_page_id = column_info.end_page_id;

    /// Containing the SMA when that it's first page for this Column
    const uint64_t start_offset_in_startpage = column_info.start_offset_in_startpage;

    const uint64_t end_offset_in_endpage = column_info.end_offset_in_endpage;

    for (size_t i = start_page_id; i <= end_page_id; ++i) {
      /// 2.1. fix it as bufferframe
      auto& bufferframe = buffer_manager.fix_page(i, true);

      /// 2.2. memory copy
      if (i == start_page_id && i == end_page_id) {
        /// Start Page and End Page
        /// maybe not Full Copy: end_offset_in_endpage - start_offset_in_startpage
        std::memcpy(bufferframe.get_data() + start_offset_in_startpage, base_pointer, end_offset_in_endpage - start_offset_in_startpage);
        base_pointer += (end_offset_in_endpage - start_offset_in_startpage);
      } else if (i == start_page_id) {
        /// Start Page
        /// maybe not Full Copy: PAGE_SIZE - this column's padding_before_SMA
        std::memcpy(bufferframe.get_data() + start_offset_in_startpage, base_pointer, PAGE_SIZE - start_offset_in_startpage);
        base_pointer += (PAGE_SIZE - start_offset_in_startpage);
      } else if (i == end_page_id) {
        /// the Last Page
        /// Maybe not Full Copy
        std::memcpy(bufferframe.get_data(), base_pointer, end_offset_in_endpage);
        base_pointer += end_offset_in_endpage;
      } else {
        /// Not the Last Page and not the first page
        /// Full Copy: PAGE_SIZE
        std::memcpy(bufferframe.get_data(), base_pointer, PAGE_SIZE);
        base_pointer += (PAGE_SIZE);
      }

      /// 2.3. unfix this page as bufferframe
      buffer_manager.unfix_page(bufferframe, true);
    }
  }

  /// 3. Copy the dictionary at the end of the last page
  auto& last_columninfo = column_infos[COLUMN_COUNT - 1];

  auto& bufferframe_dic = buffer_manager.fix_page(last_columninfo.end_page_id, true);
  uint64_t dic_offset = last_columninfo.end_offset_in_endpage;

  for (size_t str_index_iter = 0; str_index_iter < str_index_arr.size(); str_index_iter++) {
    const auto column_id = str_index_arr[str_index_iter];

    /// 1.1 Get string_and dict offset in Data Block
    const auto string_offset = header->offsets[column_id].getStringOffset();
    const auto dict_offset = header->offsets[column_id].getDictOffset();

    size_t column_dic_end_offset = 0;
    if (str_index_iter == str_index_arr.size() - 1) {
      column_dic_end_offset = header->stop_offset;
    } else {
      column_dic_end_offset = header->offsets[column_id + 1].getStringOffset();
    }

    std::memcpy(bufferframe_dic.get_data() + dic_offset, base_pointer, column_dic_end_offset - string_offset);

    column_infos[column_id].string_offset = dic_offset;
    column_infos[column_id].dict_offset = dic_offset + (dict_offset - string_offset);

    base_pointer += (column_dic_end_offset - string_offset);
    dic_offset   += (column_dic_end_offset - string_offset);

    if (dic_offset > PAGE_SIZE) {
      throw "New page for dictionaries!";
    }
  }

  buffer_manager.unfix_page(bufferframe_dic, true);

  /// 4. Since we call this function with parameter std::move(datablock's unique pointer)
  ///    So the datablock's unique pointer is moved (not exists)
  ///    NOW the datablock in managed by our buffermanager
  ///    AND we can access datablock with fix / unfix
}

template<size_t COLUMN_COUNT>
size_t Continuous_Memory_Segment<COLUMN_COUNT>::decode_string(const std::string& str, uint8_t column_id) {
  auto& last_columninfo = column_infos[COLUMN_COUNT - 1];

  auto& bufferframe_dic = buffer_manager.fix_page(last_columninfo.end_page_id, true);

  /// 1.1 Get string_offset
  const auto string_offset = column_infos[column_id].string_offset;
  uint32_t *string_offset_ptr = reinterpret_cast<uint32_t *>(bufferframe_dic.get_data() + string_offset);

  /// 1.2 Get dict_offset
  const auto dict_offset = column_infos[column_id].dict_offset;
  const uint8_t *dict_offset_ptr = reinterpret_cast<uint8_t *>(bufferframe_dic.get_data() + dict_offset);

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
    buffer_manager.unfix_page(bufferframe_dic, true);
    return str_id;

    /// NO SUCH A String
    throw "NO SUCH A String";
  }
}



template<size_t COLUMN_COUNT>
uint32_t Continuous_Memory_Segment<COLUMN_COUNT>::get_total_page_number(size_t datablock_size) {
  /// datablock_size / PAGE_SIZE (BUT FASTER)
  size_t result = datablock_size >> PAGE_SIZE_LOG2;

  /// For check if we should have a DIGIT after the DIVISION
  size_t reshift = result << PAGE_SIZE_LOG2;

  /// If there are not same: then we should have a DIGIT after DIVISION => up around using ceil function
  /// I aviod any if check (CPU Prediction)
  result += (reshift != datablock_size);

  return result;
}

template<size_t COLUMN_COUNT>
template<size_t ENCODING_SHIFT, class CallBack, class Operator>
uint32_t Continuous_Memory_Segment<COLUMN_COUNT>::cm_scan_col(const Predicate &predicate,
                                                              std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                              CallBack scan_function,
                                                              bool first_scan,
                                                              Operator op,
                                                              uint32_t match_counter) const{
  /// Step0: Prepare the necessary infos
  uint32_t col_count_position_marker = 0;  /// for the case non-first scan

  uint32_t col_id = predicate.col_id;
  const Column_Info& column_info = column_infos[col_id];
  const SMA& sma = column_info.sma;
  assert (&sma); /// No empty SMA!
  const auto start_page_id = column_info.start_page_id;
  const auto end_page_id = column_info.end_page_id;

  /// The index offset in the Datablock read by the pages (each page have # Entries in Datablock, we accumulate this #)
  uint32_t index_offset = 0;

  /// return value: count the # matches for first_scan == # elements in index_vector
  /// For first_scan: This index offset of Index-Vector for each page-scan starts(# of Matches in each page, we accumulate this #)
  /// For non_first_scan: re-check in Index_Vector
  uint32_t col_count_start_index = 0;

  /// Each time the upper bound of the indexes to be check by the NON FIRST SCAN
  uint32_t global_upper_bound = 0;

#ifdef DEBUG
  std::cout << "DEBUG cm_scan_col(): start_page_id: " << start_page_id << "  end_page_id: " << end_page_id << std::endl;
#endif

  for (size_t fix_page_id = start_page_id; fix_page_id <= end_page_id; ++fix_page_id) {
    /// 1.1: Fix the Bufferframe, which contains this COLUMN
    auto& bufferframe = buffer_manager.fix_page(fix_page_id, false);

    /// 1.2: Get the Base Pointer for THIS COLUMN
    uint8_t* column_base_pointer = reinterpret_cast<uint8_t*>(bufferframe.get_data());

    /// 1.3: Init data length with Page Size and calculate the data-length
    uint32_t dataLength = PAGE_SIZE /*default value: a page with full data from a column*/;

    /// Calucate the real data length of this Column
    const auto start_offset_in_startpage = column_info.start_offset_in_startpage;
    const auto end_offset_in_endpage = column_info.end_offset_in_endpage;

    if (fix_page_id == start_page_id) {  /// starts in this page
      /// starting with SMA(min and max) and padding, 32 Byte together
      assert(*(reinterpret_cast<SMA*>(column_base_pointer + start_offset_in_startpage)) /*sma found in DataBlock*/ == sma);
      column_base_pointer += (start_offset_in_startpage + 32 /* 16 B(SMA) + 16 B (Padding) */);

      if (start_page_id == end_page_id) {   /// starts and ends in the same page
        dataLength = end_offset_in_endpage - start_offset_in_startpage - 32 /* 16 B(SMA) + 16 B (Padding) */;
      } else {
        dataLength = PAGE_SIZE - start_offset_in_startpage - 32;
      }
    } else if (fix_page_id == end_page_id) {  /// only ends in this page
      dataLength = end_offset_in_endpage;
    }

    /// # Entries in this page (Propably only a part of this Datablock)
    const uint32_t number_entries = dataLength >> ENCODING_SHIFT;

    /// first_scan == SIMD
    /// this first scan can not be the real first_scan(the real first_scan, can be delayed by the function cm_scan)
    /// so this first scan can not be delayed
    col_count_start_index += scan_function(predicate,
                                           dataLength,
                                           column_base_pointer,
                                           index_vector,
                                           &sma,
                                           index_offset  /* SIMD add to EVERY value from scan*/,
                                           col_count_start_index);

    /// Unfix this page as bufferframe
    buffer_manager.unfix_page(bufferframe, false /*just scan, not change anything*/);
    index_offset += number_entries;
  }
  return col_count_start_index;
}

template<size_t COLUMN_COUNT>
template<size_t ENCODING_SHIFT, class CallBack, class Operator>
uint32_t Continuous_Memory_Segment<COLUMN_COUNT>::cm_scan_col_reduce(const Predicate &predicate,
                                                                    std::array<uint32_t, TUPLES_PER_DATABLOCK> &index_vector,
                                                                    CallBack scan_function,
                                                                    bool first_scan,
                                                                    Operator op,
                                                                    uint32_t match_counter) const{
  /// Step0: Prepare the necessary infos
  uint32_t col_count_position_marker = 0;  /// for the case non-first scan

  uint32_t col_id = predicate.col_id;
  const Column_Info& column_info = column_infos[col_id];
  const SMA& sma = column_info.sma;
  assert (&sma); /// No empty SMA!
  const auto start_page_id = column_info.start_page_id;
  const auto end_page_id = column_info.end_page_id;

  /// The index offset in the Datablock read by the pages (each page have # Entries in Datablock, we accumulate this #)
  uint32_t index_offset = 0;

  /// return value: count the # matches for first_scan == # elements in index_vector
  /// For first_scan: This index offset of Index-Vector for each page-scan starts(# of Matches in each page, we accumulate this #)
  /// For non_first_scan: re-check in Index_Vector
  uint32_t col_count_start_index = 0;

  /// Each time the upper bound of the indexes to be check by the NON FIRST SCAN
  uint32_t global_upper_bound = 0;

#ifdef DEBUG
  std::cout << "DEBUG cm_scan_col(): start_page_id: " << start_page_id << "  end_page_id: " << end_page_id << std::endl;
#endif

  for (size_t fix_page_id = start_page_id; fix_page_id <= end_page_id; ++fix_page_id) {
    /// 1.1: Fix the Bufferframe, which contains this COLUMN
    auto& bufferframe = buffer_manager.fix_page(fix_page_id, false);

    /// 1.2: Get the Base Pointer for THIS COLUMN
    uint8_t* column_base_pointer = reinterpret_cast<uint8_t*>(bufferframe.get_data());

    /// 1.3: Init data length with Page Size and calculate the data-length
    uint32_t dataLength = PAGE_SIZE /*default value: a page with full data from a column*/;

    /// Calucate the real data length of this Column
    const auto start_offset_in_startpage = column_info.start_offset_in_startpage;
    const auto end_offset_in_endpage = column_info.end_offset_in_endpage;

    if (fix_page_id == start_page_id) {  /// starts in this page
      /// starting with SMA(min and max) and padding, 32 Byte together
      assert(*(reinterpret_cast<SMA*>(column_base_pointer + start_offset_in_startpage)) /*sma found in DataBlock*/ == sma);
      column_base_pointer += (start_offset_in_startpage + 32 /* 16 B(SMA) + 16 B (Padding) */);

      if (start_page_id == end_page_id) {   /// starts and ends in the same page
        dataLength = end_offset_in_endpage - start_offset_in_startpage - 32 /* 16 B(SMA) + 16 B (Padding) */;
      } else {
        dataLength = PAGE_SIZE - start_offset_in_startpage - 32;
      }
    } else if (fix_page_id == end_page_id) {  /// only ends in this page
      dataLength = end_offset_in_endpage;
    }

    /// # Entries in this page (Propably only a part of this Datablock)
    const uint32_t number_entries = dataLength >> ENCODING_SHIFT;

    /// first_scan == SIMD
    /// this first scan can not be the real first_scan(the real first_scan, can be delayed by the function cm_scan)

    /// the upper bound of index in this index vector IN THIS PAGE
    uint32_t upper_bound = std::upper_bound(index_vector.begin() + global_upper_bound, index_vector.begin() + match_counter, index_offset + number_entries - 1) - index_vector.begin();

    col_count_start_index += scan_function(predicate,
                                           upper_bound - global_upper_bound,
                                           column_base_pointer,
                                           index_vector,
                                           &sma,
                                           index_offset  /* SIMD add to EVERY value from scan*/,
                                           col_count_start_index /* writer */,
                                           global_upper_bound /* reader */);
    global_upper_bound = upper_bound;

    /// Unfix this page as bufferframe
    buffer_manager.unfix_page(bufferframe, false /*just scan, not change anything*/);
    index_offset += number_entries;
  }
  return col_count_start_index;
}

template<size_t COLUMN_COUNT>
uint32_t Continuous_Memory_Segment<COLUMN_COUNT>::cm_scan(std::vector<Predicate> &predicates,
                                                          std::array<uint32_t, TUPLES_PER_DATABLOCK>& index_vector) {
  /// Vector for counting the matched index per column, (UINT32_MAX for invalid, which doesn't count)
  /// Because a Datablock can have up to TUPLES_PER_DATABLOCK (1 << 16 = 65536) Tuples
  /// UINT32_MAX = 1 << 32 = 4294967296 can not be reached
  bool first_scan = true;
  uint32_t match_count = 0;

  /// Scan / Loop all the predicates
  for (size_t index = 0; index < predicates.size(); index++) {
    auto& predicate = predicates[index];
    uint32_t col_id = predicate.col_id;
    const Column_Info& column_info = column_infos[col_id];

    assert(col_id < COLUMN_COUNT);

    /// Scan Cases for:
    /// 5 Encoding Types: byte1, byte2, byte4, byte8, singlevalue
    /// 4 Predicates Types: EQUAL, GREATER_THAN, LESS_THAN, BETWEEN
    ///
    /// Main Work deligated to Count Helper Functions

    /// FOR judging if we can jump this Datablock
    const auto[min, max] = column_info.sma.getSMA_min_max();

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
        switch (column_info.encoding_type /* Encoding Type for this CLOUMN */) {
          case Singlevalue :
            /// all equal: valid
            if (value == min /* single_value */) {
              /// if initial then all in, if initialed change nothing
              if (predicates.size() == 1) {
                /// direct return
                return max /* appear_time */;
              } else {
                /// Delay this scan / first_scan
                continue;
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
              match_count = cm_scan_col<0>(predicate, index_vector, util::first_count<Byte1, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            } else {
              match_count = cm_scan_col_reduce<0>(predicate, index_vector, util::non_first_count<Byte1, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            }
            break;
          case Byte2 :
            /// Initial match with val not in range => then skip
            if (value < min || value > max) return 0;
            if (first_scan) {
              match_count = cm_scan_col<1>(predicate, index_vector, util::first_count<Byte2, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            } else {
              match_count = cm_scan_col_reduce<1>(predicate, index_vector, util::non_first_count<Byte2, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            }
            break;
          case Byte4 :
            /// Initial match with val not in range => then skip
            if (value < min || value > max) return 0;
            if (first_scan) {
              match_count = cm_scan_col<2>(predicate, index_vector, util::first_count<Byte4, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            } else {
              match_count = cm_scan_col_reduce<2>(predicate, index_vector, util::non_first_count<Byte4, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            }
            break;
          case Byte8 :
            /// Initial match with val not in range => then skip
            if (value < min || value > max) return 0;
            if (first_scan) {
              match_count = cm_scan_col<3>(predicate, index_vector, util::first_count<Byte8, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            } else {
              match_count = cm_scan_col_reduce<3>(predicate, index_vector, util::non_first_count<Byte8, EQUAL>, first_scan, equal_to<uint64_t>(), match_count);
            }
            break;
        }
      }
      break;

        case GREATER_THAN : {
          switch (column_info.encoding_type /* Encoding Type for this CLOUMN */) {
            case Singlevalue :
              /// all greater than: valid
              if (value < min /* single_value */) {
                /// if initial then all in, if initialed change nothing
                if (predicates.size() == 1) {
                  /// direct return
                  return max /* appear_time */;
                } else {
                  /// Delay this scan / first_scan
                  continue;
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
              if (value < min) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<0>(predicate, index_vector, util::first_count<Byte1, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<0>(predicate, index_vector, util::non_first_count<Byte1, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              }
              break;
            case Byte2 :
              /// initial match with val not in range => then skip
              if (value >= max) return 0;
              /// all indexes are valid
              if (value < min) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<1>(predicate, index_vector, util::first_count<Byte2, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<1>(predicate, index_vector, util::non_first_count<Byte2, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              }
              break;
            case Byte4 :
              /// initial match with val not in range => then skip
              if (value >= max) return 0;
              /// all indexes are valid
              if (value < min) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<2>(predicate, index_vector, util::first_count<Byte4, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<2>(predicate, index_vector, util::non_first_count<Byte4, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              }
              break;
            case Byte8 :
              /// initial match with val not in range => then skip
              if (value >= max) return 0;
              /// all indexes are valid
              if (value < min) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<3>(predicate, index_vector, util::first_count<Byte8, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<3>(predicate, index_vector, util::non_first_count<Byte8, GREATER_THAN>, first_scan, greater<uint64_t>(), match_count);
              }
              break;
          }
        }
            break;

        case LESS_THAN : {
          switch (column_info.encoding_type /* Encoding Type for this CLOUMN */) {
            case Singlevalue :
              /// all less than : valid
              if (value > min /* single_value */) {
                /// if initial then all in, if initialed change nothing
                if (predicates.size() == 1) {
                  /// direct return
                  return max /* appear_time */;
                } else {
                  /// Delay this scan / first_scan
                  continue;
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
              if (max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<0>(predicate, index_vector, util::first_count<Byte1, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<0>(predicate, index_vector, util::non_first_count<Byte1, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
              }
              break;
            case Byte2 :
              /// initial match with val not in range => then skip
              if (value <= min) return 0;
              /// all indexes are valid
              if (max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<1>(predicate, index_vector, util::first_count<Byte2, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<1>(predicate, index_vector, util::non_first_count<Byte2, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
              }
              break;
            case Byte4 :
              /// initial match with val not in range => then skip
              if (value <= min) return 0;
              /// all indexes are valid
              if (max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<2>(predicate, index_vector, util::first_count<Byte4, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<2>(predicate, index_vector, util::non_first_count<Byte4, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
              }
                break;
            case Byte8 :
              /// initial match with val not in range => then skip
              if (value <= min) return 0;
              /// all indexes are valid
              if (max < value) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<3>(predicate, index_vector, util::first_count<Byte8, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<3>(predicate, index_vector, util::non_first_count<Byte8, LESS_THAN>, first_scan, less<uint64_t>(), match_count);
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

          switch (column_info.encoding_type /* Encoding Type for this CLOUMN */) {
            case Singlevalue :
              /// all between: valid
              if (value < min /* single_value */ && min /* single_value */ < predicate.right_value) {
                /// if initial then all in, if initialed change nothing
                if (predicates.size() == 1) {
                  /// direct return
                  return max /* appear_time */;
                } else {
                  /// Delay this scan / first_scan
                  continue;
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
              if (value /*left value*/ <= min && right_value >= max) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<0>(predicate, index_vector, util::first_count<Byte1, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<0>(predicate, index_vector, util::non_first_count<Byte1, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              }
              break;
            case Byte2 :
              /// initial match with val not in range => then all invalid
              if (right_value < min || value /*left value*/ > max) return 0;
              /// all indexes are valid
              if (value /*left value*/ <= min && right_value >= max) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<1>(predicate, index_vector, util::first_count<Byte2, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<1>(predicate, index_vector, util::non_first_count<Byte2, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              }
              break;
            case Byte4 :
              /// initial match with val not in range => then all invalid
              if (right_value < min || value /*left value*/ > max) return 0;
              /// all indexes are valid
              if (value /*left value*/ <= min && right_value >= max) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<2>(predicate, index_vector, util::first_count<Byte4, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<2>(predicate, index_vector, util::non_first_count<Byte4, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              }
              break;
            case Byte8 :
              /// initial match with val not in range => then all invalid
              if (right_value < min || value /*left value*/ > max) return 0;
              /// all indexes are valid
              if (value /*left value*/ <= min && right_value >= max) {
                if (predicates.size() == 1) {
                  /// direct return
                  return tuple_count;
                } else {
                  /// Delay this scan / first_scan
                  continue;
                }
              }
              if (first_scan) {
                match_count = cm_scan_col<3>(predicate, index_vector, util::first_count<Byte8, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              } else {
                match_count = cm_scan_col_reduce<3>(predicate, index_vector, util::non_first_count<Byte8, BETWEEN>, first_scan, between<uint64_t>(), match_count);
              }
              break;
          }
        }
        break;
    }
    /// Until here the predicate without any match
    /// -> none matched possible at all (Because Scan with predicates are LOGIC AND connected)
    if (first_scan && match_count == 0) return 0;

    first_scan = false;
  }
  return match_count;
}
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_CONTINUOUS_MEMORY_CONTINUOUS_MEMORY_H_
