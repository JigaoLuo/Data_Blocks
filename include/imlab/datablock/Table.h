// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_DATABLOCK_TABLE_H_
#define INCLUDE_IMLAB_DATABLOCK_TABLE_H_
// ---------------------------------------------------------------------------------------------------
#include <cstdint>
#include <functional>
#include <sstream>
#include <memory>
#include <utility>
#include <vector>
#include "imlab/datablock/DataBlock.h"
// ---------------------------------------------------------------------------------------------------
namespace imlab {
/**
 * A table can store any number tuples for Datablock
 */
template<int COLUMN_COUNT>
struct Table_Datablock {
std::vector<std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter>> data_blocks;


/**
 * Scan function to get a index vector
 */
uint64_t scan_count(std::vector<Predicate>& predicates,
                    std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors,
                    uint32_t* matches_index_vector) {
  uint64_t result = 0;
  for (size_t i = 0; i < data_blocks.size(); i++) {
    const auto &data_block = data_blocks[i];
    auto matches = data_block.get()->scan(predicates, *(index_vectors + i));
    *(matches_index_vector + i) = matches;
    result += matches;
  }

  /// Sum up all number of matches in all Index Vectors
  return result;
}

/**
 * Materialization Function
 */
 //TODO: write into DB or CM, because the encodeing is always different between DBs and CMs
template<int COLUMN_ID, EncodingType E>
void materialize(std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors,
                 uint32_t* matches_index_vector,
                 uint8_t* materialized) {
  const size_t SIZE = 1 << E;

  for (size_t i = 0; i < data_blocks.size(); i++) {
    /// matches: The # Matchtes on this Datablock
    /// if 0, then no matches on this Datablock => no need to materialize
    const auto matches = *(matches_index_vector + i);
    if (matches == 0)
      continue;

    const auto matches_bytes = matches << E; /// Not for Singlevalue
    const auto &data_block = data_blocks[i];

    /// Get the Base Pointer for THIS COLUMN
    /// starting with SMA(min and max) and padding, 32 Byte together

    ///SMA
    uint8_t* column_base_pointer = reinterpret_cast<uint8_t *>(data_block.get()) + data_block->get_SMA_offset(COLUMN_ID);
    SMA* sma_ptr = reinterpret_cast<SMA*>(column_base_pointer);
    const auto sma_min = sma_ptr->getSMA_min();

    /// Column Data
    column_base_pointer = reinterpret_cast<uint8_t *>(data_block.get()) + data_block->get_column_offset(COLUMN_ID);  /// 32 B =  16 B(SMA) + 16 B (Padding)

    /// The size offset for copying into materialized MEMORY
    /// For this Datablock
    if (E != 4) { /// Singlevalue == 4
      const auto &index_vector = *(index_vectors + i);
      for (uint32_t matches_index = 0, copy_size = 0; matches_index < matches; matches_index++, copy_size += SIZE) {
        const auto index = index_vector[matches_index];
        memcpy(materialized + copy_size, column_base_pointer + (index << E), SIZE);
      }
    }

    if (E == 0 && sma_min != 0) {
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized + scalar_i) += sma_min;
      }
    } else if (E == 1 && sma_min != 0) {
      uint16_t *materialized_16 = reinterpret_cast<uint16_t *>(materialized);
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_16 + scalar_i) += sma_min;
      }
    } else if (E == 2 && sma_min != 0) {
      uint32_t *materialized_32 = reinterpret_cast<uint32_t *>(materialized);
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_32 + scalar_i) += sma_min;
      }
    } else if (E == 3 && sma_min != 0) {
      uint64_t *materialized_64 = reinterpret_cast<uint64_t *>(materialized);
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_64 + scalar_i) += sma_min;
      }
    } else if (E == 4) {
      /// Singlevalue == 4
      uint64_t *materialized_64 = reinterpret_cast<uint64_t *>(materialized);
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_64 + scalar_i) +=
            sma_min; /// sma_min is the Singlevalue
      }
      /// Adjust the pointer: for Copying into the next datablock
      materialized = materialized + (matches << 3);
      return;
    }
    /// Adjust the pointer: for Copying into the next datablock
    materialized = materialized + matches_bytes;
  }
}

/**
 * Add a Datatblock
 */
void AddDatablock(std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> data_block_ptr) {
  data_blocks.push_back(std::move(data_block_ptr));
}
};


/**
 * A table can store any number tuples for Continous Memory
 */
template<int COLUMN_COUNT>
struct Table_CM {
std::vector<Continuous_Memory_Segment<COLUMN_COUNT>> cms;

/**
 * Scan function to get a index vector
 */
uint64_t scan_count(std::vector<Predicate>& predicates, std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors, uint32_t* matches_index_vector) {
  uint64_t result = 0;
  for (size_t i = 0; i < cms.size(); i++) {
    auto &cm = cms[i];
    auto matches = cm.cm_scan(predicates, *(index_vectors + i));
    *(matches_index_vector + i) = matches;
    result += matches;
  }
  return result;
}

/**
 * Materialization Function
 */
template<int COLUMN_ID, EncodingType E>
void materialize(std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors,
                 uint32_t* matches_index_vector,
                 uint8_t* materialized) {
  const size_t SIZE = 1 << E;

  for (size_t i = 0; i < cms.size(); i++) {
    /// matches: The # Matchtes on this Datablock
    /// if 0, then no matches on this Datablock => no need to materialize
    const auto matches = *(matches_index_vector + i);
    if (matches == 0)
      continue;

    const auto matches_bytes = matches << E;
    auto &cm = cms[i];

    /// Same preparation as the CM_scan
    const auto column_infos = cm.get_column_infos();
    const Column_Info &column_info = column_infos[COLUMN_ID];
    const SMA sma = column_info.sma;
    const auto sma_min = sma.getSMA_min();
    assert(&sma); /// No empty SMA!
    const auto start_page_id = column_info.start_page_id;
    const auto end_page_id = column_info.end_page_id;

    /// The index offset in the Datablock read by the pages
    /// (each page have # Entries in Datablock, we accumulate this #)
    uint32_t index_offset = 0;

    uint32_t reader_offset = 0;

    /// Each time the upper bound of the indexes to be check by the NON FIRST SCAN
    uint32_t global_upper_bound = 0;

    /// offset to copy in materialized
    uint32_t copy_size = 0;

    const auto &index_vector = *(index_vectors + i);

    /// A Column may have many pages
    for (size_t fix_page_id = start_page_id; fix_page_id <= end_page_id; ++fix_page_id) {
      /// 1.1: Fix the Bufferframe, which contains this COLUMN
      auto &bufferframe = cm.buffer_manager.fix_page(fix_page_id, false);

      /// 1.2: Get the Base Pointer for THIS COLUMN
      uint8_t *column_base_pointer = reinterpret_cast<uint8_t *>(bufferframe.get_data());

      /// 1.3: Init data length with Page Size and calculate the data-length
      uint32_t dataLength = PAGE_SIZE /*default value: a page with full data from a column*/;

      /// Calucate the real data length of this Column
      const auto start_offset_in_startpage = column_info.start_offset_in_startpage;
      const auto end_offset_in_endpage     = column_info.end_offset_in_endpage;

      if (fix_page_id == start_page_id) { /// starts in this page
        /// starting with SMA(min and max) and padding, 32 Byte together
        assert(*(reinterpret_cast<SMA *>(column_base_pointer + start_offset_in_startpage)) /*sma found in DataBlock*/ == sma);
        column_base_pointer += (start_offset_in_startpage + 32 /* 16 B(SMA) + 16 B (Padding) */);

        if (start_page_id == end_page_id) { /// starts and ends in the same page
          dataLength = end_offset_in_endpage - start_offset_in_startpage - 32 /* 16 B(SMA) + 16 B (Padding) */;
        } else {
          dataLength = PAGE_SIZE - start_offset_in_startpage - 32;
        }
      } else if (fix_page_id == end_page_id) { /// only ends in this page
        dataLength = end_offset_in_endpage;
      }

      /// # Entries in this page (Maybe only a part of this Datablock)
      const uint32_t number_entries = dataLength >> E;

      /// the upper bound of index in this index vector IN THIS PAGE
      uint32_t upper_bound = std::upper_bound(index_vector.begin() + global_upper_bound, index_vector.begin() + matches, index_offset + number_entries - 1) - index_vector.begin();

      /// The size offset for copying into materialized MEMORY
      /// For this Page
      if (E != 4) { /// Singlevalue == 4
        for (size_t counter = 0; counter < upper_bound - global_upper_bound; counter++, reader_offset++, copy_size += SIZE) {
          const auto index = index_vector[reader_offset];
          memcpy(materialized + copy_size, column_base_pointer + ((index - index_offset) << E), SIZE);
        }
      }

      global_upper_bound = upper_bound;

      /// Unfix this page as bufferframe
      cm.buffer_manager.unfix_page(bufferframe, false /*just scan, not change anything*/);
      index_offset += number_entries;
    }

    if (E == 0 && sma_min != 0) {
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized + scalar_i) += sma_min;
      }
    } else if (E == 1 && sma_min != 0) {
      uint16_t* materialized_16 = reinterpret_cast<uint16_t*>(materialized);
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_16 + scalar_i) += sma_min;
      }
    } else if (E == 2 && sma_min != 0) {
      uint32_t* materialized_32 = reinterpret_cast<uint32_t*>(materialized);
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_32 + scalar_i) += sma_min;
      }
    } else if (E == 3 && sma_min != 0) {
      uint64_t* materialized_64 = reinterpret_cast<uint64_t*>(materialized);
      /// scan the rest
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_64 + scalar_i) += sma_min;
      }
    } else if (E == 4) {
      /// Singlevalue == 4
      uint64_t* materialized_64 = reinterpret_cast<uint64_t*>(materialized);
      for (uint32_t scalar_i = 0; scalar_i < matches; scalar_i++) {
        *(materialized_64 + scalar_i) += sma_min;  /// sma_min is the Singlevalue
      }
      /// Adjust the pointer: for Copying into the next datablock
      materialized = materialized + (matches << 3);
      return;
    }

    materialized = materialized + matches_bytes;
  }
}

/**
 * Add a ContiMemory
 */
void addCM(Continuous_Memory_Segment<COLUMN_COUNT>& cm) {
  cms.push_back(std::move(cm));
}
};
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
#endif  // INCLUDE_IMLAB_DATABLOCK_TABLE_H_
// ---------------------------------------------------------------------------------------------------

