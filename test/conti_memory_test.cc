// ---------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------
#include <math.h>
#include <gtest/gtest.h>
#include <vector>
#include <cstdlib>
#include <iostream>
#include <ctime>
#include <tuple>
#include <map>
#include <algorithm>
#include "imlab/datablock/DataBlock.h"
#include "imlab/continuous_memory/continuous_memory.h"
#include "imlab/util/Random.h"
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
#define DEBUG_TEST
// ---------------------------------------------------------------------------------------------------
using imlab::util::Random;
using imlab::DataBlock;
using imlab::BufferManager;
using imlab::BufferFrame;
using imlab::Continuous_Memory_Segment;
using imlab::PAGE_SIZE;
using imlab::util::EncodingType::Byte8;
using imlab::util::SchemaType;
// ---------------------------------------------------------------------------------------------------
/// Maximal number of Bufferframe in Memory
static constexpr const size_t MAX_PAGES_NUM = 4;
// ---------------------------------------------------------------------------------------------------
/// A helper function to check the copy of Datablock(in Memory) and the copy of the buffer managed Datablock (in Disk and Memory)
/// Are they the same?
/// @param datablock the datablock in Memory
template<size_t COLUMN_COUNT, size_t SIZE>
void check_helper_function(std::unique_ptr<DataBlock<COLUMN_COUNT>, typename DataBlock<COLUMN_COUNT>::DataBlock_deleter> datablock,
                           const std::vector<SchemaType> &schematypes,
                           const std::array<uint8_t , SIZE>& str_index_arr) {
  /// Get Datablock size
  //TODO: reconsider it, how it for all datablock's data + dictionary as end
  //TODO: but dictionaries are for scan not relevant
  const size_t db_size = datablock->getDataBlockSize();

  const size_t db_header_size = datablock->getHeadSize();

  /// alloc this size of memory for copying all the datablock
  char* db_copy = reinterpret_cast<char *>(malloc(db_size));
  char* db_copy_copy = db_copy;
  std::memcpy(db_copy, datablock.get(), db_size);

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<COLUMN_COUNT> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  cm.build_conti_memory_segment(std::move(datablock), schematypes, str_index_arr);

  /// Test the page number
  auto page_num = cm.get_page_number();

  /// Test the column infos
  auto column_infos = cm.get_column_infos();

  /// Get th copy of all the data of buffer managed pages

  /// Test the page
  /// Are they containing the same data
  /// padding_before_SMA considered

  /// Test Datablock header
  const size_t db_start_page_id = column_infos[0].start_page_id;
  auto& bufferframe = buffer_manager.fix_page(db_start_page_id, true);
  EXPECT_EQ(std::memcmp(bufferframe.get_data(), db_copy, db_header_size), 0);
  db_copy += (db_header_size);
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
        EXPECT_EQ(0, std::memcmp(bufferframe.get_data() + start_offset_in_startpage, db_copy, end_offset_in_endpage - start_offset_in_startpage));
        db_copy += (end_offset_in_endpage - start_offset_in_startpage);
      } else if (i == start_page_id) {
        /// Start Page
        /// maybe not Full Copy: PAGE_SIZE - this column's padding_before_SMA
        EXPECT_EQ(0, std::memcmp(bufferframe.get_data() + start_offset_in_startpage, db_copy, PAGE_SIZE - start_offset_in_startpage));
        db_copy += (PAGE_SIZE - start_offset_in_startpage);
      } else if (i == end_page_id) {
        /// the Last Page
        /// Maybe not Full Copy
        EXPECT_EQ(0, std::memcmp(bufferframe.get_data(), db_copy, end_offset_in_endpage));
        db_copy += (end_offset_in_endpage);
      } else {
        /// Not the Last Page and not the first page
        /// Full Copy: PAGE_SIZE
        EXPECT_EQ(0, std::memcmp(bufferframe.get_data(), db_copy, PAGE_SIZE));
        db_copy += (PAGE_SIZE);
      }

      /// 2.3. unfix this page as bufferframe
      buffer_manager.unfix_page(bufferframe, true);
    }
  }

  /// free all allocated memory pointers
  free(db_copy_copy);
}
// ---------------------------------------------------------------------------------------------------
//// ---------------------------------------------------------------------------------------------------
/// The tests are in the same way
/// 1. Build a datablock
/// 2. Call the helper function to check equality of Memory Datablock and Buffer Managed Datablock
//// ---------------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------
#ifdef DEBUG_TEST
TEST(Continuous_Memory_Segment, CM_Simple_FixNumberTest) {
  /// Same as TEST(DataBlock, FixNumberTest)
  std::vector<uint64_t> col0 {3, 2, 1, 4, 5, 6, 7, 8, 9, 15};
  std::vector<std::vector<uint64_t>> int_table_portion {col0};

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());
  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_FixNumberTest) {
  std::vector<uint64_t> col0 {3, 2, 1, 4, 5, 6, 7, 8, 9, 70000};
  std::vector<uint64_t> col1 {16, 32, 30, 40, 50, 60, 70, 80, 90, 257};
  std::vector<uint64_t> col2 {257, 257, 257, 257, 257, 257, 257, 257, 257, 257};
  std::vector<uint64_t> col3 {20, 40, 16, 32, 48, 256, 128, 64, 16, 257};
  std::vector<std::vector<uint64_t>> int_table_portion {col0, col1, col2, col3};

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col1.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<4>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, 8bit_Random_Single_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand() >> 56);
  }

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_8bit_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
  col0.push_back(random.Rand() >> 56);
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_16bit_Random_Single_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand() >> 48);
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_16bit_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand() >> 48);
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_32bit_Random_Single_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand() >> 32);
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_32bit_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand() >> 32);
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_64bit_BETWEEN_Random_Single_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand());
  }
  col0[0] = UINT64_MAX;
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_64bit_BETWEEN_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand());
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<1>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_Random_Multi_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<uint64_t>> int_table_portion(4);
  auto &col0 = int_table_portion[0];
  auto &col1 = int_table_portion[1];
  auto &col2 = int_table_portion[2];
  auto &col3 = int_table_portion[3];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand());
    // std::cout << col0[i] << std::endl;
  }
  col1.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col1.push_back(random.Rand());
  }
  col2.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col2.push_back(random.Rand());
  }
  col3.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col3.push_back(random.Rand());
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<4>(std::move(db), schematypes, str_index_arr);
}

TEST(Continuous_Memory_Segment, CM_Random_Multi_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<uint64_t>> int_table_portion(4);
  auto &col0 = int_table_portion[0];
  auto &col1 = int_table_portion[1];
  auto &col2 = int_table_portion[2];
  auto &col3 = int_table_portion[3];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random.Rand());
  }
  col1.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col1.push_back(random.Rand());
  }
  col2.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col2.push_back(random.Rand());
  }
  col3.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col3.push_back(random.Rand());
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 0> str_index_arr{{}};
  check_helper_function<4>(std::move(db), schematypes, str_index_arr);
}
#endif
// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
