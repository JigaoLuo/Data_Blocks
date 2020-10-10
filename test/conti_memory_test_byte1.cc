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
#include "imlab/util/Random.h"
#include "imlab/continuous_memory/continuous_memory.h"
#include "conti_memory_test_helper.cc" // NOLINT

#define DEBUG 1
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::DataBlock;
using imlab::BufferManager;
using imlab::BufferFrame;
using imlab::Continuous_Memory_Segment;
using imlab::PAGE_SIZE;
using imlab::util::SchemaType;
// ---------------------------------------------------------------------------------------------------
static std::string l_shipmode[7] = {"RAIL",
                                    "SHIP",
                                    "MAIL",
                                    "TRUCK",
                                    "FOB",
                                    "REG AIR",
                                    "AIR"};

static std::string l_shipinstruct[4] = {"NONE",
                                        "TAKE BACK RETURN",
                                        "COLLECT COD",
                                        "DELIVER IN PERSON"};
// ---------------------------------------------------------------------------------------------------
#ifdef DEBUG
// ---------------------------------------------------------------------------------------------------
TEST(Continuous_Memory_Segment_Scan, 8bit_EQUAL_l_shipmode_Random_Single_Vector_2_8) {
    /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
    size_t size = 1 << 8;
    std::vector<std::vector<std::string>> str_table_portion(1);
    auto &col0 = str_table_portion[0];
    col0.reserve(size);
    Random random;
    for (size_t i = 0; i < size; ++i) {
        col0.push_back(l_shipmode[(random.Rand() % 6) + 1]);
    }

    for (size_t i = 0; i < 1* size; ++i) {
        col0[random.Rand() % size] = l_shipmode[0];
    }

    std::vector<std::vector<std::uint64_t>> int_table_portion;

    std::vector<SchemaType> schematypes{SchemaType::Char};

    auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
    EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<1> cm(0, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 1> str_index_arr{{0}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    scan_helper_str_unary_predicate<1, CompareType::EQUAL, SHIFT_BITS_BYTE1>(size, str_table_portion, cm, equal_to<std::string>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_EQUAL_l_shipmode_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
  size_t size = 1 << 16;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipmode[random.Rand() % 7]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 1> str_index_arr{{0}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_str_unary_predicate<1, CompareType::EQUAL, SHIFT_BITS_BYTE1>(size, str_table_portion, cm, equal_to<std::string>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_GREATER_THAN_l_shipmode_Random_Single_Vector_2_8) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
  size_t size = 1 << 8;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipmode[random.Rand() % 7]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 1> str_index_arr{{0}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_str_unary_predicate<1, CompareType::GREATER_THAN, SHIFT_BITS_BYTE1>(size, str_table_portion, cm, greater<std::string>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_GREATER_THAN_l_shipmode_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
  size_t size = 1 << 16;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipmode[random.Rand() % 7]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 1> str_index_arr{{0}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_str_unary_predicate<1, CompareType::GREATER_THAN, SHIFT_BITS_BYTE1>(size, str_table_portion, cm, greater<std::string>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_LESS_THAN_l_shipmode_Random_Single_Vector_2_8) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
  size_t size = 1 << 8;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipmode[random.Rand() % 7]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 1> str_index_arr{{0}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_str_unary_predicate<1, CompareType::LESS_THAN, SHIFT_BITS_BYTE1>(size, str_table_portion, cm, less<std::string>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_LESS_THAN_l_shipmode_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
  size_t size = 1 << 16;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipmode[random.Rand() % 7]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 1> str_index_arr{{0}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_str_unary_predicate<1, CompareType::LESS_THAN, SHIFT_BITS_BYTE1>(size, str_table_portion, cm, less<std::string>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_BETWEEN_l_shipmode_Random_Single_Vector_2_8) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
  size_t size = 1 << 8;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipmode[random.Rand() % 7]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 1> str_index_arr{{0}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_str_binary_predicate<1, CompareType::BETWEEN, SHIFT_BITS_BYTE1>(size, str_table_portion, cm);
}

TEST(Continuous_Memory_Segment_Scan, 8bit_BETWEEN_l_shipmode_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
  size_t size = 1 << 16;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipmode[random.Rand() % 7]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 1> str_index_arr{{0}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_str_binary_predicate<1, CompareType::BETWEEN, SHIFT_BITS_BYTE1>(size, str_table_portion, cm);
}

// ---------------------------------------------------------------------------------------------------
TEST(Continuous_Memory_Segment_Scan, 8bit_EQUAL_Random_Single_Vector_2_8) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_8)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_unary_predicate<1, CompareType::EQUAL, SHIFT_BITS_BYTE1>(size, int_table_portion, cm, equal_to<uint64_t>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_EQUAL_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_EQUAL_Random_Single_Vector_2_16)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_unary_predicate<1, CompareType::EQUAL, SHIFT_BITS_BYTE1>(size, int_table_portion, cm, equal_to<uint64_t>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_GREATER_THAN_Random_Single_Vector_2_8) {
  /// Same as TEST(DataBlock, 8bit_GREATER_THAN_Random_Single_Vector_2_8)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_unary_predicate<1, CompareType::GREATER_THAN, SHIFT_BITS_BYTE1>(size, int_table_portion, cm, greater<uint64_t>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_GREATER_THAN_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_GREATER_THAN_Random_Single_Vector_2_16)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_unary_predicate<1, CompareType::GREATER_THAN, SHIFT_BITS_BYTE1>(size, int_table_portion, cm, greater<uint64_t>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_LESS_THAN_Random_Single_Vector_2_8) {
  /// Same as TEST(DataBlock, 8bit_LESS_THAN_Random_Single_Vector_2_8)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_unary_predicate<1, CompareType::LESS_THAN, SHIFT_BITS_BYTE1>(size, int_table_portion, cm, less<uint64_t>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_LESS_THAN_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_LESS_THAN_Random_Single_Vector_2_16)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_unary_predicate<1, CompareType::LESS_THAN, SHIFT_BITS_BYTE1>(size, int_table_portion, cm, less<uint64_t>());
}

TEST(Continuous_Memory_Segment_Scan, 8bit_BETWEEN_Random_Single_Vector_2_8) {
  /// Same as TEST(DataBlock, 8bit_LESS_THAN_Random_Single_Vector_2_8)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_binary_predicate<1, CompareType::BETWEEN, SHIFT_BITS_BYTE1>(size, int_table_portion, cm);
}

TEST(Continuous_Memory_Segment_Scan, 8bit_BETWEEN_Random_Single_Vector_2_16) {
  /// Same as TEST(DataBlock, 8bit_LESS_THAN_Random_Single_Vector_2_16)
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

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);

  /// after std::move: datablock in Memory is destroied
  std::array<uint8_t, 0> str_index_arr{{}};
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  scan_helper_binary_predicate<1, CompareType::BETWEEN, SHIFT_BITS_BYTE1>(size, int_table_portion, cm);
}

TEST(Continuous_Memory_Segment_Scan, cm8bit_EQUAL_Random_Single_Vector_2_16) {
    size_t size = 1 << 16;
    size_t dup_size = 1 << 4;
    std::vector<std::vector<uint64_t>> table_portion(1);
    auto &col0 = table_portion[0];
    col0.reserve(size);
    Random random;
    for (size_t i = 0; i < size - dup_size; ++i) {
        col0.push_back(random.Rand() >> 56);
    }
    for (size_t i = 0; i < dup_size; ++i) {
        col0.push_back(col0[random.Rand() % (size - dup_size - 1)]);
    }

    std::vector<std::vector<std::string>> str_table_portion;

    std::vector<SchemaType> schematypes{SchemaType::Interger};

    auto db = imlab::DataBlock<1>::build(table_portion, str_table_portion, schematypes);

    EXPECT_EQ(col0.size(), db.get()->get_tupel_count());
    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<1> cm(0, buffer_manager);
    std::array<uint8_t, 0> str_index_arr{{}};

    /// after std::move: datablock in Memory is destroied
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    for (size_t i = 0; i < size; ++i) {
        uint64_t random1 = random.Rand() >> 56;
        uint64_t random2 = random.Rand() >> 56;
        uint64_t random3 = random.Rand() >> 56;
        Predicate predicate_1(0, random1, CompareType::EQUAL);
        Predicate predicate_2(0, random2, CompareType::EQUAL);
        Predicate predicate_3(0, random3, CompareType::EQUAL);
        std::vector<Predicate> predicates;
        predicates.push_back(predicate_1);
        predicates.push_back(predicate_2);
        predicates.push_back(predicate_3);
        uint64_t count_if = std::count_if(col0.begin(), col0.end(), [random1, random2, random3](uint64_t entry) { return random1 == entry && random2 == entry && random3 == entry;});
        EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
}

TEST(Continuous_Memory_Segment_Scan, cm8bit_GREATER_THAN_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  size_t dup_size = 1 << 4;
  std::vector<std::vector<uint64_t>> table_portion(1);
  auto &col0 = table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size - dup_size; ++i) {
    col0.push_back(random.Rand() >> 56);
  }
  for (size_t i = 0; i < dup_size; ++i) {
    col0.push_back(col0[random.Rand() % (size - dup_size - 1)]);
  }

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());
  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);
  std::array<uint8_t, 0> str_index_arr{{}};

  /// after std::move: datablock in Memory is destroied
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  for (size_t i = 0; i < size; ++i) {
    uint64_t random1 = random.Rand() >> 56;
    uint64_t random2 = random.Rand() >> 56;
    uint64_t random3 = random.Rand() >> 56;
    Predicate predicate_1(0, random1, CompareType::GREATER_THAN);
    Predicate predicate_2(0, random2, CompareType::GREATER_THAN);
    Predicate predicate_3(0, random3, CompareType::GREATER_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);
    uint64_t count_if = std::count_if(col0.begin(), col0.end(), [random1, random2, random3](uint64_t entry) { return random1 < entry && random2 < entry && random3 < entry;});
    EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
  }
}

TEST(Continuous_Memory_Segment_Scan, cm8bit_LESS_THAN_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  size_t dup_size = 1 << 4;
  std::vector<std::vector<uint64_t>> table_portion(1);
  auto &col0 = table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size - dup_size; ++i) {
    col0.push_back(random.Rand() >> 56);
  }
  for (size_t i = 0; i < dup_size; ++i) {
    col0.push_back(col0[random.Rand() % (size - dup_size - 1)]);
  }

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());
  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);
  std::array<uint8_t, 0> str_index_arr{{}};

  /// after std::move: datablock in Memory is destroied
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  for (size_t i = 0; i < size; ++i) {
    uint64_t random1 = random.Rand() >> 56;
    uint64_t random2 = random.Rand() >> 56;
    uint64_t random3 = random.Rand() >> 56;
    Predicate predicate_1(0, random1, CompareType::LESS_THAN);
    Predicate predicate_2(0, random2, CompareType::LESS_THAN);
    Predicate predicate_3(0, random3, CompareType::LESS_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);
    uint64_t count_if = std::count_if(col0.begin(), col0.end(), [random1, random2, random3](uint64_t entry) { return random1 > entry && random2 > entry && random3 > entry;});
    EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
  }
}

TEST(Continuous_Memory_Segment_Scan, cm8bit_BETWEEN_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  size_t dup_size = 1 << 4;
  std::vector<std::vector<uint64_t>> table_portion(1);
  auto &col0 = table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size - dup_size; ++i) {
    col0.push_back(random.Rand() >> 56);
  }
  for (size_t i = 0; i < dup_size; ++i) {
    col0.push_back(col0[random.Rand() % (size - dup_size - 1)]);
  }

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());
  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);
  std::array<uint8_t, 0> str_index_arr{{}};

  /// after std::move: datablock in Memory is destroied
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  for (size_t i = 0; i < size; ++i) {
    uint64_t random1 = random.Rand() >> 56;
    uint64_t random2 = random.Rand() >> 56;
    uint64_t random3 = random.Rand() >> 56;
    uint64_t random4 = random.Rand() >> 56;
    uint64_t random5 = random.Rand() >> 56;
    uint64_t random6 = random.Rand() >> 56;

    Predicate predicate_1(0, random1, random4, CompareType::BETWEEN);
    Predicate predicate_2(0, random2, random5, CompareType::BETWEEN);
    Predicate predicate_3(0, random3, random6, CompareType::BETWEEN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);

    uint64_t count_if = std::count_if(
        col0.begin(), col0.end(),
        [random1, random2, random3, random4, random5, random6](uint64_t entry) {
          return random1 <= entry && entry <= random4 && random2 <= entry &&
                 entry <= random5 && random3 <= entry && entry <= random6;
        });
    EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
  }
}
#endif
// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
