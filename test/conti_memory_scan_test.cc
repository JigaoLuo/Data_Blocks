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
#include "conti_memory_test_helper.cc" // NOLINT
#define DEBUG
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::DataBlock;
using imlab::BufferManager;
using imlab::BufferFrame;
using imlab::Continuous_Memory_Segment;
using imlab::PAGE_SIZE;
using imlab::TUPLES_PER_DATABLOCK;
using imlab::util::SchemaType;
// ---------------------------------------------------------------------------------------------------
#ifdef DEBUG
TEST(Continuous_Memory_Segment_Scan, FixNumberTest) {
  /// Same as TEST(DataBlock, FixNumberTest)
  std::vector<uint64_t> col0 {3, 2, 1, 4, 5, 6, 7, 8, 9, 15};
  std::vector<std::vector<uint64_t>> table_portion {col0};

  std::vector<std::vector<uint64_t>> int_table_portion {col0};

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  Predicate predicate(0, 257, CompareType::EQUAL);
  std::vector<Predicate> predicates;
  predicates.push_back(predicate);

  /// Continuous_Memory_Segment

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

  /// New a Continuous_Memory_Segment with Segment Id = 0
  Continuous_Memory_Segment<1> cm(0, buffer_manager);
  std::array<uint8_t, 0> str_index_arr{{}};

  /// after std::move: datablock in Memory is destroied
  cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

  EXPECT_EQ(0, cm.cm_scan(predicates, *index_vectors));
}

TEST(Continuous_Memory_Segment_Scan, Multi_Col_FixNumberTest) {
    std::vector<uint64_t> col0 {3, 2, 1, 4, 5, 6, 7, 8, 9, 70000};
    std::vector<uint64_t> col1  {16, 32, 30, 40, 50, 60, 70, 80, 90, 257};
    std::vector<uint64_t> col2 {257, 257, 257, 257, 257, 257, 257, 257, 257, 257};
    std::vector<uint64_t> col3 {20, 40, 16, 32, 48, 256, 128, 64, 16, 257};
    std::vector<std::vector<uint64_t>> table_portion {col0, col1, col2, col3};

    std::vector<std::vector<uint64_t>> int_table_portion {col0, col1, col2, col3};

    std::vector<std::vector<std::string>> str_table_portion;

    std::vector<SchemaType> schematypes{SchemaType::Interger,
                                        SchemaType::Interger,
                                        SchemaType::Interger,
                                        SchemaType::Interger};

    auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);

    EXPECT_EQ(col1.size(), db.get()->get_tupel_count());

    Predicate predicate(2, 257, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);

    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<4> cm(0, buffer_manager);
    std::array<uint8_t, 0> str_index_arr{{}};

    /// after std::move: datablock in Memory is destroied
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    EXPECT_EQ(10, cm.cm_scan(predicates, *index_vectors));
}

TEST(Continuous_Memory_Segment_Scan, Random_Multi_Vector_2_8) {
    size_t size = 1 << 8;
    std::vector<std::vector<uint64_t>> table_portion(4);
    auto &col0 = table_portion[0];
    auto &col1 = table_portion[1];
    auto &col2 = table_portion[2];
    auto &col3 = table_portion[3];
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

    auto db = imlab::DataBlock<4>::build(table_portion, str_table_portion, schematypes);

    EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<4> cm(0, buffer_manager);
    std::array<uint8_t, 0> str_index_arr{{}};

    /// after std::move: datablock in Memory is destroied
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    for (size_t i = 0; i < size; ++i) {
        uint64_t random0 = random.Rand();
        uint64_t random1 = random.Rand();
        uint64_t random2 = random.Rand();
        uint64_t random3 = random.Rand();
        Predicate predicate_0(0, random0, CompareType::LESS_THAN);
        Predicate predicate_1(1, random1, CompareType::EQUAL);
        Predicate predicate_2(2, random2, CompareType::GREATER_THAN);
        Predicate predicate_3(3, random3, CompareType::GREATER_THAN);
        std::vector<Predicate> predicates;
        predicates.push_back(predicate_0);
        predicates.push_back(predicate_1);
        predicates.push_back(predicate_2);
        predicates.push_back(predicate_3);

        uint64_t count_if = 0;
        for (size_t i = 0; i < size; ++i) {
            if (col0[i] < random0 && col1[i] == random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
        }
        EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
}

TEST(Continuous_Memory_Segment_Scan, NONEQUAL_Random_Multi_Vector_2_8) {
    size_t size = 1 << 10;
    std::vector<std::vector<uint64_t>> table_portion(4);
    auto &col0 = table_portion[0];
    auto &col1 = table_portion[1];
    auto &col2 = table_portion[2];
    auto &col3 = table_portion[3];
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

    auto db = imlab::DataBlock<4>::build(table_portion, str_table_portion, schematypes);

    EXPECT_EQ(col0.size(), db.get()->get_tupel_count());
    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, 150 * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(150, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<4> cm(0, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 0> str_index_arr{{}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    for (size_t i = 0; i < size; ++i) {
        uint64_t random0 = random.Rand();
        uint64_t random1 = random.Rand();
        uint64_t random2 = random.Rand();
        uint64_t random3 = random.Rand();
        Predicate predicate_0(0, random0, CompareType::LESS_THAN);
        Predicate predicate_1(1, random1, CompareType::LESS_THAN);
        Predicate predicate_2(2, random2, CompareType::GREATER_THAN);
        Predicate predicate_3(3, random3, CompareType::GREATER_THAN);
        std::vector<Predicate> predicates;
        predicates.push_back(predicate_0);
        predicates.push_back(predicate_1);
        predicates.push_back(predicate_2);
        predicates.push_back(predicate_3);
        uint64_t count_if = 0;


        for (size_t i = 0; i < size; ++i) {
            if (col0[i] < random0 && col1[i] < random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
        }
        EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
}

TEST(Continuous_Memory_Segment_Scan, SingleValue_Random_Single_Vector_2_16) {
    size_t size = 1 << 16;
    std::vector<std::vector<uint64_t>> table_portion(1);
    auto &col0 = table_portion[0];
    col0.reserve(size);
    Random random_;
    uint64_t random = random_.Rand();
    for (size_t i = 0; i < size; ++i) {
        col0.push_back(random);
    }

    std::vector<std::vector<std::string>> str_table_portion;

    std::vector<SchemaType> schematypes{SchemaType::Interger};

    auto db = imlab::DataBlock<1>::build(table_portion, str_table_portion, schematypes);

    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, 150 * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(150, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<1> cm(0, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 0> str_index_arr{{}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    Predicate predicate(0, random, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    EXPECT_EQ(size, cm.cm_scan(predicates, *index_vectors));

    Predicate predicate_2(0, random - 1, CompareType::GREATER_THAN);
    predicates[0] = predicate_2;
    EXPECT_EQ(size, cm.cm_scan(predicates, *index_vectors));

    Predicate predicate_3(0, random + 1, CompareType::LESS_THAN);
    predicates[0] = predicate_3;
    EXPECT_EQ(size, cm.cm_scan(predicates, *index_vectors));
}

/// needs 129 pages
TEST(Continuous_Memory_Segment_Scan, Random_Multi_Vector_2_16) {
    size_t size = 1 << 16;
    std::vector<std::vector<uint64_t>> table_portion(4);
    auto &col0 = table_portion[0];
    auto &col1 = table_portion[1];
    auto &col2 = table_portion[2];
    auto &col3 = table_portion[3];
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

    auto db = imlab::DataBlock<4>::build(table_portion, str_table_portion, schematypes);

    EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, 129 * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<4> cm(0, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 0> str_index_arr{{}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    for (size_t i = 0; i < size; ++i) {
        uint64_t random0 = random.Rand();
        uint64_t random1 = random.Rand();
        uint64_t random2 = random.Rand();
        uint64_t random3 = random.Rand();
        Predicate predicate_0(0, random0, CompareType::LESS_THAN);
        Predicate predicate_1(1, random1, CompareType::EQUAL);
        Predicate predicate_2(2, random2, CompareType::GREATER_THAN);
        Predicate predicate_3(3, random3, CompareType::GREATER_THAN);
        std::vector<Predicate> predicates;
        predicates.push_back(predicate_0);
        predicates.push_back(predicate_1);
        predicates.push_back(predicate_2);
        predicates.push_back(predicate_3);
        uint64_t count_if = 0;
        for (size_t i = 0; i < size; ++i) {
            if (col0[i] < random0 && col1[i] == random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
        }
        EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
}

TEST(Continuous_Memory_Segment_Scan, NONEQUAL_Random_Multi_Vector_2_16) {
    size_t size = 1 << 16;
    std::vector<std::vector<uint64_t>> table_portion(4);
    auto &col0 = table_portion[0];
    auto &col1 = table_portion[1];
    auto &col2 = table_portion[2];
    auto &col3 = table_portion[3];
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

    auto db = imlab::DataBlock<4>::build(table_portion, str_table_portion, schematypes);

    EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

    /// Continuous_Memory_Segment

    /// New this buffermanager
    imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, 129 * imlab::PAGE_SIZE)), &imlab::aligned_free_wrapper);
    BufferManager buffer_manager(MAX_PAGES_NUM, std::move(loaded_pages));

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<4> cm(0, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 0> str_index_arr{{}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    for (size_t i = 0; i < size; ++i) {
        uint64_t random0 = random.Rand();
        uint64_t random1 = random.Rand();
        uint64_t random2 = random.Rand();
        uint64_t random3 = random.Rand();
        Predicate predicate_0(0, random0, CompareType::LESS_THAN);
        Predicate predicate_1(1, random1, CompareType::LESS_THAN);
        Predicate predicate_2(2, random2, CompareType::GREATER_THAN);
        Predicate predicate_3(3, random3, CompareType::GREATER_THAN);
        std::vector<Predicate> predicates;
        predicates.push_back(predicate_0);
        predicates.push_back(predicate_1);
        predicates.push_back(predicate_2);
        predicates.push_back(predicate_3);
        uint64_t count_if = 0;
        for (size_t i = 0; i < size; ++i) {
            if (col0[i] < random0 && col1[i] < random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
        }
        EXPECT_EQ(count_if, cm.cm_scan(predicates, *index_vectors));
    }
}
#endif
// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
