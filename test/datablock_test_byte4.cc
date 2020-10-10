// ---------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------
#include <math.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <map>
#include <tuple>
#include <vector>
#include "datablock_test_helper.cc" // NOLINT
#include "imlab/datablock/DataBlock.h"
#include "imlab/util/Random.h"

#define DEBUG
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::util::SchemaType;

#ifdef DEBUG
TEST(DataBlock, 32bit_EQUAL_Random_Single_Vector_2_8) {
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

  scan_helper_singleCol_uni(size,
      int_table_portion,
      std::move(db),
      CompareType::EQUAL,
      [](uint64_t compared, uint64_t entry) {return compared == entry;} );
}

TEST(DataBlock, 32bit_EQUAL_Random_Single_Vector_2_16) {
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
  scan_helper_singleCol_uni(size,
      int_table_portion,
      std::move(db),
      CompareType::EQUAL,
      [](uint64_t compared, uint64_t entry) {return compared == entry;} );
}

TEST(DataBlock, 32bit_GREATER_THAN_Random_Single_Vector_2_8) {
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
  scan_helper_singleCol_uni(size,
      int_table_portion,
      std::move(db),
      CompareType::GREATER_THAN,
      [](uint64_t compared, uint64_t entry) {return compared < entry;} );
}

TEST(DataBlock, 32bit_GREATER_THAN_Random_Single_Vector_2_16) {
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
  scan_helper_singleCol_uni(size,
      int_table_portion,
      std::move(db),
      CompareType::GREATER_THAN,
      [](uint64_t compared, uint64_t entry) {return compared < entry;} );
}

TEST(DataBlock, 32bit_LESS_THAN_Random_Single_Vector_2_8) {
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
  scan_helper_singleCol_uni(size,
      int_table_portion,
      std::move(db),
      CompareType::LESS_THAN,
      [](uint64_t compared, uint64_t entry) {return compared > entry;} );
}

TEST(DataBlock, 32bit_LESS_THAN_Random_Single_Vector_2_16) {
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
  scan_helper_singleCol_uni(size,
      int_table_portion,
      std::move(db),
      CompareType::LESS_THAN,
      [](uint64_t compared, uint64_t entry) {return compared > entry;} );
}

TEST(DataBlock, 32bit_BETWEEN_Random_Single_Vector_2_8) {
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
  scan_helper_singleCol_bin(size, int_table_portion, std::move(db));
}

TEST(DataBlock, 32bit_BETWEEN_Random_Single_Vector_2_16) {
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
  scan_helper_singleCol_bin(size, int_table_portion, std::move(db));
}

TEST(DataBlock, 32bit_MULTI_EQUAL_THAN_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  size_t dup_size = 1 << 4;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size - dup_size; ++i) {
    col0.push_back(random.Rand() >> 32);
  }
  for (size_t i = 0; i < dup_size; ++i) {
    col0.push_back(col0[random.Rand() % (size - dup_size - 1)]);
  }

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  for (size_t i = 0; i < size; ++i) {
    uint64_t random1 = random.Rand() >> 32;
    uint64_t random2 = random.Rand() >> 32;
    uint64_t random3 = random.Rand() >> 32;
    Predicate predicate_1(0, random1, CompareType::EQUAL);
    Predicate predicate_2(0, random2, CompareType::EQUAL);
    Predicate predicate_3(0, random3, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);

    uint64_t count_if = std::count_if(col0.begin(), col0.end(), [random1, random2, random3](uint64_t entry) { return entry == random1 && entry == random2 && entry == random3;});

    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, 32bit_MULTI_LESS_THAN__Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  size_t dup_size = 1 << 4;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size - dup_size; ++i) {
    col0.push_back(random.Rand() >> 32);
  }
  for (size_t i = 0; i < dup_size; ++i) {
    col0.push_back(col0[random.Rand() % (size - dup_size - 1)]);
  }

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  for (size_t i = 0; i < size; ++i) {
    uint64_t random1 = random.Rand() >> 32;
    uint64_t random2 = random.Rand() >> 32;
    uint64_t random3 = random.Rand() >> 32;
    Predicate predicate_1(0, random1, CompareType::LESS_THAN);
    Predicate predicate_2(0, random2, CompareType::LESS_THAN);
    Predicate predicate_3(0, random3, CompareType::LESS_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);

    uint64_t count_if = std::count_if(col0.begin(), col0.end(), [random1, random2, random3](uint64_t entry) { return entry < random1 && entry < random2 && entry < random3;});

    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, 32bit_MULTI_GREATER_THAN_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  size_t dup_size = 1 << 4;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size - dup_size; ++i) {
    col0.push_back(random.Rand() >> 32);
  }
  for (size_t i = 0; i < dup_size; ++i) {
    col0.push_back(col0[random.Rand() % (size - dup_size - 1)]);
  }

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  for (size_t i = 0; i < size; ++i) {
    uint64_t random1 = random.Rand() >> 32;
    uint64_t random2 = random.Rand() >> 32;
    uint64_t random3 = random.Rand() >> 32;
    Predicate predicate_1(0, random1, CompareType::GREATER_THAN);
    Predicate predicate_2(0, random2, CompareType::GREATER_THAN);
    Predicate predicate_3(0, random3, CompareType::GREATER_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);

    uint64_t count_if = std::count_if(col0.begin(), col0.end(), [random1, random2, random3](uint64_t entry) { return entry > random1 && entry > random2 && entry > random3;});

    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, 32bit_MULTI_BETWEEN_Random_Single_Vector_2_16) {
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

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion,
                                       schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  for (size_t i = 0; i < size; ++i) {
    uint64_t random1 = random.Rand() >> 32;
    uint64_t random2 = random.Rand() >> 32;
    uint64_t random3 = random.Rand() >> 32;
    uint64_t random4 = random.Rand() >> 32;
    uint64_t random5 = random.Rand() >> 32;
    uint64_t random6 = random.Rand() >> 32;

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
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}
#endif
// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
