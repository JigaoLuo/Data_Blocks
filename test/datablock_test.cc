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

#define DEBUG
// ---------------------------------------------------------------------------------------------------
using imlab::TUPLES_PER_DATABLOCK;
static std::array<uint32_t, TUPLES_PER_DATABLOCK> INDEX_VECTORS[1];
std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
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
TEST(DataBlock, Simple_FixNumberTest) {
  /// Same as TEST(DataBlock, FixNumberTest)
  std::vector<uint64_t> col0 {3, 2, 1, 4, 5, 6, 7, 8, 9, 15};
  std::vector<std::vector<uint64_t>> int_table_portion {col0};

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  Predicate predicate(0, 257, CompareType::EQUAL);
  std::vector<Predicate> predicates;
  predicates.push_back(predicate);
  EXPECT_EQ(0, db.get()->scan(predicates, *index_vectors));
}

TEST(DataBlock, FixNumberTest) {
  std::vector<uint64_t> col0 {3, 2, 1, 4, 5, 6, 7, 8, 9, 70000};
  std::vector<uint64_t> col1  {16, 32, 30, 40, 50, 60, 70, 80, 90, 257};
  std::vector<uint64_t> col2 {257, 257, 257, 257, 257, 257, 257, 257, 257, 257}; /// Singlevalue Encoding
  std::vector<uint64_t> col3 {20, 40, 16, 32, 48, 256, 128, 64, 16, 257};
  std::vector<std::vector<uint64_t>> int_table_portion {col0, col1, col2, col3};

  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger,
                                      SchemaType::Interger};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col1.size(), db.get()->get_tupel_count());
  // db.get()->getData(0);
  // db.get()->getData(1);
  // db.get()->getData(2);
  // db.get()->getData(3);
  Predicate predicate(2, 257, CompareType::EQUAL);
  std::vector<Predicate> predicates;
  predicates.push_back(predicate);
  EXPECT_EQ(10, db.get()->scan(predicates, *index_vectors));
}

TEST(DataBlock, str_MutiPre_Random_Single_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<std::string>> str_table_portion(1);
  auto &col0 = str_table_portion[0];
  col0.reserve(size);
  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 1> str_index_arr{{0}};
  std::vector<std::set<std::string>> str_set_vec = db->decode_dictionary<1>(str_index_arr);

  for (size_t i = 0; i < size; ++i) {
    std::string random1 = col0[random.Rand() >> (64 - 8)];
    std::string random2 = col0[random.Rand() >> (64 - 8)];
    std::string random3 = col0[random.Rand() >> (64 - 8)];

    /// Binary Search
    auto found_1 = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random1);
    uint64_t compared_1 = std::distance(str_set_vec[0].begin(), found_1);

    auto found_2 = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random2);
    uint64_t compared_2 = std::distance(str_set_vec[0].begin(), found_2);

    auto found_3 = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random3);
    uint64_t compared_3 = std::distance(str_set_vec[0].begin(), found_3);

    Predicate predicate_1(0, compared_1, CompareType::LESS_THAN);
    Predicate predicate_2(0, compared_2, CompareType::EQUAL);
    Predicate predicate_3(0, compared_3, CompareType::GREATER_THAN);

    std::vector<Predicate> predicates;
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);
    uint64_t count_if = std::count_if(col0.begin(), col0.end(), [random1, random2, random3](std::string entry) { return random1 > entry && entry == random2 && entry > random3;});
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}


TEST(DataBlock, Random_Multi_Vector_2_8) {
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
  std::map<int, int> dup_0;
  std::for_each(col0.begin(), col0.end(), [&dup_0](int val){ dup_0[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(0, col0[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_0.find(col0[i]);
      if (it == dup_0.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<int, int> dup_1;
  std::for_each(col1.begin(), col1.end(), [&dup_1](int val){ dup_1[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(1, col1[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_1.find(col1[i]);
      if (it == dup_1.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<int, int> dup_2;
  std::for_each(col2.begin(), col2.end(), [&dup_2](int val){ dup_2[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(2, col2[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_2.find(col2[i]);
      if (it == dup_2.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<int, int> dup_3;
  std::for_each(col3.begin(), col3.end(), [&dup_3](int val){ dup_3[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(3, col3[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_3.find(col3[i]);
      if (it == dup_3.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

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
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, str_Random_Multi_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<std::string>> str_table_portion(4);
  auto &col0 = str_table_portion[0];
  auto &col1 = str_table_portion[1];
  auto &col2 = str_table_portion[2];
  auto &col3 = str_table_portion[3];
  col0.reserve(size);

  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col1.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col1.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col2.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col2.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col3.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col3.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 4> str_index_arr{{0, 1, 2, 3}};
  std::vector<std::set<std::string>> str_set_vec = db->decode_dictionary<4>(str_index_arr);

  std::map<std::string, int> dup_0;
  std::for_each(col0.begin(), col0.end(), [&dup_0](std::string val){ dup_0[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col0[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random);
    uint64_t compared = std::distance(str_set_vec[0].begin(), found);

    Predicate predicate(0, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_0.find(col0[i]);
    if (it == dup_0.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_1;
  std::for_each(col1.begin(), col1.end(), [&dup_1](std::string val){ dup_1[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col1[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random);
    uint64_t compared = std::distance(str_set_vec[1].begin(), found);

    Predicate predicate(1, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_1.find(col1[i]);
    if (it == dup_1.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_2;
  std::for_each(col2.begin(), col2.end(), [&dup_2](std::string val){ dup_2[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col2[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random);
    uint64_t compared = std::distance(str_set_vec[2].begin(), found);

    Predicate predicate(2, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_2.find(col2[i]);
    if (it == dup_2.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_3;
  std::for_each(col3.begin(), col3.end(), [&dup_3](std::string val){ dup_3[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col3[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random);
    uint64_t compared = std::distance(str_set_vec[3].begin(), found);

    Predicate predicate(3, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_3.find(col3[i]);
    if (it == dup_3.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
    std::string random0 = col0[random.Rand() >> (64 - 8)];
    std::string random1 = col1[random.Rand() >> (64 - 8)];
    std::string random2 = col2[random.Rand() >> (64 - 8)];
    std::string random3 = col3[random.Rand() >> (64 - 8)];

    /// Binary Search
    auto found_0 = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random0);
    uint64_t compared_0 = std::distance(str_set_vec[0].begin(), found_0);

    auto found_1 = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random1);
    uint64_t compared_1 = std::distance(str_set_vec[1].begin(), found_1);

    auto found_2 = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random2);
    uint64_t compared_2 = std::distance(str_set_vec[2].begin(), found_2);

    auto found_3 = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random3);
    uint64_t compared_3 = std::distance(str_set_vec[3].begin(), found_3);

    Predicate predicate_0(0, compared_0, CompareType::LESS_THAN);
    Predicate predicate_1(1, compared_1, CompareType::EQUAL);
    Predicate predicate_2(2, compared_2, CompareType::GREATER_THAN);
    Predicate predicate_3(3, compared_3, CompareType::GREATER_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_0);
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);
    uint64_t count_if = 0;
    for (size_t i = 0; i < size; ++i) {
      if (col0[i] < random0 && col1[i] == random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
    }
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, NONEQUAL_Random_Multi_Vector_2_8) {
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
  std::map<int, int> dup_0;
  std::for_each(col0.begin(), col0.end(), [&dup_0](int val){ dup_0[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(0, col0[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_0.find(col0[i]);
      if (it == dup_0.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<int, int> dup_1;
  std::for_each(col1.begin(), col1.end(), [&dup_1](int val){ dup_1[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(1, col1[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_1.find(col1[i]);
      if (it == dup_1.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<int, int> dup_2;
  std::for_each(col2.begin(), col2.end(), [&dup_2](int val){ dup_2[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(2, col2[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_2.find(col2[i]);
      if (it == dup_2.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<int, int> dup_3;
  std::for_each(col3.begin(), col3.end(), [&dup_3](int val){ dup_3[val]++; });
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(3, col3[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto it = dup_3.find(col3[i]);
      if (it == dup_3.end()) EXPECT_EQ(1, 0);
      EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

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
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, str_NONEQUAL_Random_Multi_Vector_2_8) {
  size_t size = 1 << 8;
  std::vector<std::vector<std::string>> str_table_portion(4);
  auto &col0 = str_table_portion[0];
  auto &col1 = str_table_portion[1];
  auto &col2 = str_table_portion[2];
  auto &col3 = str_table_portion[3];
  col0.reserve(size);

  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col1.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col1.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col2.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col2.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col3.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col3.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 4> str_index_arr{{0, 1, 2, 3}};
  std::vector<std::set<std::string>> str_set_vec = db->decode_dictionary<4>(str_index_arr);

  std::map<std::string, int> dup_0;
  std::for_each(col0.begin(), col0.end(), [&dup_0](std::string val){ dup_0[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col0[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random);
    uint64_t compared = std::distance(str_set_vec[0].begin(), found);

    Predicate predicate(0, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_0.find(col0[i]);
    if (it == dup_0.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_1;
  std::for_each(col1.begin(), col1.end(), [&dup_1](std::string val){ dup_1[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col1[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random);
    uint64_t compared = std::distance(str_set_vec[1].begin(), found);

    Predicate predicate(1, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_1.find(col1[i]);
    if (it == dup_1.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_2;
  std::for_each(col2.begin(), col2.end(), [&dup_2](std::string val){ dup_2[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col2[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random);
    uint64_t compared = std::distance(str_set_vec[2].begin(), found);

    Predicate predicate(2, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_2.find(col2[i]);
    if (it == dup_2.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_3;
  std::for_each(col3.begin(), col3.end(), [&dup_3](std::string val){ dup_3[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col3[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random);
    uint64_t compared = std::distance(str_set_vec[3].begin(), found);

    Predicate predicate(3, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_3.find(col3[i]);
    if (it == dup_3.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
    std::string random0 = col0[random.Rand() >> (64 - 8)];
    std::string random1 = col1[random.Rand() >> (64 - 8)];
    std::string random2 = col2[random.Rand() >> (64 - 8)];
    std::string random3 = col3[random.Rand() >> (64 - 8)];

    /// Binary Search
    auto found_0 = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random0);
    uint64_t compared_0 = std::distance(str_set_vec[0].begin(), found_0);

    auto found_1 = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random1);
    uint64_t compared_1 = std::distance(str_set_vec[1].begin(), found_1);

    auto found_2 = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random2);
    uint64_t compared_2 = std::distance(str_set_vec[2].begin(), found_2);

    auto found_3 = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random3);
    uint64_t compared_3 = std::distance(str_set_vec[3].begin(), found_3);

    Predicate predicate_0(0, compared_0, CompareType::LESS_THAN);
    Predicate predicate_1(1, compared_1, CompareType::LESS_THAN);
    Predicate predicate_2(2, compared_2, CompareType::GREATER_THAN);
    Predicate predicate_3(3, compared_3, CompareType::GREATER_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_0);
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);
    uint64_t count_if = 0;
    for (size_t i = 0; i < size; ++i) {
      if (col0[i] < random0 && col1[i] < random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
    }
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, SingleValue_Random_Single_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<uint64_t>> int_table_portion(1);
  auto &col0 = int_table_portion[0];
  col0.reserve(size);
  Random random_;
  uint64_t random = random_.Rand();
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(random);
  }
  std::vector<std::vector<std::string>> str_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Interger};

  auto db = imlab::DataBlock<1>::build(int_table_portion, str_table_portion, schematypes);

  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());
  Predicate predicate(0, random, CompareType::EQUAL);
  std::vector<Predicate> predicates;
  predicates.push_back(predicate);
  EXPECT_EQ(size, db.get()->scan(predicates, *index_vectors));

  Predicate predicate_2(0, random - 1, CompareType::GREATER_THAN);
  predicates[0] = predicate_2;
  EXPECT_EQ(size, db.get()->scan(predicates, *index_vectors));

  Predicate predicate_3(0, random + 1, CompareType::LESS_THAN);
  predicates[0] = predicate_3;
  EXPECT_EQ(size, db.get()->scan(predicates, *index_vectors));

  Predicate predicate_4(0, random + 1, CompareType::GREATER_THAN);
  predicates[0] = predicate_4;
  EXPECT_EQ(0, db.get()->scan(predicates, *index_vectors));

  Predicate predicate_5(0, random - 1, CompareType::LESS_THAN);
  predicates[0] = predicate_5;
  EXPECT_EQ(0, db.get()->scan(predicates, *index_vectors));
}


TEST(DataBlock, str_Random_Multi_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<std::string>> str_table_portion(4);
  auto &col0 = str_table_portion[0];
  auto &col1 = str_table_portion[1];
  auto &col2 = str_table_portion[2];
  auto &col3 = str_table_portion[3];
  col0.reserve(size);

  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col1.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col1.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col2.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col2.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col3.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col3.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 4> str_index_arr{{0, 1, 2, 3}};
  std::vector<std::set<std::string>> str_set_vec = db->decode_dictionary<4>(str_index_arr);

  std::map<std::string, int> dup_0;
  std::for_each(col0.begin(), col0.end(), [&dup_0](std::string val){ dup_0[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col0[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random);
    uint64_t compared = std::distance(str_set_vec[0].begin(), found);

    Predicate predicate(0, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_0.find(col0[i]);
    if (it == dup_0.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_1;
  std::for_each(col1.begin(), col1.end(), [&dup_1](std::string val){ dup_1[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col1[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random);
    uint64_t compared = std::distance(str_set_vec[1].begin(), found);

    Predicate predicate(1, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_1.find(col1[i]);
    if (it == dup_1.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_2;
  std::for_each(col2.begin(), col2.end(), [&dup_2](std::string val){ dup_2[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col2[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random);
    uint64_t compared = std::distance(str_set_vec[2].begin(), found);

    Predicate predicate(2, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_2.find(col2[i]);
    if (it == dup_2.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_3;
  std::for_each(col3.begin(), col3.end(), [&dup_3](std::string val){ dup_3[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col3[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random);
    uint64_t compared = std::distance(str_set_vec[3].begin(), found);

    Predicate predicate(3, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_3.find(col3[i]);
    if (it == dup_3.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
    std::string random0 = col0[random.Rand() >> (64 - 8)];
    std::string random1 = col1[random.Rand() >> (64 - 8)];
    std::string random2 = col2[random.Rand() >> (64 - 8)];
    std::string random3 = col3[random.Rand() >> (64 - 8)];

    /// Binary Search
    auto found_0 = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random0);
    uint64_t compared_0 = std::distance(str_set_vec[0].begin(), found_0);

    auto found_1 = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random1);
    uint64_t compared_1 = std::distance(str_set_vec[1].begin(), found_1);

    auto found_2 = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random2);
    uint64_t compared_2 = std::distance(str_set_vec[2].begin(), found_2);

    auto found_3 = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random3);
    uint64_t compared_3 = std::distance(str_set_vec[3].begin(), found_3);

    Predicate predicate_0(0, compared_0, CompareType::LESS_THAN);
    Predicate predicate_1(1, compared_1, CompareType::EQUAL);
    Predicate predicate_2(2, compared_2, CompareType::GREATER_THAN);
    Predicate predicate_3(3, compared_3, CompareType::GREATER_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_0);
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);
    uint64_t count_if = 0;
    for (size_t i = 0; i < size; ++i) {
      if (col0[i] < random0 && col1[i] == random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
    }
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, Random_Multi_Vector_2_16) {
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
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(0, col0[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col0[i];
      uint64_t count_if = std::count_if(col0.begin(), col0.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(1, col1[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col1[1];
      uint64_t count_if = std::count_if(col1.begin(), col1.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(2, col2[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col2[i];
      uint64_t count_if = std::count_if(col2.begin(), col2.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(3, col3[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col3[i];
      uint64_t count_if = std::count_if(col3.begin(), col3.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

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
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, str_NONEQUAL_Random_Multi_Vector_2_16) {
  size_t size = 1 << 16;
  std::vector<std::vector<std::string>> str_table_portion(4);
  auto &col0 = str_table_portion[0];
  auto &col1 = str_table_portion[1];
  auto &col2 = str_table_portion[2];
  auto &col3 = str_table_portion[3];
  col0.reserve(size);

  Random random;
  for (size_t i = 0; i < size; ++i) {
    col0.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col1.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col1.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col2.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col2.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }
  col3.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    col3.push_back(l_shipinstruct[random.Rand() >> (64 - 2)]);
  }

  std::vector<std::vector<std::uint64_t>> int_table_portion;

  std::vector<SchemaType> schematypes{SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char,
                                      SchemaType::Char};

  auto db = imlab::DataBlock<4>::build(int_table_portion, str_table_portion, schematypes);
  EXPECT_EQ(col0.size(), db.get()->get_tupel_count());

  std::array<uint8_t, 4> str_index_arr{{0, 1, 2, 3}};
  std::vector<std::set<std::string>> str_set_vec = db->decode_dictionary<4>(str_index_arr);

  std::map<std::string, int> dup_0;
  std::for_each(col0.begin(), col0.end(), [&dup_0](std::string val){ dup_0[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col0[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random);
    uint64_t compared = std::distance(str_set_vec[0].begin(), found);

    Predicate predicate(0, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_0.find(col0[i]);
    if (it == dup_0.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_1;
  std::for_each(col1.begin(), col1.end(), [&dup_1](std::string val){ dup_1[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col1[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random);
    uint64_t compared = std::distance(str_set_vec[1].begin(), found);

    Predicate predicate(1, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_1.find(col1[i]);
    if (it == dup_1.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_2;
  std::for_each(col2.begin(), col2.end(), [&dup_2](std::string val){ dup_2[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col2[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random);
    uint64_t compared = std::distance(str_set_vec[2].begin(), found);

    Predicate predicate(2, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_2.find(col2[i]);
    if (it == dup_2.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  std::map<std::string, int> dup_3;
  std::for_each(col3.begin(), col3.end(), [&dup_3](std::string val){ dup_3[val]++; });
  for (size_t i = 0; i < size; ++i) {
    std::string random = col3[i];

    /// Binary Search
    auto found = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random);
    uint64_t compared = std::distance(str_set_vec[3].begin(), found);

    Predicate predicate(3, compared, CompareType::EQUAL);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate);
    auto it = dup_3.find(col3[i]);
    if (it == dup_3.end()) EXPECT_EQ(1, 0);
    EXPECT_EQ(it->second, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
    std::string random0 = col0[random.Rand() >> (64 - 8)];
    std::string random1 = col1[random.Rand() >> (64 - 8)];
    std::string random2 = col2[random.Rand() >> (64 - 8)];
    std::string random3 = col3[random.Rand() >> (64 - 8)];

    /// Binary Search
    auto found_0 = std::lower_bound(str_set_vec[0].begin(), str_set_vec[0].end(), random0);
    uint64_t compared_0 = std::distance(str_set_vec[0].begin(), found_0);

    auto found_1 = std::lower_bound(str_set_vec[1].begin(), str_set_vec[1].end(), random1);
    uint64_t compared_1 = std::distance(str_set_vec[1].begin(), found_1);

    auto found_2 = std::lower_bound(str_set_vec[2].begin(), str_set_vec[2].end(), random2);
    uint64_t compared_2 = std::distance(str_set_vec[2].begin(), found_2);

    auto found_3 = std::lower_bound(str_set_vec[3].begin(), str_set_vec[3].end(), random3);
    uint64_t compared_3 = std::distance(str_set_vec[3].begin(), found_3);

    Predicate predicate_0(0, compared_0, CompareType::LESS_THAN);
    Predicate predicate_1(1, compared_1, CompareType::LESS_THAN);
    Predicate predicate_2(2, compared_2, CompareType::GREATER_THAN);
    Predicate predicate_3(3, compared_3, CompareType::GREATER_THAN);
    std::vector<Predicate> predicates;
    predicates.push_back(predicate_0);
    predicates.push_back(predicate_1);
    predicates.push_back(predicate_2);
    predicates.push_back(predicate_3);
    uint64_t count_if = 0;
    for (size_t i = 0; i < size; ++i) {
      if (col0[i] < random0 && col1[i] < random1 && col2[i] > random2 && col3[i] > random3) ++count_if;
    }
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}

TEST(DataBlock, NONEQUAL_Random_Multi_Vector_2_16) {
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
  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(0, col0[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col0[i];
      uint64_t count_if = std::count_if(col0.begin(), col0.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(1, col1[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col1[i];
      uint64_t count_if = std::count_if(col1.begin(), col1.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(2, col2[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col2[i];
      uint64_t count_if = std::count_if(col2.begin(), col2.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

  for (size_t i = 0; i < size; ++i) {
      Predicate predicate(3, col3[i], CompareType::EQUAL);
      std::vector<Predicate> predicates;
      predicates.push_back(predicate);
      auto compared = col3[i];
      uint64_t count_if = std::count_if(col3.begin(), col3.end(), [compared](uint64_t entry) { return compared == entry;});
      EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }

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
    EXPECT_EQ(count_if, db.get()->scan(predicates, *index_vectors));
  }
}
#endif
// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
