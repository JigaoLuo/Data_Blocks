// ---------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------
#include <algorithm>
#include <chrono>
#include <stdlib.h>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include "imlab/datablock/DataBlock.h"
#include "imlab/datablock/Table.h"
#include "imlab/util/DataBlock_Types.h"
#include "imlab/util/Predicate.h"
#include "imlab/util/Random.h"
#include "imlab/util/types.h"
#include "imlab/buffer_manage/buffer_manager.h"
// ---------------------------------------------------------------------------------------------------
//#define DEBUG_TPCH
#define SINGLE_THREAD
//#define MULTI_THREAD
// ---------------------------------------------------------------------------------------------------
using std::vector;
// ---------------------------------------------------------------------------------------------------
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::TUPLES_PER_DATABLOCK;
using imlab::BufferFrame;
using imlab::BufferManager;
using imlab::Continuous_Memory_Segment;
using imlab::util::SchemaType;
// ---------------------------------------------------------------------------
/// Nation Path adjuested
const char * const TPCH_NATION_PATH = "/media/luojig/LuoDisk/nation.tbl";

constexpr size_t NATION_LINES = 25;

constexpr size_t NATION_INT_COLUMNS = 2;

constexpr size_t NATION_CHAR_COLUMNS = 2;

/// Lineitem Path adjuested
/// SF 10
//const char * const TPCH_LINEITEM_PATH = "../../lineitem_sf10.tbl";
//const char * const TPCH_LINEITEM_SORTED_PATH = "../../lineitem_sorted_sf10.tbl";

//const char * const TPCH_LINEITEM_PATH = "/media/luojig/LuoDisk/lineitem_sf10.tbl";
const char * const TPCH_LINEITEM_SORTED_PATH = "/media/luojig/LuoDisk/lineitem_sorted_sf10.tbl";
//const char * const TPCH_LINEITEM_SORTED_PATH = "../../lineitem.tbl";

/// Lines in the TPCH_LINEITEM_SORTED_PATH
constexpr size_t LINEITEM_LINES = 59'986'051;  // 59'986'051 > 2 ^ 25 = 33'554'432

/// SF 1
const char * const TPCH_LINEITEM_PATH = "../../lineitem.tbl";
//const char * const TPCH_LINEITEM_SORTED_PATH = "../../lineitem_sorted.tbl";

/// Lines in the TPCH_LINEITEM_SORTED_PATH
//constexpr size_t LINEITEM_LINES = 6'001'215;  // 6'001'215 > 2 ^ 22 = 4 194 304

// constexpr uint64_t SIZE_READ = 1 << 22;
// constexpr size_t NUM_DB = SIZE_READ / imlab::TUPLES_PER_DATABLOCK;
/// Column Number of Lineitems (without String Columns)
constexpr size_t LINEITEM_INT_COLUMNS = 13;

constexpr size_t LINEITEM_CHAR_COLUMNS = 2;

/// Number of Datablock needed for this TPCH_LINEITEM_SORTED_PATH
constexpr size_t NUM_DB = LINEITEM_LINES / TUPLES_PER_DATABLOCK + 1; /// >>> 6001215.0 / 65536 = 91.57 => round to 92

constexpr uint64_t SIZE_READ = NUM_DB * TUPLES_PER_DATABLOCK;

/// Benchmark: scan NUMBER_OF_SCANS times, and check this NUMBER_OF_SCANS scan time
constexpr uint64_t NUMBER_OF_SCANS = 1000;

/// Benchmark: Each Iteration run NUMBER_OF_SCANS times scan
constexpr uint64_t SCAN_ITERATIONS = 10;

/// For such a file TPCH_LINEITEM_SORTED_PATH: maximal 97 page needed
constexpr uint64_t MAX_PAGES_NUM = 1692;
//constexpr uint64_t MAX_PAGES_NUM = 846;
//constexpr uint64_t MAX_PAGES_NUM = 432;
//constexpr uint64_t MAX_PAGES_NUM = 216;

//constexpr uint64_t MAX_PAGES_NUM = 108;
//constexpr uint64_t MAX_PAGES_NUM = 54;
//constexpr uint64_t MAX_PAGES_NUM = 27;
//constexpr uint64_t MAX_PAGES_NUM = 14;
//constexpr uint64_t MAX_PAGES_NUM = 7;
//constexpr uint64_t MAX_PAGES_NUM = 4;
//constexpr uint64_t MAX_PAGES_NUM = 2;
//constexpr uint64_t MAX_PAGES_NUM = 1;

static int days_in_years[7] = {0,
                               366,
                               365 + 366,
                               365 + 365 + 366,
                               365 + 365 + 365 + 366,
                               366 + 365 + 365 + 365 + 366,
                               365 + 366 + 365 + 365 + 365 + 366};

static int days_in_year[12] = {0,
                               31,
                               28 + 31,
                               31 + 28 + 31,
                               30 + 31 + 28 + 31,
                               31 + 30 + 31 + 28 + 31,
                               30 + 31 + 30 + 31 + 28 + 31,
                               31 + 30 + 31 + 30 + 31 + 28 + 31,
                               31 + 31 + 30 + 31 + 30 + 31 + 28 + 31,
                               30 + 31 + 31 + 30 + 31 + 30 + 31 + 28 + 31,
                               31 + 30 + 31 + 31 + 30 + 31 + 30 + 31 + 28 + 31,
                               30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31 + 28 + 31};

#ifdef SINGLE_THREAD
/// Recording the Index Vector: as Compile time Stack allocated memory
static std::array<uint32_t, TUPLES_PER_DATABLOCK> INDEX_VECTORS[NUM_DB];

static std::array<uint32_t, TUPLES_PER_DATABLOCK> INDEX_VECTORS_[NUM_DB];

/// Recoding the number of matches of each Index Vector: as Compile time Stack allocated memory
static std::array<uint32_t, NUM_DB> MATCHES_INDEX_VECTOR;

static std::array<uint32_t, NUM_DB> MATCHES_INDEX_VECTOR_;


/// Materialization L_DISCOUNT EncodingType::Byte1: as Compile time Stack allocated memory
static std::array<uint8_t, LINEITEM_LINES> L_DISCOUNT;         // [6]

/// Materialization L_EXTENDEDPRICE EncodingType::Byte4: as Compile time Stack allocated memory
static std::array<uint8_t , (LINEITEM_LINES << 2)> L_EXTENDEDPRICE;    // [5]

/// Materialization LINEITEM_LINES EncodingType::Byte1: as Compile time Stack allocated memory
static std::array<uint8_t, LINEITEM_LINES> L_RETURNFLAG;         // [8]

/// Materialization L_LINESTATUS EncodingType::Byte1: as Compile time Stack allocated memory
static std::array<uint8_t, LINEITEM_LINES> L_LINESTATUS;    // [9]

/// Materialization L_QUANTITY EncodingType::Byte1: as Compile time Stack allocated memory
static std::array<uint8_t, LINEITEM_LINES> L_QUANTITY;    // [4]

/// Materialization L_ORDERKEY EncodingType::Byte4: as Compile time Stack allocated memory
static std::array<uint8_t , (LINEITEM_LINES << 2)> L_ORDERKEY;    // [0]
static std::array<uint8_t , (LINEITEM_LINES << 2)> L_ORDERKEY_;    // [0]

/// Materialization L_SUPPKEY EncodingType::Byte4: as Compile time Stack allocated memory
static std::array<uint8_t , (LINEITEM_LINES << 2)> L_SUPPKEY;    // [0]

/// Materialization L_SHIPMODE EncodingType::Byte4: as Compile time Stack allocated memory
static std::array<uint8_t , (LINEITEM_LINES)> L_SHIPMODE;    // [14]

/// Materialization L_SHIPDATE EncodingType::Byte2 as Compile time Stack allocated memory
static std::array<uint8_t , (LINEITEM_LINES << 1)> L_SHIPDATE;    // [10]
static std::array<uint8_t , (LINEITEM_LINES << 1)> L_SHIPDATE_;    // [10]

/// Materialization L_COMMITDATE EncodingType::Byte2: as Compile time Stack allocated memory
static std::array<uint8_t , (LINEITEM_LINES << 1)> L_COMMITDATE;    // [11]
static std::array<uint8_t , (LINEITEM_LINES << 1)> L_COMMITDATE_;    // [11]

/// Materialization L_RECEIPTDATE EncodingType::Byte1: as Compile time Stack allocated memory
static std::array<uint8_t , (LINEITEM_LINES << 1)> L_RECEIPTDATE;    // [12]
static std::array<uint8_t , (LINEITEM_LINES << 1)> L_RECEIPTDATE_;    // [12]

// Lineitem ENCODING
// [0]  1 Byte4
// [1]  2 Byte4
// [2]  1 Byte2
// [3]  0 Byte1
// [4]  1 Byte1  l_quantity  last   first_count() Byte2 LESS_THAN for val: 2400
// [5]  2 Byte4
// [6]  0 Byte1  l_discount  second first_count() Byte1 BETWEEN for val: 5  7
// [7]  0 Byte1
// [8]  0 Byte1
// [9]  0 Byte1 / Single
// [10] 1 Byte1  l_shipdate  first  first_count() Byte1 BETWEEN for val: 19940100  19950101
// [11] 1 Byte2
// [12] 1 Byte1 / Byte2
// [13] 1 Byte1
// [14] 1 Byte1

#endif

#ifdef MULTI_THREAD
constexpr uint8_t THREAD_NUM = 10;
static std::array<uint32_t, TUPLES_PER_DATABLOCK> MULTI_INDEX_VECTORS[THREAD_NUM][NUM_DB];
static std::array<uint32_t, NUM_DB> MULTI_MATCHES_INDEX_VECTORS[THREAD_NUM];

/// Materialization L_DISCOUNT EncodingType::Byte1
static std::array<uint8_t, LINEITEM_LINES> L_DISCOUNTS[THREAD_NUM];         // [6]
/// Materialization L_EXTENDEDPRICE EncodingType::Byte4
static std::array<uint8_t, (LINEITEM_LINES << 2)> L_EXTENDEDPRICES[THREAD_NUM];    // [5]
#endif
// ---------------------------------------------------------------------------
uint64_t getDataSize(EncodingType encodingType) {
  uint64_t db_dataSize = 0;
  switch (encodingType) {
  case EncodingType::Byte1: {
    db_dataSize = imlab::TUPLES_PER_DATABLOCK + 32;
  }
    break;
  case EncodingType::Byte2: {
    db_dataSize = 2 * imlab::TUPLES_PER_DATABLOCK + 32;
  }
    break;
  case EncodingType::Byte4: {
    db_dataSize = 4 * imlab::TUPLES_PER_DATABLOCK + 32;
  }
    break;
  case EncodingType::Byte8: {
    db_dataSize = 8 * imlab::TUPLES_PER_DATABLOCK + 32;
  }
    break;
  case EncodingType::Singlevalue: {
    db_dataSize = 32;
  }
  }
  return db_dataSize;
}

void generateSizeCsv_SLOTT( vector<vector<vector<uint64_t>>> table_portions, const std::vector<SchemaType>& schematypes) {
  std::ofstream myfile;
  myfile.open ("../data/size_compare_tpch.csv");
  if (myfile.is_open()) {
    myfile << "data_block\n";
    imlab::Table_Datablock<LINEITEM_INT_COLUMNS> db_table;
    uint64_t db_size = 0;
    for (size_t i = 0; i < NUM_DB; ++i) {
      std::vector<std::vector<std::string>> str_table_portion;

      auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS>::build(table_portions[i], str_table_portion, schematypes);

      db_size += sizeof(*db);
      auto encodings = db->getEncodings();
      for (size_t it = 0; it < LINEITEM_INT_COLUMNS; ++it) {
        db_size = db->getDataBlockSize();
      }
      myfile << db_size << "\n";
      db_table.AddDatablock(std::move(db));
    }

    // auto db = imlab::DataBlock<COLUMNS>::build(table_portion);
    // auto db_size = sizeof(*db);
    // auto encodings = db->getEncoding();
    // for (size_t i = 0; i < COLUMNS; ++i) {
    //   db_size += getDataSize(encodings[i]);
    //   myfile << SIZE_READ * (i + 1) << ", " << sizeof(table_portion) + sizeof(table_portion[0]) * table_portion.capacity() << ", "<< db_size << "\n";
    // }
    myfile.close();
  } else {
    std::cout << "error opening" << std::endl;
  }
}

// void generateSizeCsv_CompressionRate(vector<vector<vector<uint64_t>>> &table_portions) {
//   imlab::Table_Datablock<COLUMNS> db_table;
//   for (size_t i = 0; i < NUM_DB; ++i) {
//     auto db = imlab::DataBlock<COLUMNS>::build(table_portions[i]);
//     db_table.AddDatablock(std::move(db));
//   }
//   std::ofstream myfile;
//   myfile.open ("../data/size_compression_rate_tpch.csv");
//   if (myfile.is_open()) {
//     myfile << "num uint64_t,std::vector,data_block\n";
//     auto db_0 = imlab::DataBlock<COLUMNS>::build(table_portions[0]);
//     auto db_1 = imlab::DataBlock<COLUMNS>::build(table_portions[1]);
//     auto db_size = sizeof(*db_0) + sizeof(*db_1);
//     auto encodings_0 = db_0->getEncoding();
//     auto encodings_1 = db_1->getEncoding();
//     for (size_t i = 0; i < COLUMNS; ++i) {
//       db_size += getDataSize(encodings_0[i]) + getDataSize(encodings_1[i]);
//     }
//     myfile << SIZE * NUM_DB * COLUMNS << ", " << SIZE * 8 * COLUMNS * NUM_DB << ", "<< db_size << "\n";
//     myfile.close();
//   } else {
//     std::cout << "error opening" << std::endl;
//   }
// }

uint64_t Query_6(vector<vector<vector<uint64_t>>> &table_portions) {
  uint64_t l_shipdate_left = days_in_years[1994 - 1992] + 1;  // [10]  19940101
  uint64_t l_shipdate_right = days_in_years[1995 - 1992] - 1;  // 19950101
  uint64_t l_discount_left = 5;  // [6]
  uint64_t l_discount_right = 7;
  uint64_t l_quantity = 24;  // [4]
  std::vector<std::vector<uint32_t>> counters(table_portions.size());
  uint64_t first_count = 0;

  for (auto &counter : counters) counter.resize(imlab::TUPLES_PER_DATABLOCK);

  for (size_t i_outter = 0; i_outter < table_portions.size(); ++i_outter) {
    for (size_t i = 0; i < table_portions[i_outter][0].size(); ++i) {
      if (table_portions[i_outter][10][i] >= l_shipdate_left
          && table_portions[i_outter][10][i] <= l_shipdate_right) {
        counters[i_outter][first_count] = i;
        ++first_count;
      }
    }
    counters[i_outter].resize(first_count);
    first_count = 0;
  }

  for (size_t i_outter = 0; i_outter < table_portions.size(); ++i_outter) {
    for (size_t i_inner = 0; i_inner < counters[i_outter].size(); ++i_inner) {
      if (table_portions[i_outter][6][counters[i_outter][i_inner]] < l_discount_left
          || table_portions[i_outter][6][counters[i_outter][i_inner]] > l_discount_right) {
        counters[i_outter][i_inner] = UINT32_MAX;
      }
    }
  }

  for (size_t i_outter = 0; i_outter < counters.size(); ++i_outter) {
    for (auto& index : counters[i_outter]) {
      if (index != UINT32_MAX && table_portions[i_outter][4][index] >= l_quantity) {
        index = UINT32_MAX;
      }
    }
  }

  uint32_t count_if = 0;
  for (size_t i_outter = 0; i_outter < counters.size(); ++i_outter) {
    for (auto& index : counters[i_outter]) {
      if (index != UINT32_MAX) {
        /// Print Index Vector
//        std::cout << index << std::endl;
        ++count_if;
      }
    }
  }

  /// Materialization of Column DISCOUNT
  std::vector<uint64_t> discount;
  discount.reserve(count_if);
  size_t discount_counter = 0;
  for (size_t i_outter = 0; i_outter < counters.size(); ++i_outter) {
    for (auto& index : counters[i_outter]) {
      if (index != UINT32_MAX) {
        discount[discount_counter] = static_cast<uint8_t>(table_portions[i_outter][6][index]);
//        std::cout << discount[discount_counter] << "  " << table_portions[i_outter][10][index] << "  " << table_portions[i_outter][4][index] << std::endl;
//        std::cout << discount[discount_counter] << std::endl;
        discount_counter++;
      }
    }
  }
  std::cout << "discount_counter: " << discount_counter << std::endl;
  for (size_t dc = 0; dc < count_if; dc++) {
    std::cout << discount[dc] << std::endl;
  }
  return count_if;
}

void generateQuery6Csv_vector(vector<vector<vector<uint64_t>>> &table_portions) {
  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_vector.csv");
  myfile << "scans,std::vector\n";

  uint64_t count_if = 0;
  for (size_t i = 0; i < SCAN_ITERATIONS; ++i) {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = Query_6(table_portions);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    myfile << time_vec << "\n";
    std::cout << "match couter: " << count_if << std::endl;
  }
  myfile.close();
}

/**
 * Datablock Benchmark
 * RELEVANT FOR Perf
 * @param table_portions contains the tables, containing all the columns of Lineitems
 */
void generateQuery6Csv_db(vector<vector<vector<uint64_t>>>    &table_portions,
                          vector<vector<vector<std::string>>> &str_table_portion,
                          const std::vector<SchemaType>& schematypes) {
  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,datablock\n";

  uint64_t count_if = 0;  /// scan result

  /// Encoded Values
  uint64_t l_shipdate_left  = days_in_years[1994 - 1992] + 1;    // [10]  19940101
  uint64_t l_shipdate_right = days_in_years[1995 - 1992] - 1;   // 19950101
  uint64_t l_discount_left  = 5;                                // [6]
  uint64_t l_discount_right = 7;
  uint64_t l_quantity       = 24;                               // [4]

  /// Predicates
  Predicate predicate_10(10, l_shipdate_left, l_shipdate_right, CompareType::BETWEEN);
  Predicate predicate_6 (6, l_discount_left, l_discount_right, CompareType::BETWEEN);
  Predicate predicate_4 (4, l_quantity, CompareType::LESS_THAN);
  std::vector<Predicate> predicates;
  predicates.push_back(predicate_10);
  predicates.push_back(predicate_6);
  predicates.push_back(predicate_4);

  /// Table and build Datablock
  imlab::Table_Datablock<LINEITEM_INT_COLUMNS> db_table;
  for (size_t i = 0; i < NUM_DB; ++i) {

    auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS>::build(table_portions[i], str_table_portion[i], schematypes);
    db_table.AddDatablock(std::move(db));
  }

#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
  uint32_t* matches_index_vector = MATCHES_INDEX_VECTOR.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);

      /// Materialization code
      db_table.materialize<6, EncodingType::Byte1>(index_vectors, matches_index_vector, L_DISCOUNT.data());
      db_table.materialize<5, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_EXTENDEDPRICE.data()));
    }


    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << count_if << std::endl;
    myfile << time_vec << "\n";
  }
#endif

#ifdef MULTI_THREAD
  std::vector<uint64_t> times(THREAD_NUM); //TODO: debug
  std::vector<std::thread> threads;

  for (size_t i = 0; i < THREAD_NUM; ++i) {
    std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = MULTI_INDEX_VECTORS[i];
    std::array<uint32_t, NUM_DB>& MATCH_INDEX_VECTOR = MULTI_MATCHES_INDEX_VECTORS[i];
    uint32_t* matches_index_vector = MATCH_INDEX_VECTOR.data();

    threads.emplace_back([&db_table, &predicates, &times, i, index_vectors, matches_index_vector] {
      for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
        uint64_t count_if = 0;  /// scan result
        auto start_time = std::chrono::high_resolution_clock::now();

        for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
          count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);
          db_table.materialize<6, EncodingType::Byte1>(index_vectors, matches_index_vector, L_DISCOUNTS[i].data());
          db_table.materialize<5, EncodingType::Byte4>(index_vectors, matches_index_vector, L_EXTENDEDPRICES[i].data());
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        times[i] += time_vec;
        std::cout << count_if << std::endl;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  uint64_t total_time = 0;
  for (const auto& time : times) {
    total_time  += time;
    std::cout << "Each Time : " << time / SCAN_ITERATIONS << std::endl;
  }
  std::cout << "AVG per Thread: " << total_time / THREAD_NUM / SCAN_ITERATIONS << std::endl;
#endif

  myfile.close();
}

/**
 * Continuous Memory Benchmark
 * RELEVANT FOR Perf
 * @param table_portions contains the tables, containing all the columns of Lineitems
 */
void generateQuery6Csv_cm_db(vector<vector<vector<uint64_t>>> &table_portions,
                             vector<vector<vector<std::string>>> &str_table_portion,
                             const std::vector<SchemaType>& schematypes) {
  /// Almost same as the Datablock Main Mermoy version

  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,CM\n";

  // TODO: selectity still slow, because the 2rd, 3th predicate on a uniform distributed columns
  /// Encoded Values
  // ================  l_shipdate_================
  /// Selectivity: 909455.0 / 6001215 = 0.1515

  uint64_t l_shipdate_left = days_in_years[1994 - 1992] + 1;  // [10]  19940101
  uint64_t l_shipdate_right = days_in_years[1995 - 1992] - 1;  // 19950101

//  /// Selectivity: 899585.0 / 6001215 = 0.15 = 0.1499004
//  uint64_t l_shipdate_left = 19940105;  // [10]  19940105
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 839851.0 / 6001215 = 0.14 = 0.139946
//  uint64_t l_shipdate_left = 19940125;  // [10]  19940125
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 778064.0 / 6001215 = 0.13 = 0.1296510
//  uint64_t l_shipdate_left = 19940223;  // [10]  19940223
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 720390.0 / 6001215 = 0.12 = 0.120040
//  uint64_t l_shipdate_left = 19940318;  // [10]  19940318
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 661364.0 / 6001215 = 0.11 = 0.1102050
//  uint64_t l_shipdate_left = 19940410;  // [10]  19940410
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 605039.0 / 6001215 = 0.10 = 0.100819
//  uint64_t l_shipdate_left = 19940503;  // [10]  19940503
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 542921.0 / 6001215 = 0.09 = 0.090468
//  uint64_t l_shipdate_left = 19940528;  // [10]  19940528
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 485475.0 / 6001215 = 0.08 = 0.080896
//  uint64_t l_shipdate_left = 19940620;  // [10]  19940620
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 422900.0 / 6001215 = 0.07 = 0.0704690
//  uint64_t l_shipdate_left = 19940715;  // [10]  19940715
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 346304.0 / 6001215 = 0.06 = 0.0577056
//  uint64_t l_shipdate_left = 19940720;  // [10]  19940720
//  uint64_t l_shipdate_right = 19941231;  // 19950101

//  /// Selectivity: 293704.0 / 6001215 = 0.05 = 0.048940
//  uint64_t l_shipdate_left = 19940905;  // [10]  19940905
//  uint64_t l_shipdate_right = 19941231;  // 19950101

  /// Selectivity: 242847.0 / 6001215 = 0.04 = 0.04046630
//  uint64_t l_shipdate_left = 19940920;  // [10]  19940920
//  uint64_t l_shipdate_right = 19941231;  // 19950101

  /// Selectivity: 181168.0 / 6001215 = 0.03 = 0.030188
//  uint64_t l_shipdate_left = 19941020;  // [10]  19941020
//  uint64_t l_shipdate_right = 19941231;  // 19950101

  /// Selectivity: 116798.0 / 6001215 = 0.02 = 0.01946
//  uint64_t l_shipdate_left = 19941115;  // [10]  19941115
//  uint64_t l_shipdate_right = 19941231;  // 19950101

  /// Selectivity: 54563.0 / 6001215 = 0.01 = 0.009
//   uint64_t l_shipdate_left = 19941210;  // [10]  19941210
//   uint64_t l_shipdate_right = 19941231;  // 19950101

  // ================  l_discount ================
  // Selectivity: 1627406.0 / 6001215 = 0.2711
  uint64_t l_discount_left = 5;  // [6]
  uint64_t l_discount_right = 7;

  // Selectivity: 1084409.0 / 6001215 = 0.1806
  // uint64_t l_discount_left = 6;  // [6]
  // uint64_t l_discount_right = 7;

  // Selectivity: 1084603.0 / 6001215 = 0.1807 [MIN]
//   uint64_t l_discount_left = 5;  // [6]
//   uint64_t l_discount_right = 6;

  // ================  l_quantity ================
  // Selectivity: 2741544.0 / 6001215 = 0.4568
  uint64_t l_quantity = 24;  // [4]

  // Selectivity: 476492.0 / 6001215 = 0.08 = 0.079399
//  uint64_t l_quantity = 5;  // [4]

  // Selectivity: 119657.0 / 6001215 = 0.02 = 0.01993 [MIN]
//  uint64_t l_quantity = 2;  // [4]

  uint64_t count_if = 0;  /// scan result

  /// Predicates
  Predicate predicate_10(10, l_shipdate_left, l_shipdate_right, CompareType::BETWEEN); // min = 1992-01-02, max = 1998-12-01
  Predicate predicate_6(6, l_discount_left, l_discount_right, CompareType::BETWEEN);  // min = 0.00, max = 0.10 | uniform distribution: all values about 540000
  Predicate predicate_4(4, l_quantity, CompareType::LESS_THAN);                       // min = 1.00, max = 50.00 (all int but represented by float)| uniform distribution: all values about 120000
  std::vector<Predicate> predicates;
  predicates.push_back(predicate_10);
  predicates.push_back(predicate_6);
  predicates.push_back(predicate_4);

  /// Table and build Datablock
  imlab::Table_CM<LINEITEM_INT_COLUMNS> db_table;

  /// New a single buffermanager
  std::cout << "Building " << NUM_DB << " Continuous Memory." << std::endl;

  /// New this buffermanager
  //TODO: problem not located???
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE * (NUM_DB - 0))), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM * NUM_DB - 0, std::move(loaded_pages));

  for (size_t i = 0; i < NUM_DB; ++i) {
    auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS>::build(table_portions[i], str_table_portion[i], schematypes);

    std::cout << i << " st Page: Each page size: " << db->getDataBlockSize() << "  |  ";

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<LINEITEM_INT_COLUMNS> cm(i, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 0> str_index_arr{{}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    std::cout << "Each page number: " << cm.get_page_number() << std::endl;
    buffer_manager.add_used_page_count(cm.get_page_number());
    db_table.addCM(cm);
  }

#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
  uint32_t* matches_index_vector = MATCHES_INDEX_VECTOR.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
#ifdef DEBUG_TPCH
      std::cout << "b_table.scan_count(predicates) " << it * SCAN_ITERATIONS + scan << " starts!" << std::endl;
      buffer_manager.get_fifo_list();
      buffer_manager.get_lru_list();
#endif

      count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);

      db_table.materialize<6, EncodingType::Byte1>(index_vectors, matches_index_vector, L_DISCOUNT.data());
      db_table.materialize<5, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_EXTENDEDPRICE.data()));
#ifdef DEBUG_TPCH
      std::cout << "b_table.scan_count(predicates) " << it * SCAN_ITERATIONS + scan << " ends!" << std::endl;
      buffer_manager.get_fifo_list();
      buffer_manager.get_lru_list();
#endif
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    std::cout << count_if << std::endl;
    myfile << time_vec << "\n";
  }
#endif

#ifdef MULTI_THREAD
  std::vector<uint64_t> times(THREAD_NUM);
   std::vector<std::thread> threads;
    for (size_t i = 1; i < THREAD_NUM; ++i) {
     std::array<uint32_t, TUPLES_PER_DATABLOCK> *index_vectors =
         MULTI_INDEX_VECTORS[i];
     std::array<uint32_t, NUM_DB> &MATCH_INDEX_VECTOR =
         MULTI_MATCHES_INDEX_VECTORS[i];
     uint32_t *matches_index_vector = MATCH_INDEX_VECTOR.data();
     threads.emplace_back([&db_table, &predicates, &times, i, index_vectors,
                           matches_index_vector] {
       for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
         uint64_t count_if = 0; /// scan result
         auto start_time = std::chrono::high_resolution_clock::now();

         for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
           count_if = db_table.scan_count(predicates, index_vectors,
                                          matches_index_vector);
           db_table.materialize<6, EncodingType::Byte1>(
               index_vectors, matches_index_vector, L_DISCOUNTS[i].data());
           db_table.materialize<5, EncodingType::Byte4>(
               index_vectors, matches_index_vector, L_EXTENDEDPRICES[i].data());
         }

         auto end_time = std::chrono::high_resolution_clock::now();
         auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(
                             end_time - start_time)
                             .count();
         times[i] += time_vec;
         std::cout << count_if << std::endl;
       }
     });
   }

  /// Main Thread
   for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    uint64_t count_if = 0;  /// scan result
    auto start_time = std::chrono::high_resolution_clock::now();

    std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = MULTI_INDEX_VECTORS[0];
    std::array<uint32_t, NUM_DB>& MATCH_INDEX_VECTOR = MULTI_MATCHES_INDEX_VECTORS[0];
    uint32_t* matches_index_vector = MATCH_INDEX_VECTOR.data();
    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);
      db_table.materialize<6, EncodingType::Byte1>(index_vectors, matches_index_vector, L_DISCOUNTS[0].data());
      db_table.materialize<5, EncodingType::Byte4>(index_vectors, matches_index_vector, L_EXTENDEDPRICES[0].data());
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    times[0] += time_vec;
    std::cout << count_if << std::endl;
  }

    for (auto& thread : threads) {
        thread.join();
    }

    uint64_t total_time = 0;
    for (auto& time : times) {
        total_time  += time;
        std::cout << "Each Time : " << time / SCAN_ITERATIONS << std::endl;
    }
    std::cout << "AVG per Thread: " << total_time / THREAD_NUM / SCAN_ITERATIONS << std::endl;
#endif

  myfile.close();
}

std::vector<std::string> split(std::string strToSplit, char delimeter) {
  std::stringstream ss(strToSplit);
  std::string item;
  std::vector<std::string> splittedStrings;
  while (std::getline(ss, item, delimeter)) {
    splittedStrings.push_back(item);
  }
  return splittedStrings;
}

// 1994-01-01 to the right encoding!
int parseDate(const std::string &input) {
  int month;
  int day;
  int year;
  if (std::sscanf(input.c_str(), "%d-%d-%d", &year, &month, &day) != 3) {
    throw std::string("error format date");
  } if (year < 1992 || year > 1998) {
    throw std::string("FOCUS ON MIN AND MAX FROM DATE");
  }  else {
    // 1992 and 1996 are leap year
    int result = days_in_years[year - 1992] + days_in_year[month] + day;
    if ((year == 1992 && month > 2) || (year == 1996 && month > 2)) result++;
    if (result < 600 && year > 1995) std::cout << "ssss" << std::endl;
    return result;
  }
}

struct lineitem {
  uint64_t  col0;
  uint64_t  col1;
  uint64_t  col2;
  uint64_t  col3;
  uint64_t  col4;
  uint64_t  col5;
  uint64_t  col6;
  uint64_t  col7;
  uint64_t  col8;
  uint64_t  col9;
  uint64_t  col10;
  uint64_t  col11;
  uint64_t  col12;
  std::string  col13;
  std::string  col14;

  lineitem(
      uint64_t  col0,
      uint64_t  col1,
      uint64_t  col2,
      uint64_t  col3,
      uint64_t  col4,
      uint64_t  col5,
      uint64_t  col6,
      uint64_t  col7,
      uint64_t  col8,
      uint64_t  col9,
      uint64_t  col10,
      uint64_t  col11,
      uint64_t  col12,
      std::string col13,
      std::string col14)
      : col0(col0), col1(col1), col2(col2), col3(col3), col4(col4), col5(col5), col6(col6), col7(col7),
        col8(col8), col9(col9), col10(col10), col11(col11), col12(col12), col13(col13), col14(col14) {}
};

bool vec_sort_function (lineitem &table_portions_l, lineitem &table_portions_r) {
  return table_portions_l.col10 < table_portions_r.col10;
}

void sort() {
  // vector<uint64_t> table_portions_big;
  // table_portions_big.reserve(SIZE_READ);
  vector<lineitem> table_portions_big;
  table_portions_big.reserve(SIZE_READ);
  std::ifstream in(TPCH_LINEITEM_PATH);
  if (in.is_open()) {
    std::string lineFromText;
    uint64_t i = 0;  // lines read
    size_t current_num_db = 0;
    size_t vec_offset = 0;

    while (i < LINEITEM_LINES && std::getline(in, lineFromText)) {
      auto spilte_vec = split(lineFromText, '|');
      // auto &table_part_0 = table_portions_big[0];
      // auto &table_part_1 = table_portions_big[1];
      // auto &table_part_2 = table_portions_big[2];
      // auto &table_part_3 = table_portions_big[3];
      // auto &table_part_4 = table_portions_big[4];
      // auto &table_part_5 = table_portions_big[5];
      // auto &table_part_6 = table_portions_big[6];
      // auto &table_part_7 = table_portions_big[7];
      // auto &table_part_8 = table_portions_big[8];
      // auto &table_part_9 = table_portions_big[9];
      // auto &table_part_10 = table_portions_big[10];
      // auto &table_part_11 = table_portions_big[11];
      // auto &table_part_12 = table_portions_big[12];
      table_portions_big.emplace_back(
          (uint64_t)std::stol(spilte_vec[0]),
          (uint64_t)std::stol(spilte_vec[1]),
          (uint64_t)std::stol(spilte_vec[2]),
          (uint64_t)std::stol(spilte_vec[3]),
          (uint64_t)(std::stof(spilte_vec[4]) * 100),
          (uint64_t)(std::stof(spilte_vec[5]) * 100),
          (uint64_t)(std::stof(spilte_vec[6]) * 100),
          (uint64_t)(std::stof(spilte_vec[7]) * 100),
          (uint64_t)spilte_vec[8].at(0),
          (uint64_t)spilte_vec[9].at(0),
          (uint64_t)parseDate(spilte_vec[10]),
          (uint64_t)parseDate(spilte_vec[11]),
          (uint64_t)parseDate(spilte_vec[12]),
          spilte_vec[13],
          spilte_vec[14]
      );
      // table_part_0.emplace_back((uint64_t)std::stol(spilte_vec[0]));  // int
      // table_part_1.emplace_back((uint64_t)std::stol(spilte_vec[1]));  // int
      // table_part_2.emplace_back((uint64_t)std::stol(spilte_vec[2]));  // int
      // table_part_3.emplace_back((uint64_t)std::stol(spilte_vec[3]));  // int
      // table_part_4.emplace_back((uint64_t)(std::stof(spilte_vec[4]) * 100));  // float with 2 point number
      // table_part_5.emplace_back((uint64_t)(std::stof(spilte_vec[5]) * 100));  // float with 2 point number
      // table_part_6.emplace_back((uint64_t)(std::stof(spilte_vec[6]) * 100));  // float with 2 point number
      // table_part_7.emplace_back((uint64_t)(std::stof(spilte_vec[7]) * 100));  // float with 2 point number
      // table_part_8.emplace_back((uint64_t)spilte_vec[8].at(0));  // char
      // table_part_9.emplace_back((uint64_t)spilte_vec[9].at(0));  // char
      // table_part_10.emplace_back((uint64_t)parseDate(spilte_vec[10]));  // date
      // table_part_11.emplace_back((uint64_t)parseDate(spilte_vec[11]));  // date
      // table_part_12.emplace_back((uint64_t)parseDate(spilte_vec[12]));  // date
      i++;
    }

    std::sort(table_portions_big.begin(), table_portions_big.end(), vec_sort_function);
    std::cout << "std::sort done" << std::endl;
    in.close();
  } else {
    std::cout << "TPCH_LINEITEM_PATH open error" << std::endl;
    exit(1);
  }
  // for (auto& tb : table_portions_big[10]) {
  //   std::cout << " date: " << tb << std::endl;
  // }
  uint64_t i = 0;  // lines written
  std::fstream in_sorted(TPCH_LINEITEM_SORTED_PATH);
  if (in_sorted.is_open()) {
    while (i < LINEITEM_LINES) {
      in_sorted << table_portions_big[i].col0 << "|"
                << table_portions_big[i].col1 << "|"
                << table_portions_big[i].col2 << "|"
                << table_portions_big[i].col3 << "|"
                << table_portions_big[i].col4 << "|"
                << table_portions_big[i].col5 << "|"
                << table_portions_big[i].col6 << "|"
                << table_portions_big[i].col7 << "|"
                << table_portions_big[i].col8 << "|"
                << table_portions_big[i].col9 << "|"
                << table_portions_big[i].col10 << "|"
                << table_portions_big[i].col11 << "|"
                << table_portions_big[i].col12 << "|"
                << table_portions_big[i].col13 << "|"
                << table_portions_big[i].col14
                << std::endl;
      ++i;
    }
  } else {
    std::cout << "TPCH_LINEITEM_SORTED_PATH open error" << std::endl;
    exit(1);
  }
}

/**
 * Build the data in std::vector
 * NOT REVELANT FOR Perf
 * @param table_portions
 */
void getTablePortionsLineitem(
    vector<vector<vector<uint64_t>>> &table_portions,
    vector<vector<vector<std::string>>> &str_table_portion) {
  /// SELECT THE FILE
  std::ifstream in(TPCH_LINEITEM_SORTED_PATH);
  // std::ifstream in(TPCH_LINEITEM_PATH);

  if (in.is_open()) {
    std::string lineFromText;
    uint64_t i = 0;  // lines read
    size_t current_num_db = 0;
    size_t vec_offset = 0;
    while (i < SIZE_READ && std::getline(in, lineFromText)) {
      auto spilte_vec = split(lineFromText, '|');

      auto &table_part = table_portions[current_num_db];

      auto &str_table_part = str_table_portion[current_num_db];

      auto &table_part_0 = table_part[0];
      auto &table_part_1 = table_part[1];
      auto &table_part_2 = table_part[2];
      auto &table_part_3 = table_part[3];
      auto &table_part_4 = table_part[4];
      auto &table_part_5 = table_part[5];
      auto &table_part_6 = table_part[6];
      auto &table_part_7 = table_part[7];
      auto &table_part_8 = table_part[8];
      auto &table_part_9 = table_part[9];
      auto &table_part_10 = table_part[10];
      auto &table_part_11 = table_part[11];
      auto &table_part_12 = table_part[12];

      auto &table_part_13 = str_table_part[0];
      auto &table_part_14 = str_table_part[1];

      table_part_0[vec_offset] = ((uint64_t)std::stol(spilte_vec[0]));                   // int
      table_part_1[vec_offset] = ((uint64_t)std::stol(spilte_vec[1]));                   // int
      table_part_2[vec_offset] = ((uint64_t)std::stol(spilte_vec[2]));                   // int
      table_part_3[vec_offset] = ((uint64_t)std::stol(spilte_vec[3]));                   // int

      table_part_4[vec_offset] = ((uint64_t)(std::stol(spilte_vec[4]) / 100));           // l_quantity: float with 2 point number, but in fact all int, SO I devide by 100 here
      table_part_5[vec_offset] = ((uint64_t)(std::stol(spilte_vec[5])));                 // l_extendedprice: float with 2 point number
      table_part_6[vec_offset] = ((uint64_t)(std::stol(spilte_vec[6])));                 // float with 2 point number
      table_part_7[vec_offset] = ((uint64_t)(std::stol(spilte_vec[7])));                 // float with 2 point number

      table_part_8[vec_offset] = ((uint64_t)(spilte_vec[8][0]));                        // char
      table_part_9[vec_offset] = ((uint64_t)(spilte_vec[9][0]));                        // char

      table_part_10[vec_offset] = ((uint64_t)std::stol(spilte_vec[10]));                 // sorted date as int
      table_part_11[vec_offset] = ((uint64_t)std::stol(spilte_vec[11]));                 // sorted date as int
      table_part_12[vec_offset] = ((uint64_t)std::stol(spilte_vec[12]));                 // sorted date as int

      table_part_13[vec_offset] = spilte_vec[13];
      table_part_14[vec_offset] = spilte_vec[14];

      // std::cout << "i " << i << "  ";
      // std::cout << table_part[0][vec_offset] << "  ";
      // std::cout << table_part[1][vec_offset] << "  ";
      // std::cout << table_part[2][vec_offset] << "  ";
      // std::cout << table_part[3][vec_offset] << "  ";
      // std::cout << table_part[4][vec_offset] << "  ";
      // std::cout << table_part[5][vec_offset] << "  ";
      // std::cout << table_part[6][vec_offset] << "  ";
      // std::cout << table_part[7][vec_offset] << "  ";
      // std::cout << table_part[8][vec_offset] << "  ";
      // std::cout << table_part[9][vec_offset] << "  ";
      // std::cout << table_part[10][vec_offset] << "  ";
      // std::cout << table_part[11][vec_offset] << "  ";
      // std::cout << table_part[12][vec_offset] << "  " << std::endl;
      i++;
      vec_offset++;
      if (vec_offset == TUPLES_PER_DATABLOCK) { vec_offset = 0; ++current_num_db; }
    }
    in.close();
  } else {
    std::cout << "open error" << std::endl;
    exit(1);
  }
//   for (size_t in = 0; in < NUM_DB; in++) {
//     for (size_t i = 0; i < imlab::TUPLES_PER_DATABLOCK; ++i) {
//       std::cout << "outter offset " << in << " inside offset " << i << "  ";
//       std::cout << table_portions[in][0][i] << "  ";
//       std::cout << table_portions[in][1][i] << "  ";
//       std::cout << table_portions[in][2][i] << "  ";
//       std::cout << table_portions[in][3][i] << "  ";
//       std::cout << table_portions[in][4][i] << "  ";
//       std::cout << table_portions[in][5][i] << "  ";
//       std::cout << table_portions[in][6][i] << "  ";
//       std::cout << table_portions[in][7][i] << "  ";
//       std::cout << table_portions[in][8][i] << "  ";
//       std::cout << table_portions[in][9][i] << "  ";
//       std::cout << table_portions[in][10][i] << "  ";
//       std::cout << table_portions[in][11][i] << "  ";
//       std::cout << table_portions[in][12][i] << "  " << std::endl;
//     }
//   }
// std::cout << NUM_DB;
}

void getTablePortionsNation(
    vector<vector<vector<uint64_t>>> &table_portions,
    vector<vector<vector<std::string>>> &str_table_portion) {
  /// SELECT THE FILE
  std::ifstream in(TPCH_NATION_PATH);

  if (in.is_open()) {
    std::string lineFromText;
    uint64_t i = 0;  // lines read
    size_t current_num_db = 0;
    size_t vec_offset = 0;
    while (i < NATION_LINES && std::getline(in, lineFromText)) {
      auto spilte_vec = split(lineFromText, '|');

      auto &table_part = table_portions[current_num_db];

      auto &str_table_part = str_table_portion[current_num_db];

      auto &table_part_0 = table_part[0];
      auto &table_part_1 = table_part[1];

      auto &table_part_2 = str_table_part[0];
      auto &table_part_3 = str_table_part[1];

      table_part_0[vec_offset] = ((uint64_t)std::stol(spilte_vec[0]));                   // int
      table_part_1[vec_offset] = ((uint64_t)std::stol(spilte_vec[2]));                   // int

      table_part_2[vec_offset] = spilte_vec[1];
      table_part_3[vec_offset] = spilte_vec[3];

//       std::cout << "i " << i << "  ";
//       std::cout << table_part[0][vec_offset] << "  ";
//       std::cout << str_table_part[0][vec_offset] << "  ";
//       std::cout << table_part[1][vec_offset] << "  ";
//       std::cout << str_table_part[1][vec_offset] << "  " << std::endl;

      i++;
      vec_offset++;
      if (vec_offset == TUPLES_PER_DATABLOCK) { vec_offset = 0; ++current_num_db; }
    }
    in.close();
  } else {
    std::cout << "open error" << std::endl;
    exit(1);
  }
}

/// -------------------------------------------------------------------------------
/// -------------------------------------------------------------------------------
/// -----------------------------Query1--------------------------------------------
/// -------------------------------------------------------------------------------
/// -------------------------------------------------------------------------------
void generateQuery1Csv_db(vector<vector<vector<uint64_t>>> &table_portions,
                          const std::vector<SchemaType>& schematypes) {
  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,datablock\n";

  uint64_t count_if = 0;  /// scan result

  /// Encoded Values
  uint64_t l_shipdate  = days_in_years[1998 - 1992] - 30 - 90;    // [10]  '1998-12-01' - interval '90' day

  /// Predicates
  Predicate predicate_10(10, l_shipdate, CompareType::LESS_THAN);
  std::vector<Predicate> predicates;
  predicates.push_back(predicate_10);

  /// Table and build Datablock
  imlab::Table_Datablock<LINEITEM_INT_COLUMNS> db_table;
  for (size_t i = 0; i < NUM_DB; ++i) {
    std::vector<std::vector<std::string>> str_table_portion;

    auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS>::build(table_portions[i], str_table_portion, schematypes);
    db_table.AddDatablock(std::move(db));
  }

#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
  uint32_t* matches_index_vector = MATCHES_INDEX_VECTOR.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);

      /// Materialization code
      db_table.materialize<6, EncodingType::Byte1>(index_vectors, matches_index_vector, L_DISCOUNT.data());
      db_table.materialize<5, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_EXTENDEDPRICE.data()));
      db_table.materialize<8, EncodingType::Byte1>(index_vectors, matches_index_vector, L_RETURNFLAG.data());
      db_table.materialize<9, EncodingType::Byte1>(index_vectors, matches_index_vector, L_LINESTATUS.data());
      db_table.materialize<4, EncodingType::Byte1>(index_vectors, matches_index_vector, L_QUANTITY.data());
    }


    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << count_if << std::endl;
    myfile << time_vec << "\n";
  }
#endif
  myfile.close();
}

void generateQuery1Csv_cm_db(vector<vector<vector<uint64_t>>> &table_portions,
                             const std::vector<SchemaType>& schematypes) {
  /// Almost same as the Datablock Main Mermoy version

  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,CM\n";


  uint64_t count_if = 0;  /// scan result
  uint64_t l_shipdate  = days_in_years[1998 - 1992] - 30 - 90;    // [10]  '1998-12-01' - interval '90' day

  /// Predicates
  Predicate predicate_10(10, l_shipdate, CompareType::LESS_THAN);
  std::vector<Predicate> predicates;
  predicates.push_back(predicate_10);

  /// Table and build Datablock
  imlab::Table_CM<LINEITEM_INT_COLUMNS> db_table;

  /// New a single buffermanager
  std::cout << "Building " << NUM_DB << " Continuous Memory." << std::endl;

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE * (NUM_DB - 0))), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM * NUM_DB - 0, std::move(loaded_pages));

  for (size_t i = 0; i < NUM_DB; ++i) {
    std::vector<std::vector<std::string>> str_table_portion;

    auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS>::build(table_portions[i], str_table_portion, schematypes);

    std::cout << i << " st Page: Each page size: " << db->getDataBlockSize() << "  |  ";

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<LINEITEM_INT_COLUMNS> cm(i, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 0> str_index_arr{{}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    std::cout << "Each page number: " << cm.get_page_number() << std::endl;
    buffer_manager.add_used_page_count(cm.get_page_number());
    db_table.addCM(cm);
  }

#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
  uint32_t* matches_index_vector = MATCHES_INDEX_VECTOR.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {

      count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);

//      db_table.materialize<6, EncodingType::Byte1>(index_vectors, matches_index_vector, L_DISCOUNT.data());
//      db_table.materialize<5, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_EXTENDEDPRICE.data()));
//      db_table.materialize<8, EncodingType::Byte1>(index_vectors, matches_index_vector, L_RETURNFLAG.data());
//      db_table.materialize<9, EncodingType::Byte1>(index_vectors, matches_index_vector, L_LINESTATUS.data());
//      db_table.materialize<4, EncodingType::Byte1>(index_vectors, matches_index_vector, L_QUANTITY.data());
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    std::cout << count_if << std::endl;
    myfile << time_vec << "\n";
  }
#endif

  myfile.close();
}


/// -------------------------------------------------------------------------------
/// -------------------------------------------------------------------------------
/// -----------------------------Query7--------------------------------------------
/// -------------------------------------------------------------------------------
/// -------------------------------------------------------------------------------
void generateQuery7Csv_db(vector<vector<vector<uint64_t>>>    &lineitem_table_portions,
                          vector<vector<vector<std::string>>> &lineitem_str_table_portion,
                          const std::vector<SchemaType>       &lineitem_schematypes,
                          vector<vector<vector<uint64_t>>>    &nation_table_portions,
                          vector<vector<vector<std::string>>> &nation_str_table_portion,
                          const std::vector<SchemaType>       &nation_schematypes) {
  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,datablock\n";

  uint64_t count_if = 0;  /// scan result
  uint64_t count_if_ = 0;  /// scan result
  uint64_t count_if__ = 0;  /// scan result


  /// Encoded Values
  uint64_t l_shipdate_left  = days_in_years[1995 - 1992] + 1;   // [10]  19950101
  uint64_t l_shipdate_right = days_in_years[1996 - 1992];       // [10]  19961231

  /// Predicates
  Predicate lineitem_predicate_10(10, l_shipdate_left, l_shipdate_right, CompareType::BETWEEN);
  std::vector<Predicate> lineitem_predicates;
  lineitem_predicates.push_back(lineitem_predicate_10);

//  Predicate natoion_predicate_1(1, "FRANCE", CompareType::EQUAL);
//  std::vector<Predicate> nation_predicates;
//  nation_predicates.push_back(natoion_predicate_1);
//
//  Predicate natoion_predicate_1_(1, "GERMANY", CompareType::EQUAL);
//  std::vector<Predicate> nation_predicates_;
//  nation_predicates_.push_back(natoion_predicate_1_);

  /// Table and build lineitem Datablocks
  imlab::Table_Datablock<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS> lineitem_db_table;
  for (size_t i = 0; i < NUM_DB; ++i) {
    auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS>::build(lineitem_table_portions[i], lineitem_str_table_portion[i], lineitem_schematypes);
    lineitem_db_table.AddDatablock(std::move(db));
  }

  /// Table and build nation Datablocks
//  imlab::Table_Datablock<NATION_INT_COLUMNS + NATION_CHAR_COLUMNS> nation_db_table;
//  for (size_t i = 0; i < 1; ++i) {
//    auto db = imlab::DataBlock<NATION_INT_COLUMNS + NATION_CHAR_COLUMNS>::build(nation_table_portions[i], nation_str_table_portion[i], nation_schematypes);
//    nation_db_table.AddDatablock(std::move(db));
//  }



#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
  uint32_t* matches_index_vector = MATCHES_INDEX_VECTOR.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = lineitem_db_table.scan_count(lineitem_predicates, index_vectors, matches_index_vector);

      /// Materialization code
      lineitem_db_table.materialize<6, EncodingType::Byte1>(index_vectors, matches_index_vector, L_DISCOUNT.data());
      lineitem_db_table.materialize<5, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_EXTENDEDPRICE.data()));
      lineitem_db_table.materialize<0, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_ORDERKEY.data()));
      lineitem_db_table.materialize<1, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_SUPPKEY.data()));
    }

//    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
//      count_if_ = nation_db_table.scan_count(nation_predicates, index_vectors, matches_index_vector);
//      count_if__ = nation_db_table.scan_count(nation_predicates_, index_vectors, matches_index_vector);
//    }


    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << count_if << std::endl;
    std::cout << count_if_ << std::endl;
    std::cout << count_if__ << std::endl;

    myfile << time_vec << "\n";
  }
#endif
  myfile.close();
}

void generateQuery7Csv_cm_db(vector<vector<vector<uint64_t>>>    &lineitem_table_portions,
                             vector<vector<vector<std::string>>> &lineitem_str_table_portion,
                             const std::vector<SchemaType>       &lineitem_schematypes,
                             vector<vector<vector<uint64_t>>>    &nation_table_portions,
                             vector<vector<vector<std::string>>> &nation_str_table_portion,
                             const std::vector<SchemaType>       &nation_schematypes) {
  /// Almost same as the Datablock Main Mermoy version

  /// Open the output file
  std::ofstream myfile;
  myfile.open("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,CM\n";

  uint64_t count_if = 0; /// scan result

  /// Encoded Values
  uint64_t l_shipdate_left = days_in_years[1995 - 1992] + 1; // [10]  19950101
  uint64_t l_shipdate_right = days_in_years[1996 - 1992];    // [10]  19961231

  /// Predicates
  Predicate lineitem_predicate_10(10, l_shipdate_left, l_shipdate_right,
                                  CompareType::BETWEEN);
  std::vector<Predicate> lineitem_predicates;
  lineitem_predicates.push_back(lineitem_predicate_10);

  /// Table and build Datablock
  imlab::Table_CM<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS> db_table;

  /// New a single buffermanager
  std::cout << "Building " << NUM_DB << " Continuous Memory." << std::endl;

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(
      static_cast<char *>(imlab::aligned_malloc(
          32, MAX_PAGES_NUM * imlab::PAGE_SIZE * (NUM_DB - 0))),
      &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM * NUM_DB - 0,
                               std::move(loaded_pages));

  for (size_t i = 0; i < NUM_DB; ++i) {
    auto db =
        imlab::DataBlock<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS>::build(
            lineitem_table_portions[i], lineitem_str_table_portion[i],
            lineitem_schematypes);

    std::cout << i << " st Page: Each page size: " << db->getDataBlockSize()
              << "  |  ";

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS> cm(
        i, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 0> str_index_arr{{}};
    cm.build_conti_memory_segment(std::move(db), lineitem_schematypes,
                                  str_index_arr);

    std::cout << "Each page number: " << cm.get_page_number() << std::endl;
    buffer_manager.add_used_page_count(cm.get_page_number());
    db_table.addCM(cm);
  }

#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK> *index_vectors = INDEX_VECTORS;
  uint32_t *matches_index_vector = MATCHES_INDEX_VECTOR.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = db_table.scan_count(lineitem_predicates, index_vectors,
                                     matches_index_vector);

      db_table.materialize<6, EncodingType::Byte1>(
          index_vectors, matches_index_vector, L_DISCOUNT.data());
      db_table.materialize<5, EncodingType::Byte4>(
          index_vectors, matches_index_vector,
          reinterpret_cast<uint8_t *>(L_EXTENDEDPRICE.data()));
      db_table.materialize<0, EncodingType::Byte4>(
          index_vectors, matches_index_vector,
          reinterpret_cast<uint8_t *>(L_ORDERKEY.data()));
      db_table.materialize<1, EncodingType::Byte4>(
          index_vectors, matches_index_vector,
          reinterpret_cast<uint8_t *>(L_SUPPKEY.data()));
      auto end_time = std::chrono::high_resolution_clock::now();
      auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(
          end_time - start_time)
          .count();
      std::cout << count_if << std::endl;
      myfile << time_vec << "\n";
    }
#endif

#ifdef MULTI_THREAD
    std::vector<uint64_t> times(THREAD_NUM);
    std::vector<std::thread> threads;
    for (size_t i = 1; i < THREAD_NUM; ++i) {
      std::array<uint32_t, TUPLES_PER_DATABLOCK> *index_vectors =
          MULTI_INDEX_VECTORS[i];
      std::array<uint32_t, NUM_DB> &MATCH_INDEX_VECTOR =
          MULTI_MATCHES_INDEX_VECTORS[i];
      uint32_t *matches_index_vector = MATCH_INDEX_VECTOR.data();
      threads.emplace_back([&db_table, &predicates, &times, i, index_vectors,
                            matches_index_vector] {
        for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
          uint64_t count_if = 0; /// scan result
          auto start_time = std::chrono::high_resolution_clock::now();

          for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
            count_if = db_table.scan_count(predicates, index_vectors,
                                           matches_index_vector);
            db_table.materialize<6, EncodingType::Byte1>(
                index_vectors, matches_index_vector, L_DISCOUNTS[i].data());
            db_table.materialize<5, EncodingType::Byte4>(
                index_vectors, matches_index_vector,
                L_EXTENDEDPRICES[i].data());
          }

          auto end_time = std::chrono::high_resolution_clock::now();
          auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();
          times[i] += time_vec;
          std::cout << count_if << std::endl;
        }
      });
    }

    /// Main Thread
    for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
      uint64_t count_if = 0; /// scan result
      auto start_time = std::chrono::high_resolution_clock::now();

      std::array<uint32_t, TUPLES_PER_DATABLOCK> *index_vectors =
          MULTI_INDEX_VECTORS[0];
      std::array<uint32_t, NUM_DB> &MATCH_INDEX_VECTOR =
          MULTI_MATCHES_INDEX_VECTORS[0];
      uint32_t *matches_index_vector = MATCH_INDEX_VECTOR.data();
      for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
        count_if = db_table.scan_count(predicates, index_vectors,
                                       matches_index_vector);
        db_table.materialize<6, EncodingType::Byte1>(
            index_vectors, matches_index_vector, L_DISCOUNTS[0].data());
        db_table.materialize<5, EncodingType::Byte4>(
            index_vectors, matches_index_vector, L_EXTENDEDPRICES[0].data());
      }

      auto end_time = std::chrono::high_resolution_clock::now();
      auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(
                          end_time - start_time)
                          .count();
      times[0] += time_vec;
      std::cout << count_if << std::endl;
    }

    for (auto &thread : threads) {
      thread.join();
    }

    uint64_t total_time = 0;
    for (auto &time : times) {
      total_time += time;
      std::cout << "Each Time : " << time / SCAN_ITERATIONS << std::endl;
    }
    std::cout << "AVG per Thread: " << total_time / THREAD_NUM / SCAN_ITERATIONS
              << std::endl;
#endif

    myfile.close();
  }
}

/// -------------------------------------------------------------------------------
/// -------------------------------------------------------------------------------
/// -----------------------------Query12--------------------------------------------
/// -------------------------------------------------------------------------------
/// -------------------------------------------------------------------------------
void generateQuery12Csv_db(vector<vector<vector<uint64_t>>>    &table_portions,
                           vector<vector<vector<std::string>>> &str_table_portion,
                           const std::vector<SchemaType>& schematypes) {
  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,datablock\n";

  uint64_t count_if = 0;  /// scan result
  uint64_t count_if_ = 0;  /// scan result


  /// Encoded Values
  uint64_t l_receiptdate_left  = days_in_years[1994 - 1992] + 1;    // [10]  19940101
  uint64_t l_receiptdate_right = days_in_years[1995 - 1992];   // 19941231

  /// Predicates
  Predicate predicate_14(14, "MAIL", CompareType::EQUAL);
//  Predicate predicate_14_(14, "SHIP", CompareType::EQUAL);
  Predicate predicate_12(12, l_receiptdate_left, l_receiptdate_right, CompareType::BETWEEN);

  std::vector<Predicate> predicates;
  predicates.push_back(predicate_12);
  predicates.push_back(predicate_14);

//  std::vector<Predicate> predicates_;
//  predicates_.push_back(predicate_12);
//  predicates_.push_back(predicate_14_);

  /// Table and build Datablock
  imlab::Table_Datablock<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS /*15*/> db_table;
  for (size_t i = 0; i < NUM_DB; ++i) {

    auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS /*15*/>::build(table_portions[i], str_table_portion[i], schematypes);
    db_table.AddDatablock(std::move(db));
  }

#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors_ = INDEX_VECTORS_;

  uint32_t* matches_index_vector = MATCHES_INDEX_VECTOR.data();
  uint32_t* matches_index_vector_ = MATCHES_INDEX_VECTOR_.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);

//      count_if_ = db_table.scan_count(predicates_, index_vectors_, matches_index_vector_);

      /// Materialization code
//      db_table.materialize<0, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_ORDERKEY.data()));
////      db_table.materialize<0, EncodingType::Byte4>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_ORDERKEY_.data()));
////
//      db_table.materialize<10, EncodingType::Byte2>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_SHIPDATE.data()));
////      db_table.materialize<10, EncodingType::Byte2>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_SHIPDATE_.data()));
////
//      db_table.materialize<11, EncodingType::Byte2>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_COMMITDATE.data()));
////      db_table.materialize<11, EncodingType::Byte2>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_COMMITDATE_.data()));
////
//      db_table.materialize<12, EncodingType::Byte2>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_RECEIPTDATE.data()));
////      db_table.materialize<12, EncodingType::Byte2>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_RECEIPTDATE_.data()));

    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << count_if << std::endl;
    std::cout << count_if_ << std::endl;
    myfile << time_vec << "\n";
  }
#endif
  myfile.close();
}

void generateQuery12Csv_cm_db(vector<vector<vector<uint64_t>>>    &table_portions,
                              vector<vector<vector<std::string>>> &str_table_portion,
                              const std::vector<SchemaType>& schematypes) {
  /// Open the output file
  std::ofstream myfile;
  myfile.open ("../data/scan_compare_query6_tpch_db.csv");
  myfile << "scans,datablock\n";

  uint64_t count_if = 0;  /// scan result
  uint64_t count_if_ = 0;  /// scan result

  /// Encoded Values
  uint64_t l_receiptdate_left  = days_in_years[1994 - 1992] + 1;    // [10]  19940101
  uint64_t l_receiptdate_right = days_in_years[1995 - 1992];   // 19941231

  /// Predicates
  Predicate predicate_14(14, "MAIL", CompareType::EQUAL);
//  Predicate predicate_14(14, 2, CompareType::EQUAL);
  Predicate predicate_14_(14, "SHIP", CompareType::EQUAL);
  Predicate predicate_12(12, l_receiptdate_left, l_receiptdate_right, CompareType::BETWEEN);

  std::vector<Predicate> predicates;
  predicates.push_back(predicate_12);
  predicates.push_back(predicate_14);

//  std::vector<Predicate> predicates_;
//  predicates_.push_back(predicate_14_);
//  predicates_.push_back(predicate_12);

  /// Table and build Datablock
  imlab::Table_CM<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS> db_table;

  /// New a single buffermanager
  std::cout << "Building " << NUM_DB << " Continuous Memory." << std::endl;

  /// New this buffermanager
  imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, MAX_PAGES_NUM * imlab::PAGE_SIZE * (NUM_DB - 0))), &imlab::aligned_free_wrapper);
  BufferManager buffer_manager(MAX_PAGES_NUM * NUM_DB - 0, std::move(loaded_pages));

  for (size_t i = 0; i < NUM_DB; ++i) {
    auto db = imlab::DataBlock<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS>::build(table_portions[i], str_table_portion[i], schematypes);

    std::cout << i << " st Page: Each page size: " << db->getDataBlockSize() << "  |  ";

    /// New a Continuous_Memory_Segment with Segment Id = 0
    Continuous_Memory_Segment<LINEITEM_INT_COLUMNS + LINEITEM_CHAR_COLUMNS> cm(i, buffer_manager);

    /// after std::move: datablock in Memory is destroied
    std::array<uint8_t, 2> str_index_arr{{13, 14}};
    cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

    std::cout << "Each page number: " << cm.get_page_number() << std::endl;
    buffer_manager.add_used_page_count(cm.get_page_number());
    db_table.addCM(cm);
  }

#ifdef SINGLE_THREAD
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors = INDEX_VECTORS;
  std::array<uint32_t, TUPLES_PER_DATABLOCK>* index_vectors_ = INDEX_VECTORS_;

  uint32_t* matches_index_vector = MATCHES_INDEX_VECTOR.data();
  uint32_t* matches_index_vector_ = MATCHES_INDEX_VECTOR_.data();
  for (size_t it = 0; it < SCAN_ITERATIONS; ++it) {
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t scan = 0; scan < NUMBER_OF_SCANS; ++scan) {
      count_if = db_table.scan_count(predicates, index_vectors, matches_index_vector);

//      count_if_ = db_table.scan_count(predicates_, index_vectors_, matches_index_vector_);

      /// Materialization code
      db_table.materialize<0, EncodingType::Byte4>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_ORDERKEY.data()));
//      db_table.materialize<0, EncodingType::Byte4>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_ORDERKEY_.data()));

      db_table.materialize<10, EncodingType::Byte2>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_SHIPDATE.data()));
//      db_table.materialize<10, EncodingType::Byte2>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_SHIPDATE_.data()));

      db_table.materialize<11, EncodingType::Byte2>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_COMMITDATE.data()));
//      db_table.materialize<11, EncodingType::Byte2>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_COMMITDATE_.data()));

      db_table.materialize<12, EncodingType::Byte2>(index_vectors, matches_index_vector, reinterpret_cast<uint8_t*>(L_RECEIPTDATE.data()));
//      db_table.materialize<12, EncodingType::Byte2>(index_vectors, matches_index_vector_, reinterpret_cast<uint8_t*>(L_RECEIPTDATE_.data()));

    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto time_vec = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << count_if << std::endl;
    std::cout << count_if_ << std::endl;
    myfile << time_vec << "\n";
  }
#endif
  myfile.close();
}

int main(int argc, char **argv) {
  vector<vector<vector<uint64_t>>> table_portions    (NUM_DB, vector<vector<uint64_t>>(LINEITEM_INT_COLUMNS));
  vector<vector<vector<std::string>>> str_table_portions(NUM_DB, vector<vector<std::string>>(LINEITEM_CHAR_COLUMNS));

  vector<vector<vector<uint64_t>>> nation_table_portions    (1, vector<vector<uint64_t>>(NATION_INT_COLUMNS));
  vector<vector<vector<std::string>>> nation_str_table_portions(1, vector<vector<std::string>>(NATION_CHAR_COLUMNS));

  for (size_t i = 0; i < NUM_DB; i++) {
    if (i != NUM_DB - 1) {
      for (auto &v : table_portions[i]) {
        v.resize(TUPLES_PER_DATABLOCK);
      }
    } else {
      for (auto &v : table_portions[i]) {
        v.resize(LINEITEM_LINES % TUPLES_PER_DATABLOCK);
      }
    }
  }

  for (size_t i = 0; i < NUM_DB; i++) {
    if (i != NUM_DB - 1) {
      for (auto &v : str_table_portions[i]) {
        v.resize(TUPLES_PER_DATABLOCK);
      }
    } else {
      for (auto &v : str_table_portions[i]) {
        v.resize(LINEITEM_LINES % TUPLES_PER_DATABLOCK);
      }
    }
  }

  nation_table_portions[0][0].resize(NATION_LINES);
  nation_table_portions[0][1].resize(NATION_LINES);
  nation_str_table_portions[0][0].resize(NATION_LINES);
  nation_str_table_portions[0][1].resize(NATION_LINES);


//  sort();

  std::vector<SchemaType> lineitem_schematypes{
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Interger,
      SchemaType::Char,
      SchemaType::Char
  };

  std::vector<SchemaType> nation_schematypes{
      SchemaType::Interger,
      SchemaType::Char,
      SchemaType::Interger,
      SchemaType::Char,
  };

  /// Load all the Lineitems into table_portions
  getTablePortionsLineitem(table_portions, str_table_portions);

//  getTablePortionsNation(nation_table_portions, nation_str_table_portions);

  std::cout << "READ DONE" << std::endl;
  // generateSizeCsv_SLOTT(table_portions);
  // generateSizeCsv_CompressionRate(table_portions);

  /// Benchmark Functions

  /// std::vector in DRAM
//  generateQuery6Csv_vector(table_portions, schematypes);

  /// -------------------------------------------------------------------------------
  /// -------------------------------------------------------------------------------
  /// Datablock in DRAM
  /// -------------------------------------------------------------------------------
  /// -------------------------------------------------------------------------------

//  generateQuery12Csv_db(table_portions, str_table_portions, lineitem_schematypes);

//  generateQuery6Csv_db(table_portions, str_table_portions, lineitem_schematypes);

//  generateQuery7Csv_db(table_portions, str_table_portions, lineitem_schematypes, nation_table_portions, nation_str_table_portions, nation_schematypes);

//  generateQuery1Csv_db(table_portions, schematypes);

  /// -------------------------------------------------------------------------------
  /// -------------------------------------------------------------------------------
  /// Continuous Memory
  /// -------------------------------------------------------------------------------
  /// -------------------------------------------------------------------------------
//  generateQuery6Csv_cm_db(table_portions, str_table_portions, lineitem_schematypes);

//  generateQuery1Csv_cm_db(table_portions, schematypes);

//  generateQuery7Csv_cm_db(table_portions, str_table_portions, lineitem_schematypes, nation_table_portions, nation_str_table_portions, nation_schematypes);

  generateQuery12Csv_cm_db(table_portions, str_table_portions, lineitem_schematypes);
}
// ---------------------------------------------------------------------------