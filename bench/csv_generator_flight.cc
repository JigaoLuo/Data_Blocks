// ---------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------
#include "imlab/datablock/DataBlock.h"
#include "imlab/datablock/Table.h"
#include "imlab/util/DataBlock_Types.h"
#include "imlab/util/Predicate.h"
#include "imlab/util/Random.h"
#include "imlab/util/types.h"
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
// ---------------------------------------------------------------------------------------------------
using std::vector;
using imlab::util::Predicate;
using imlab::util::EncodingType;
using imlab::util::CompareType;
using imlab::util::Random;
using imlab::util::SchemaType;
// ---------------------------------------------------------------------------
const char * const FLIGHT_DATA_PATH = "/home/jigao/Desktop/2008.csv";
// constexpr uint64_t SIZE = 1 << 16;
// constexpr size_t NUM_DB = SIZE / imlab::TUPLES_PER_DATABLOCK;
constexpr size_t COLUMNS = 29;
constexpr size_t LINEITEM_LINES = 2'241'757;  // 2 ^ 22 = 4 194 304
constexpr size_t NUM_DB = LINEITEM_LINES / imlab::TUPLES_PER_DATABLOCK;
constexpr uint64_t SIZE = NUM_DB * imlab::TUPLES_PER_DATABLOCK;
// ---------------------------------------------------------------------------
uint64_t getDataSize(EncodingType encodingType) {
  uint64_t db_dataSize = 0;
  switch (encodingType) {
    case EncodingType::Byte1: {
      db_dataSize = SIZE + 32;
    }
      break;
    case EncodingType::Byte2: {
      db_dataSize = 2 * SIZE + 32;
    }
      break;
    case EncodingType::Byte4: {
      db_dataSize = 4 * SIZE + 32;
    }
      break;
    case EncodingType::Byte8: {
      db_dataSize = 8 * SIZE + 32;        
    }
      break;
    case EncodingType::Singlevalue: {
      db_dataSize = 32;
    }
  }
  return db_dataSize;
}

void generateSizeCsv_SLOTT(vector<vector<vector<uint64_t>>> table_portions, std::vector<SchemaType> schematypes) {
  std::ofstream myfile;
  myfile.open ("../data/size_compare_flight.csv");
  if (myfile.is_open()) {
    myfile << "data_block\n";
    imlab::Table_Datablock<COLUMNS> db_table;
    uint64_t db_size = 0;
    uint64_t db_size_ = 0;

    for (size_t i = 0; i < NUM_DB; ++i) {
      std::vector<std::vector<std::string>> str_table_portion;

      auto db = imlab::DataBlock<COLUMNS>::build(table_portions[i], str_table_portion, schematypes);
      db_size_ += sizeof(*db);
      auto encodings = db->getEncodings();
      db_size += db->getDataBlockSize();
      db_table.AddDatablock(std::move(db));
    }
    std::cout << db_size << "\n";
    std::cout<< db_size_ << "\n";




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

void generateSizeCm_SLOTT(vector<vector<vector<uint64_t>>> table_portions, std::vector<SchemaType> schematypes) {
  std::ofstream myfile;
  myfile.open ("../data/size_compare_flight.csv");
  uint64_t db_size = 0;
  uint64_t db_size_ = 0;
  if (myfile.is_open()) {
    myfile << "data_block\n";
    imlab::Table_Datablock<COLUMNS> db_table;


    for (size_t i = 0; i < NUM_DB; ++i) {

      std::vector<std::vector<std::string>> str_table_portion;
      auto db = imlab::DataBlock<COLUMNS>::build(table_portions[i], str_table_portion, schematypes);

      /// New this buffermanager
      imlab::unique_ptr_aligned<char[]> loaded_pages(static_cast<char*>(imlab::aligned_malloc(32, 20 * 1024 * 16 * 2)), &imlab::aligned_free_wrapper);
      imlab::BufferManager buffer_manager(20, std::move(loaded_pages));

      /// New a Continuous_Memory_Segment with Segment Id = 0
      imlab::Continuous_Memory_Segment<COLUMNS> cm(0, buffer_manager);
      std::array<uint8_t, 0> str_index_arr{{}};

      /// after std::move: datablock in Memory is destroied
      cm.build_conti_memory_segment(std::move(db), schematypes, str_index_arr);

      db_size += cm.get_page_number();
      db_size_ += sizeof(cm.get_column_infos());

//      db_size_ += sizeof(*db);
//      auto encodings = db->getEncodings();
//      db_size += db->getDataBlockSize();
//      db_table.AddDatablock(std::move(db));
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
  std::cout << db_size << "\n";
  std::cout<< db_size_ << "\n";
}

//void generateSizeCsv_CompressionRate(vector<vector<vector<uint64_t>>> &table_portions) {
//  imlab::Table_Datablock<COLUMNS> db_table;
//  for (size_t i = 0; i < NUM_DB; ++i) {
//    auto db = imlab::DataBlock<COLUMNS>::build(table_portions[i]);
//    db_table.AddDatablock(std::move(db));
//  }
//  std::ofstream myfile;
//  myfile.open ("../data/size_compression_rate_tpch.csv");
//  if (myfile.is_open()) {
//    myfile << "num uint64_t,std::vector,data_block\n";
//    auto db_0 = imlab::DataBlock<COLUMNS>::build(table_portions[0]);
//    auto db_1 = imlab::DataBlock<COLUMNS>::build(table_portions[1]);
//    auto db_size = sizeof(*db_0) + sizeof(*db_1);
//    auto encodings_0 = db_0->getEncodings();
//    auto encodings_1 = db_1->getEncodings();
//    for (size_t i = 0; i < COLUMNS; ++i) {
//      db_size += getDataSize(encodings_0[i]) + getDataSize(encodings_1[i]);
//    }
//    myfile << SIZE * NUM_DB * COLUMNS << ", " << SIZE * 8 * COLUMNS * NUM_DB << ", "<< db_size << "\n";
//    myfile.close();
//  } else {
//    std::cout << "error opening" << std::endl;
//  }
//}
//
//std::vector<std::string> split(std::string strToSplit, char delimeter) {
//  std::stringstream ss(strToSplit);
//  std::string item;
//  std::vector<std::string> splittedStrings;
//  while (std::getline(ss, item, delimeter)) {
//    if (item == "NA") item = "255";
//    if (item == "") item = "255";
//    splittedStrings.push_back(item);
//  }
//  return splittedStrings;
//}

// 1994-01-01 to 19940101
int parseDate(const std::string &input) {
  int month;
  int day;
  int year;
  if (std::sscanf(input.c_str(), "%d-%d-%d", &year, &month, &day) != 3) {
    std::cout << "error format date" << std::endl;
  } else {
    // check values to avoid int overflow if you can be bothered
    return  404 * year + 32 * month + day;  // (12 * 31 + 31 + 1)  (31 + 1) 
  }
}

uint64_t code_tenth(std::string str) {
  auto length = str.length();
  uint64_t coded = 0;
  for (size_t i = 0; i < length; ++i) {
    coded += (str.at(i) << (8 * (length - 1 - i)));
  }
  return coded;
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

void getTablePortions(vector<vector<vector<uint64_t>>> &table_portions) {
  std::ifstream in(FLIGHT_DATA_PATH);
  if (in.is_open()) {
    std::string lineFromText;
    uint64_t i = 0;  // lines read
    size_t current_num_db = 0;
    size_t vec_offset = 0; 
    while (i < SIZE && std::getline(in, lineFromText)) {
      auto spilte_vec = split(lineFromText, ',');
      auto &table_part = table_portions[current_num_db];
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
      auto &table_part_13 = table_part[13];
      auto &table_part_14 = table_part[14];
      auto &table_part_15 = table_part[15];
      auto &table_part_16 = table_part[16];
      auto &table_part_17 = table_part[17];
      auto &table_part_18 = table_part[18];
      auto &table_part_19 = table_part[19];
      auto &table_part_20 = table_part[20];
      auto &table_part_21 = table_part[21];
      auto &table_part_22 = table_part[22];
      auto &table_part_23 = table_part[23];
      auto &table_part_24 = table_part[24];
      auto &table_part_25 = table_part[25];
      auto &table_part_26 = table_part[26];
      auto &table_part_27 = table_part[27];
      auto &table_part_28 = table_part[28];

      table_part_0[vec_offset] = ((uint64_t)std::stol(spilte_vec[0]));  // int
      table_part_1[vec_offset] = ((uint64_t)std::stol(spilte_vec[1]));  // int
      table_part_2[vec_offset] = ((uint64_t)std::stol(spilte_vec[2]));  // int
      table_part_3[vec_offset] = ((uint64_t)std::stol(spilte_vec[3]));  // int
      if (spilte_vec[4] != "NA") {
        table_part_4[vec_offset] = ((uint64_t)std::stol(spilte_vec[4])); // int
      } else {
        table_part_4[vec_offset] = 255; // int
      }
      table_part_5[vec_offset] = ((uint64_t)std::stol(spilte_vec[5]));  // int

      if (spilte_vec[6] != "NA") {
        table_part_6[vec_offset] = ((uint64_t)std::stol(spilte_vec[6]));  // int
      } else {
        table_part_6[vec_offset] = 255; // int
      }
      table_part_7[vec_offset] = ((uint64_t)std::stol(spilte_vec[7]));  // int
      table_part_8[vec_offset] = ((uint64_t)spilte_vec[8].at(0) << 8 + spilte_vec[8].at(1));  // 2 char
      table_part_9[vec_offset] = ((uint64_t)std::stol(spilte_vec[9]));  // int
      // skip 10 th
      table_part_28[vec_offset] = code_tenth(spilte_vec[10]);  // int

      if (spilte_vec[11] != "NA") {
        table_part_10[vec_offset] = ((uint64_t)std::stol(spilte_vec[11]));  // int
      } else {
        table_part_10[vec_offset] = 255; // int
      }

      if (spilte_vec[12] != "NA") {
        table_part_11[vec_offset] = ((uint64_t)std::stol(spilte_vec[12]));  // int
      } else {
        table_part_11[vec_offset] = 255; // int
      }

      if (spilte_vec[13] != "NA") {
        table_part_12[vec_offset] = ((uint64_t)std::stol(spilte_vec[13]));  // int
      } else {
        table_part_12[vec_offset] = 255; // int
      }

      if (spilte_vec[15] != "NA") {
        table_part_14[vec_offset] = ((uint8_t)std::stol(spilte_vec[15]));  // int
      } else {
        table_part_14[vec_offset] = 255; // int
      }
      table_part_15[vec_offset] = ((uint64_t)(spilte_vec[16].at(0) << 16) + (spilte_vec[16].at(1) << 8) + spilte_vec[16].at(2));  // 3 char
      table_part_16[vec_offset] = ((uint64_t)(spilte_vec[17].at(0) << 16) + (spilte_vec[17].at(1) << 8) + spilte_vec[17].at(2));  // 3 char
      table_part_17[vec_offset] = ((uint64_t)std::stol(spilte_vec[18]));  // int

      if (spilte_vec[19] != "NA") {
        table_part_18[vec_offset] = ((uint64_t)std::stol(spilte_vec[19]));  // int
      } else {
        table_part_18[vec_offset] = 255; // int
      }

      if (spilte_vec[20] != "NA") {
        table_part_19[vec_offset] = ((uint64_t)std::stol(spilte_vec[20]));  // int
      } else {
        table_part_19[vec_offset] = 255; // int
      }
      table_part_20[vec_offset] = ((uint64_t)std::stol(spilte_vec[21]));  // int

      if (spilte_vec[22] == "") {
        table_part_21[vec_offset] = 0;  // int
      } else if (spilte_vec[22] != "255") {
        table_part_21[vec_offset] = ((uint64_t)spilte_vec[22].at(0));  // int
      } else {
        table_part_21[vec_offset] = UINT8_MAX;
      }
      if (spilte_vec[23] != "255") {
        table_part_22[vec_offset] = ((uint64_t)std::stol(spilte_vec[23]));  // int
      } else {
        table_part_22[vec_offset] = UINT8_MAX;
      }
      if (spilte_vec[24] != "255" && spilte_vec[24] != "NA") {
        table_part_23[vec_offset] = ((uint64_t)std::stol(spilte_vec[24]));  // int
      } else {
        table_part_23[vec_offset] = UINT8_MAX;
      }
      if (spilte_vec[25] != "255" && spilte_vec[25] != "NA") {
        table_part_24[vec_offset] = ((uint64_t)std::stol(spilte_vec[25]));  // int
      } else {
        table_part_24[vec_offset] = UINT8_MAX;
      }
      if (spilte_vec[26] != "255" && spilte_vec[26] != "NA") {
        table_part_25[vec_offset] = ((uint64_t)std::stol(spilte_vec[26]));  // 3char
      } else {
        table_part_25[vec_offset] = UINT8_MAX;
      } 
      if (spilte_vec[27] != "NA") {
        table_part_26[vec_offset] = ((uint64_t)std::stol(spilte_vec[27]));  // 3char
      } else {
        table_part_26[vec_offset] = UINT8_MAX;
      } 
      if (spilte_vec[28] != "NA") {
        table_part_27[vec_offset] = ((uint64_t)std::stol(spilte_vec[28]));  // int
      } else {
        table_part_27[vec_offset] = UINT8_MAX;
      }
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
      if (vec_offset == imlab::TUPLES_PER_DATABLOCK) { vec_offset = 0; ++current_num_db; }
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


int main(int argc, char **argv) {
  vector<vector<vector<uint64_t>>> table_portions(NUM_DB, vector<vector<uint64_t>>(COLUMNS));
  for (auto &vec : table_portions) {
    for (auto &v : vec) {
      v.resize(imlab::TUPLES_PER_DATABLOCK);
    }
  }
  getTablePortions(table_portions);
  std::vector<SchemaType> schematypes{SchemaType::Interger,
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
                                    SchemaType::Interger,
                                    SchemaType::Interger};
//  generateSizeCsv_SLOTT(table_portions, schematypes);
  generateSizeCm_SLOTT(table_portions, schematypes);

    // generateSizeCsv_CompressionRate(table_portions);
}
// ---------------------------------------------------------------------------