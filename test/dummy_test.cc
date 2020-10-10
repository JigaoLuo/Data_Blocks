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
// #include "imlab/buffer/PID.hh"
// #include "imlab/datablock/TID.hh"
// #include "imlab/datablock/Record.hh"
// #include "imlab/datablock/DataBlock_easy.hh"
// #include "imlab/datablock/SMA.hh"
// #include "imlab/datablock/PSMA.hh"
// #include "imlab/util/util.hh"
// #include "imlab/util/Predicate.hh"
// #include "imlab/util/Random.hh"
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------
// using imlab::Predicate;
// using imlab::EncodingType;
// using imlab::CompareType;

// TEST(DummyTest, MeaningfulName) {
//  EXPECT_EQ(42, imlab::foo());
//}

// TEST(PID, Segment_Page) {
//   imlab::PID pid(10, 20);
//   EXPECT_EQ(10, pid.getSegmentID());
//   EXPECT_EQ(20, pid.getPageID());
// }

// TEST(PID, toString) {
//   imlab::PID pid(200, 20);
//   EXPECT_EQ("(200,20)", (std::string)pid);
// }

// TEST(PID, maxValue) {
//   uint16_t maxSegmentID = pow(2, 16) - 1;
//   uint64_t maxPageID = pow(2, 48) - 1;
//   imlab::PID pid(maxSegmentID, maxPageID);
//   EXPECT_EQ(maxSegmentID, pid.getSegmentID());
//   EXPECT_EQ(maxPageID, pid.getPageID());
//   EXPECT_EQ(0, ~pid.getId());  // 1111 = ~0000
// }

// TEST(TID, Slot_Page) {
//   imlab::TID tid(10, 20);
//   EXPECT_EQ(10, tid.getPageId());
//   EXPECT_EQ(20, tid.getSlotId());
// }

// TEST(TID, toString) {
//   imlab::TID tid(200, 20);
//   EXPECT_EQ("(200,20)", (std::string)tid);
// }

// TEST(TID, maxValue) {
//   uint64_t maxPageID = pow(2, 48) - 1;
//   uint16_t maxSlotID = pow(2, 16) - 1;
//   imlab::TID tid(maxPageID, maxSlotID);
//   EXPECT_EQ(maxPageID, tid.getPageId());
//   EXPECT_EQ(maxSlotID, tid.getSlotId());
// }

// TEST(Record, FixNumber) {
//   int64_t fixNumber = 1024;
//   imlab::Record r(fixNumber);
//   EXPECT_EQ(fixNumber, r.getData());
// }

// TEST(Record, toString) {
//   int64_t fixNumber = 1024;
//   imlab::Record r(fixNumber);
//   EXPECT_EQ("Record: (1024)", (std::string) r);
// }

// TEST(Record, FixNumbers) {
//   int64_t fixNumbers[] = {1024, 2048, 256, 42};
//   size_t size = sizeof(fixNumbers) / sizeof(*fixNumbers);
//   std::vector<imlab::Record> records;
//   records.reserve(size);
//   for (size_t i = 0; i < size; ++i) {
//     records.push_back(imlab::Record(fixNumbers[i]));
//   }
//   for (size_t i = 0; i < size; ++i) {
//     EXPECT_EQ(fixNumbers[i], records[i].getData());
//   }
// }

// TEST(Record, RandomNumbers_twenty) {
//   size_t size = 20;
//   std::srand(std::time(nullptr));  // use current time as seed for random generator
//   int64_t fixNumbers[20];
//   for (size_t i = 0; i < size; ++i) {
//     fixNumbers[i] = (int64_t) std::rand();
//   }
//   std::vector<imlab::Record> records;
//   records.reserve(size);
//   for (size_t i = 0; i < size; ++i) {
//     records.push_back(imlab::Record(fixNumbers[i]));
//   }
//   for (size_t i = 0; i < size; ++i) {
//     EXPECT_EQ(fixNumbers[i], records[i].getData());
//   }
// }

// TEST(SMA, Init) {
//   int64_t min = 10;
//   int64_t max = 1024;
//   EXPECT_EQ(true, min <= max);
//   imlab::SMA<int64_t> sma(min, max);
//   EXPECT_EQ(min, sma.getSMA_min());
//   EXPECT_EQ(max, sma.getSMA_max());
//   EXPECT_EQ(true, sma.getSMA_min() <= sma.getSMA_max());

//   int32_t min_int32 = 10;
//   int32_t max_int32 = 1024;
//   EXPECT_EQ(true, min_int32 <= max_int32);
//   imlab::SMA<int32_t> sma_int32(min_int32, max_int32);
//   EXPECT_EQ(min_int32, sma_int32.getSMA_min());
//   EXPECT_EQ(max_int32, sma_int32.getSMA_max());
//   EXPECT_EQ(true, sma_int32.getSMA_min() <= sma_int32.getSMA_max());

//   int16_t min_int16 = 10;
//   int16_t max_int16 = 1024;
//   EXPECT_EQ(true, min_int16 <= max_int16);
//   imlab::SMA<int16_t> sma_int16(min_int16, max_int16);
//   EXPECT_EQ(min_int16, sma_int16.getSMA_min());
//   EXPECT_EQ(max_int16, sma_int16.getSMA_max());
//   EXPECT_EQ(true, sma_int16.getSMA_min() <= sma_int16.getSMA_max());

//   std::cout << "imlab::SMA<int64_t> size: " << sizeof(sma)
//             << " \nimlab::SMA<int32_t> size: " << sizeof(sma_int32)
//             << " \nimlab::SMA<int16_t> size: " << sizeof(sma_int16) << std::endl;
// }

// TEST(SMA, toString) {
//   int64_t min = 10;
//   int64_t max = 1024;
//   EXPECT_EQ(true, min <= max);
//   imlab::SMA<int64_t> sma(min, max);
//   EXPECT_EQ("SMA: (min: 10, max: 1024)", (std::string) sma);

//   int32_t min_int32 = 10;
//   int32_t max_int32 = 1024;
//   EXPECT_EQ(true, min_int32 <= max_int32);
//   imlab::SMA<int32_t> sma_int32(min_int32, max_int32);
//   EXPECT_EQ("SMA: (min: 10, max: 1024)", (std::string) sma_int32);

//   int16_t min_int16 = 10;
//   int16_t max_int16 = 1024;
//   EXPECT_EQ(true, min_int16 <= max_int16);
//   imlab::SMA<int16_t> sma_int16(min_int16, max_int16);
//   EXPECT_EQ("SMA: (min: 10, max: 1024)", (std::string) sma_int16);
// }

// TEST(SMA, Init_uniform_distribution) {
//   int64_t min = 1024;
//   int64_t max = 1024;
//   EXPECT_EQ(true, min <= max);
//   imlab::SMA<int64_t> sma(min, max);
//   EXPECT_EQ(min, sma.getSMA_min());
//   EXPECT_EQ(max, sma.getSMA_max());
//   EXPECT_EQ(true, sma.getSMA_min() <= sma.getSMA_max());

//   int32_t min_int32 = 1024;
//   int32_t max_int32 = 1024;
//   EXPECT_EQ(true, min_int32 <= max_int32);
//   imlab::SMA<int32_t> sma_int32(min_int32, max_int32);
//   EXPECT_EQ(min_int32, sma_int32.getSMA_min());
//   EXPECT_EQ(max_int32, sma_int32.getSMA_max());
//   EXPECT_EQ(true, sma_int32.getSMA_min() <= sma_int32.getSMA_max());

//   int16_t min_int16 = 1024;
//   int16_t max_int16 = 1024;
//   EXPECT_EQ(true, min_int16 <= max_int16);
//   imlab::SMA<int16_t> sma_int16(min_int16, max_int16);
//   EXPECT_EQ(min_int16, sma_int16.getSMA_min());
//   EXPECT_EQ(max_int16, sma_int16.getSMA_max());
//   EXPECT_EQ(true, sma_int16.getSMA_min() <= sma_int16.getSMA_max());
// }

// TEST(PSMA, Init_int32) {
//   // imlab::PSMA<int32_t> psma;
//   EXPECT_EQ(4, imlab::PSMA<int32_t>::BYTE);
//   EXPECT_EQ(4 * pow(2, 8), imlab::PSMA<int32_t>::SIZE);
//   EXPECT_EQ(3 * pow(2, 8), imlab::PSMA<int32_t>::ENETY_VALUES);
// }

// TEST(PSMA, Init_int16) {
//   // imlab::PSMA<int16_t> psma;
//   EXPECT_EQ(2, imlab::PSMA<int16_t>::BYTE);
//   EXPECT_EQ(2 * pow(2, 8), imlab::PSMA<int16_t>::SIZE);
//   EXPECT_EQ(1 * pow(2, 8), imlab::PSMA<int16_t>::ENETY_VALUES);
// }

// TEST(PSMA, Init_int64) {
//   // imlab::PSMA<int64_t> psma;
//   EXPECT_EQ(8, imlab::PSMA<int64_t>::BYTE);
//   EXPECT_EQ(8 * pow(2, 8), imlab::PSMA<int64_t>::SIZE);
//   EXPECT_EQ(7 * pow(2, 8), imlab::PSMA<int64_t>::ENETY_VALUES);
// }

// TEST(PSMA, getPSMASlot) {
//   EXPECT_EQ(5, imlab::PSMA<int64_t>::getPSMASlot(7, 2));
//   EXPECT_EQ(259, imlab::PSMA<int64_t>::getPSMASlot(998, 2));
//   EXPECT_EQ(5, imlab::PSMA<int32_t>::getPSMASlot(7, 2));
//   EXPECT_EQ(259, imlab::PSMA<int32_t>::getPSMASlot(998, 2));
//   EXPECT_EQ(5, imlab::PSMA<int16_t>::getPSMASlot(7, 2));
//   EXPECT_EQ(259, imlab::PSMA<int16_t>::getPSMASlot(998, 2));
// }

// TEST(PSMA, buildTable_find_fixNumbers_int64) {
//   imlab::PSMA<int64_t> psma;
//   int64_t fixNumbers[]= {1024, 2048, 256, 42, 100, 200, 300, 400, 500, 600};
//   psma.buildLookupTable(fixNumbers, 10, 42);
//   // when lookupTable is public
//   // for (size_t i = 0; i < imlab::PSMA<int64_t>::SIZE; ++i) {
//   //   if (std::get<0>(psma.lookupTable[i]) != 0 || std::get<1>(psma.lookupTable[i]) != 0)
//   //     std::cout << i << ": " << std::get<0>(psma.lookupTable[i]) << " " << std::get<1>(psma.lookupTable[i]) << std::endl;
//   // }
//   for (size_t i = 0; i < 10; ++i) {
//     auto range = psma.loopup(fixNumbers[i], 42);
//     // std::cout << std::get<0>(range) << "  " << std::get<1>(range) << std::endl;
//     bool found = false;
//     for (size_t index = std::get<0>(range); index <= std::get<1>(range); ++index) {
//       if (fixNumbers[i] == fixNumbers[index]) {
//         found = true;
//       }
//     }
//     if (found == false) {
//       EXPECT_EQ(1, 0);
//     }
//   }
//   EXPECT_EQ(1, 1);
// }

// TEST(PSMA, buildTable_find_fixNumbers_int32) {
//   imlab::PSMA<int32_t> psma;
//   int32_t fixNumbers[]= {1024, 2048, 256, 42, 100, 200, 300, 400, 500, 600};
//   psma.buildLookupTable(fixNumbers, 10, 42);
//   for (size_t i = 0; i < 10; ++i) {
//     auto range = psma.loopup(fixNumbers[i], 42);
//     bool found = false;
//     for (size_t index = std::get<0>(range); index <= std::get<1>(range); ++index) {
//       if (fixNumbers[i] == fixNumbers[index]) found = true;
//     }
//     if (found == false) EXPECT_EQ(1, 0);
//   }
//   EXPECT_EQ(1, 1);
// }

// TEST(PSMA, buildTable_find_randomNumbers_int64) {
//   imlab::PSMA<int64_t> psma;
//   size_t size = 1024;
//   std::srand(std::time(nullptr));  // use current time as seed for random generator
//   int64_t fixNumbers[1024];
//   int64_t min = 2;
//   fixNumbers[0] = min;
//   for (size_t i = 1; i < size; ++i) {
//     fixNumbers[i] = (int64_t) std::rand() + 5;
//   }
//   psma.buildLookupTable(fixNumbers, size, min);
//   for (size_t i = 0; i < size; ++i) {
//     auto range = psma.loopup(fixNumbers[i], min);
//     bool found = false;
//     for (size_t index = std::get<0>(range); index <= std::get<1>(range); ++index) {
//       if (fixNumbers[i] == fixNumbers[index]) found = true;
//     }
//     if (found == false) EXPECT_EQ(1, 0);
//   }
//   EXPECT_EQ(1, 1);
// }

// TEST(PSMA, buildTable_find_randomNumbers_int32) {
//   imlab::PSMA<int32_t> psma;
//   size_t size = 1024;
//   std::srand(std::time(nullptr));  // use current time as seed for random generator
//   int32_t fixNumbers[1024];
//   int32_t min = 2;
//   fixNumbers[0] = min;
//   for (size_t i = 1; i < size; ++i) {
//     fixNumbers[i] = (int64_t) std::rand() + 5;
//   }
//   psma.buildLookupTable(fixNumbers, size, min);
//   for (size_t i = 0; i < size; ++i) {
//     auto range = psma.loopup(fixNumbers[i], min);
//     bool found = false;
//     for (size_t index = std::get<0>(range); index <= std::get<1>(range); ++index) {
//       if (fixNumbers[i] == fixNumbers[index]) found = true;
//     }
//     if (found == false) EXPECT_EQ(1, 0);
//   }
//   EXPECT_EQ(1, 1);
// }

// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------
