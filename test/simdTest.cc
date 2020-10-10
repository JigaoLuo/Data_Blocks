// ---------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------
#include <math.h>
#include <gtest/gtest.h>
#include <immintrin.h>
#include <vector>
#include <cstdlib>
#include <iostream>
#include <ctime>
#include <tuple>
#include <map>
#include <cstddef>
#include "imlab/util/DataBlock_Types.h"
#include "imlab/util/MatchTable.h"
// ---------------------------------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------------------------------

using imlab::util::FirstScan_MatchTable;

TEST(SIMDTest, FirstScan_MatchTable) {
//   for (size_t i = 0; i < FirstScan_MatchTable.size(); ++i) {
//     std::cout << "offset: " << i << "  : ";
//     for (size_t in = 0; in < 8; ++in)
//       std::cout << FirstScan_MatchTable[i].cell[in] << " ";
//     std::cout << std::endl;
//   }
  for (size_t i = 0; i < 8; ++i)
    EXPECT_EQ(-256, FirstScan_MatchTable[0].cell[i]);
  EXPECT_EQ(1, FirstScan_MatchTable[1].cell[0]);
  EXPECT_EQ(257, FirstScan_MatchTable[2].cell[0]);
  for (size_t i = 1; i < 8; ++i) {
    EXPECT_EQ(-255, FirstScan_MatchTable[1].cell[i]);
    EXPECT_EQ(-255, FirstScan_MatchTable[2].cell[i]);
  }
  EXPECT_EQ(263, FirstScan_MatchTable[254].cell[0]);
  EXPECT_EQ(519, FirstScan_MatchTable[254].cell[1]);
  EXPECT_EQ(775, FirstScan_MatchTable[254].cell[2]);
  EXPECT_EQ(1031, FirstScan_MatchTable[254].cell[3]);
  EXPECT_EQ(1287, FirstScan_MatchTable[254].cell[4]);
  EXPECT_EQ(1543, FirstScan_MatchTable[254].cell[5]);
  EXPECT_EQ(1799, FirstScan_MatchTable[254].cell[6]);
  EXPECT_EQ(-249, FirstScan_MatchTable[254].cell[7]);

  EXPECT_EQ(8, FirstScan_MatchTable[255].cell[0]);
  EXPECT_EQ(264, FirstScan_MatchTable[255].cell[1]);
  EXPECT_EQ(520, FirstScan_MatchTable[255].cell[2]);
  EXPECT_EQ(776, FirstScan_MatchTable[255].cell[3]);
  EXPECT_EQ(1032, FirstScan_MatchTable[255].cell[4]);
  EXPECT_EQ(1288, FirstScan_MatchTable[255].cell[5]);
  EXPECT_EQ(1544, FirstScan_MatchTable[255].cell[6]);
  EXPECT_EQ(1800, FirstScan_MatchTable[255].cell[7]);
}
// ---------------------------------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------------------------------

