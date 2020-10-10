// ---------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------
#include <vector>
#include <cstdlib>
#include <iostream>
#include <ctime>
#include <tuple>
#include <map>
#include "imlab/datablock/DataBlock.h"
#include "benchmark/benchmark.h"
// ---------------------------------------------------------------------------
static std::vector<uint64_t> col0;
// ---------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------
void BM_MeaningfulName(benchmark::State &state) {
    char buffer[255];
    for (auto _ : state) {

        // The following line basically tells the compiler that
        // anything could happen with the contents of the buffer which
        // prevents many compiler optimizations.
        asm volatile("" : "+m" (buffer));
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
    state.SetBytesProcessed(state.iterations() * 1);

    // Use user-defined counters if you want to track something else.
    // state.counters["user_defined_counter"] = 42;
}

void BM_DataBlockStorage(benchmark::State &state) {
    char buffer[255];
    for (auto _ : state) {

        // The following line basically tells the compiler that
        // anything could happen with the contents of the buffer which
        // prevents many compiler optimizations.
        asm volatile("" : "+m" (buffer));
    }

    // state.SetItemsProcessed(state.iterations() * state.range(0));
    // state.SetBytesProcessed(state.iterations() * 1);

    // Use user-defined counters if you want to track something else.
    // state.counters["user_defined_counter"] = 42;
}
// ---------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------
BENCHMARK(BM_MeaningfulName) 
    -> Range(1 << 8, 1 << 10);
BENCHMARK(BM_DataBlockStorage) 
    -> Range(1 << 8, 1 << 16);    
// ---------------------------------------------------------------------------
int main(int argc, char **argv) {
    // Your could load TPCH into global vectors here
    size_t size = 1 << 16;
    col0.reserve(size);
    std::srand(std::time(nullptr));
    for (size_t i = 0; i < size; ++i) {
        col0.push_back((uint64_t) std::rand());
    }
    std::vector<std::vector<uint64_t>> table_portion {col0};
    auto db = imlab::DataBlock<1>::build(table_portion);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
}
// ---------------------------------------------------------------------------
