# Data Block

There is my implementation about the data blocks.

Data Blocks is a Compressed Storage for DBMS.   
More about Data Block: [Data Blocks: Hybrid OLTP and OLAP on Compressed Storage using both Vectorization and Compilation](https://db.in.tum.de/downloads/publications/datablocks.pdf)


## Implemented Parts
- SMA
- SIMD optimized Scan
- SARGable predicates
- Attribute Compression
  
My Data Block is considered only for the `unsigned integer`, like:
- `uint8_t`
- `uint16_t`
- `uint32_t`
- `uint64_t`
  
The compression encoding:
- `Byte1`
- `Byte2`
- `Byte4`
- `Byte8`
- `single_value`
They are 4 different `integer` and `single_value`, which indicates the case, where all the values are same. 
