// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
// 用于生成一个string作为filter，仍然以block形式存在SST中
// filter block实际就是meta block
// 虽然SST中声称是支持多个meta block的，但实际上就只有一个，也就是filter block。
// 但是注意，filter block只有一个，而filter有很多个，
// 这个filter block中连续存放着多个filter
// 每当data blocks的大小达到2k时，就会创建一个filter
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  // 过滤策略
  const FilterPolicy* policy_;
  // keys_包含所有展开的key，并且它们都是连续存放在一起的，
  // 可以通过AddKey()看出。
  // 但是注意：
  // keys_不是filter block中的所有key，而block中当前filter的所有key.
  // 具体可以去看StartBlock()，每生成一次filter，都会清空keys_和start_
  std::string keys_;             // Flattened key contents
  // keys_中每一个key的起始位置
  std::vector<size_t> start_;    // Starting index in keys_ of each key
  
  /**
   * filter block虽然只有一个，但实际上filter block里面却有很多filter，
   * 每个filter都会利用一系列keys来生成相应的输入，这些输入连续存放在result_中，
   * filter_offsets_就是记录每个输入在result_中的位置。
   * 
   * result_与filter_offsets_的关系同keys_与start_的关系
  */
  std::string result_;           // Filter data computed so far
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
