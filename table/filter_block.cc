// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;  // 11是2K对2的对数
static const size_t kFilterBase = 1 << kFilterBaseLg; // 2^11=2K

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}


// block_offset是指filter block在SST中的偏移
// 1.只有一个filter block，其中包含多个filter
// 2.没2K大小的data block创建一个
// 3.filter block紧跟在data blocks之后
// 基于以上三点，只要给出block_offset即可得到filter的数目
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase); // 名字蛮奇怪，filter_number比较合适
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

/**
 * filter_block结构如下：
 * |        filter 1         |
 * |        filter 2         |
 * |        filter 3         |
 * |        filter 4         |
 * |     filter 1 offset     |
 * |     filter 2 offset     |
 * |     filter 3 offset     |
 * |     filter 4 offset     |
 * |  offset of offset array |
 * |      kFilterBaseLg      |
*/
Slice FilterBlockBuilder::Finish() {

  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  // 将result_中每一个filter的offest编码，并依次拼接在result_后
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }
  // filter的总数就是filter_offset_数组的偏移，将其也编码，拼接在result_后
  PutFixed32(&result_, array_offset);
  // 2k也编码
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  // 返回拼接后的整个result_结果
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  // 拿到key的数目
  const size_t num_keys = start_.size();
  
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size()); // 此时的result_还没有本次的filter
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  // base_lg_占1 bytes
  // filter_offset_数组的偏移占4 bytes
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  // last_word实际是filter_offset_数组的偏移
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word; // 取出filter_offset数组
  // filter的个数，一个filter占4 bytes
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
