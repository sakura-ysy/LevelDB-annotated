// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

/**
 * xxxBuilder的工作均如下：
 * 1.构造builder对象
 * 2.向对象中添加数据，如data block中添加record，meta blcok中添加filter
 * 3.builder.Finish()完成block构建
 * 4.构建完成的结果一般存在内存中
*/

/**
 * BlockBuilder负责data block，FilterBlockBuilder负责filter block
 * 使用方式大致为：
 * BlockBuilder b;
 * b.Add(key, val);
 * b.Add(key, val);
 * content = b.Finish();
*/
class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  // 目标缓冲区，也就是按照输出格式处理好的内存区域
  // 其中只含有records，不包含restarts_数组
  std::string buffer_;              // Destination buffer
  // 每一个restart点的位置，
  // 一个restart内的key存在共享部分，只记录非共享部分
  std::vector<uint32_t> restarts_;  // Restart points
  // 一个restart内record的数目
  // 所以是个暂时的计数器
  int counter_;                     // Number of entries emitted since restart
  // 是否已经finish了
  bool finished_;                   // Has Finish() been called?
  // 记录最后一个key
  std::string last_key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
