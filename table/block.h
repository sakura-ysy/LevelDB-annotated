// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

/**
 * 1.data block
 * 2.meta block
 * 3.data block index
 * 4.meta block index
 * 上述四种结构都是基于类Block的
*/
class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }

  // record的迭代器
  // 只有New，没有Delete，说明用完需要手动释放（通过delete）
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter;

  uint32_t NumRestarts() const;

  // 数据区，由三部分组成
  // 1.顺序排列的多个record
  // 2.restart数组
  // 3.最后一个uint32记录num_restart，即restart个数
  // +----------------------------+ <----|
  // |         record 1           |      |
  // +----------------------------+      |
  // |         record 2           |      |
  // +----------------------------+      |
  // |         record 3           |      | 
  // +----------------------------+      | data_
  // |        restart 1           |      | 
  // +----------------------------+      |
  // |        restart 2           |      | 
  // +----------------------------+      |
  // |       num_restarts         |      | 
  // +----------------------------+ <----|
  // record格式 ->
  // record i   : | key共享长度 | key非共享长度 | value长度 | key非共享内容 | value内容 |
  // record i+1 : | key共享长度 | key非共享长度 | value长度 | key非共享内容 | value内容 |
  // 其中`共享`是指和前一个key共享的内容
  const char* data_;
  // 数据区data_的大小
  size_t size_;
  // restart数组在data_中的偏移
  uint32_t restart_offset_;  // Offset in data_ of restart array
  // 该block是否自己负责数据的申请和释放
  bool owned_;               // Block owns data_[]
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
