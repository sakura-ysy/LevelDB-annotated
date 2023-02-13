// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {

/**
 * 为了防止出现内存碎片，LevelDB自己写了一套内存分配管理器，名称为Arena。
 * 但要注意，Arena并不是为了整个LevelDB项目考虑的，主要是为了skiplist也就是memtable服务的
 * memtable里记录了用户传进来的k-v，这些字符串有长有短，放到内存中时很容易导致内存碎片，
 * 所以这里写了个统一的内存管理器。
 * 
 * memtable要申请内存时，就利用Arena分配器来分配内存。当memtable要释放时，就直接通过Arena中的
 * block_把所有申请的内存释放掉
*/


class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  // 使用方法同malloc()
  // 分配一片特定大小的内存，返回其首址
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  // 分配对齐的内容
  char* AllocateAligned(size_t bytes);

  // 为什么只有分配，没有释放？
  // 1.因为memtable本身是没有删除k-v的操作的，只会一直的添加
  // 2.memtable在flush之后，统一销毁掉，所以内存块可以直接由Arena来统一销毁

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  // 统计所有由Arena申请的内存总数
  // 这里面可能包含一些内存碎片
  // 所以返回的是近似值
  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  // 当前申请的内存块的指针
  char* alloc_ptr_;
  // 已经申请的内存块，剩余的bytes数
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  // 记录所有已经分配的内存块
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  // Arena总共申请出去的内存条目
  std::atomic<size_t> memory_usage_;
};


inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  // 如果当前block余下的空间还够用
  // 那就从中扣出bytes大小分配出去即可
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  // 不够用了，就申请新block
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
