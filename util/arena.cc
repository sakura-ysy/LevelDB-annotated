// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}


// 对AllocateNewBlock进行一次封装
// Arena中以block为单位分配内存，一个block最大为kBlockSize
// 如果bytes>1/4 kBlockSize，那么直接为其分配一个bytes大小的block
// 如果bytes<=1/4 kBlockSize，那么申请一个kBlockSize大小的block
// 并从中取出bytes的内存空间给它使用，预留下的空间可以供下一次分配
char* Arena::AllocateFallback(size_t bytes) {

  // 当bytes>1/4 kBlockSize时，如果为其申请一个完整的block（kBlockSize）
  // 那么余下的空间最多只有3/4 kBlockSize，Arena认为其为容易浪费的碎片
  // 一次直接分配一个bytes大小的block给它，不留剩余
  // 当然，如果bytes很大，那么分配的空间就不止一个block了
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // bytes<=1/4 kBlockSize，直接申请一个kBlockSize大小的block
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  // 从中扣出bytes大小的空间给它
  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}
 
// 对齐分配
char* Arena::AllocateAligned(size_t bytes) {
  // sizeof(void*)的值与目标平台位数有关
  // 32位：sizeof(void*) == 4
  // 64位：sizeof(void*) == 8
  // 默认按8 bytes对齐
  // 即要求地址为0、8、16、24、32...
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  
  // 取得当前地址未对齐的尾数，即超出的部分
  // 比如，要对齐的要求是8bytes
  // 但是当前指针指向的是0x017（23）这里。
  // 那么current_mod就是7
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  // 取出要补齐的部分
  // 如果当前地址是0x17, 要求对齐是8 bytes
  // 那么current_mod = 7, slop就是1
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);

  // 要补齐的部分算在申请者头上
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}


// new一个新块
// 把块指针push进block_中
// 增加memory_usage_
// 返回块指针
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
