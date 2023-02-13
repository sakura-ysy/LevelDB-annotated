// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

// 二级迭代器
// sst中有data block和data index block
// 前者存放k-v，后者存放前者的索引（分隔符）
// 第一级Iter服务于data index block
// 第二级Iter服务于data block
class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  // 不管是啥Iter
  // 只要继承自Iterator，那么该有的接口就要有
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  // 实际上就是指向Table::BlockReader()函数
  BlockFunction block_function_;
  // 使用的时候，就是指向sst
  void* arg_;
  // 读选项
  const ReadOptions options_;
  // 状态
  Status status_;
  // 第一级Iter，服务于data index block
  IteratorWrapper index_iter_;
  // 第二级Iter，服务于data block
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  // 只有data_iter_非空时该字段才有效
  // 为data block的句柄
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  // index_iter_移到first，即第一个data block
  index_iter_.SeekToFirst();
  // 生成data_iter_
  InitDataBlock();
  // data_iter_移到first，即第一个record（k-v）
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  // 一般而言，每次走完data_iter_都判断一下最为保险
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

// 注意
// 二级Iter的遍历
// 一定是第二级（data_iter_）先走，走到边界后第一级（index_iter_）才走
// 通过SkipEmptyDataBlocksForward/Backward()判断第二级是否走到边界处
void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

// 当data_iter一直往前走时，总会走到下一个data block的开头的
// 该函数就是用来检查data_iter_如果要走下一步的话，是否需要设置到下一个非空block的iter那里
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  // 如果data_iter_为空，或者走到底了
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    // index_iter_向前走一步，指向下一个block的iter
    index_iter_.Next();
    // 生成data_iter_
    InitDataBlock();
    // data_iter_移到first
    // 之前在Iterator时分析过，一个iter正常使用之前必须要Seek一下，否则出错
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

// 同上，只不过是向后了
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

// 设置data iter
// 如果不为空（.iter()非空），则先保存状态
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  // 赋值
  // Set()会先释放掉原有的内存，在赋值
  data_iter_.Set(data_iter);
}

// 其实该函数名起的不好
// 取名叫GenerateSecondLevelIteratorFromIndexIterator()更好hhh
// 其作用为由一级Iter（index_iter_）生成二级Iter（data_iter_）
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    // 如果index_iter无效，那么data_iter置空
    SetDataIterator(nullptr);
  } else {
    // index_iter_的value就是data block的句柄
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // 如果已经指向这个data block了，那就不用额外操作了
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      // 直接生成一个新的iter，赋值给data_iter_
      // block_function就是Table::BlockReader()
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
