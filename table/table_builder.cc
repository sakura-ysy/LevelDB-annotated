// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {


struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        // 根据策略生成filter
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        // 留个疑问，为什么是false
        // 看完Add()就明白了
        pending_index_entry(false) {
    // 一个restart中有多少个key
    // 初始化时设置为1
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  // SST写文件指针
  WritableFile* file;
  // 当前写的offset，即写到哪了
  // 注意，这个offset初始值为0，也就是说
  // 它假设这个文件一开始的内容就是空
  uint64_t offset;
  // 写的状态，之前的写入是否出错了
  Status status;
  // data block
  BlockBuilder data_block;
  // data blockc index
  // 结构如下
  // |  key  |  BlockHandle   |
  // |  key  |  BlockHandle   |
  // |        offset1         |
  // |        offset2         |
  // |     number_of_items    |
  // |  compress type (1byte) | 
  // |      crc32 (4byte)     | 
  // 注意，key是用来分隔两个data block的，而不是指该key位于该data block中
  // <=key，说明在该BlockHandle中
  // >key，说明在下一个BlockHandle中
  // 具体可以看Add()和WriteRawBlock()相关注释
  BlockBuilder index_block;
  // 记录最后添加的key
  // 每个新来的key要进来时，都要与该key比较，进而
  // 保证整体的顺序是有序的
  // 注意：
  // 该字段是副本而不是指针，为什么只能是副本？
  // 在data index block块中添加item时可以得到解答，具体可以看Add()的相关注释
  std::string last_key;
  // SST的条目总数
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  // filter block
  // 因为filter block只有一个（meta block）
  // 构建完之后接着就是meta index block
  // 因此不需要为meta index block准备专门的字段
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  // 字面意思，entry(k-v)挂起，说明没有data block给它
  // 故这个字段为true就说明该为一个新的data block了
  // 然后为其在data index block中生成相应的item
  // 即 key | BlockHandle
  // 其中BlockHandle是`旧`的data block的指针
  // 即pending_handle
  // key是分隔新旧data block的分隔符
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    // 一开始传入0，说明目前还不会生成filter block
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  // rep_和rep_->filter_block都是new出来的
  // 因此需要手动释放
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

// 大致步骤：
// 1. 判断key是否递增，否则出错
// 2. 判断是否需要创新的data block，如果是，在data index block块中为其添加item
// 3. key加入filter block
// 4. key加入data block
// 5. 如果data_block超过设定大小了，就将其刷写到文件中
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // sst内部是有序的，因为新加的key一定是递增的
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 意思是说，当需要生成一个新的data block时
  // 这里为其在data index block块中生成相应的item
  // 即 key | BlockHandle
  if (r->pending_index_entry) {
    // pending_index_entry为ture，则data_block一定为空
    // 说明是新的data_block
    // 注意，这个空可不是指对象为空
    // 而是指data_block中没有entry(k-v)
    // 所以后续依旧可以对其调用data_block.AddKey()
    assert(r->data_block.empty());
    // 将r->last_key改为[r->last_key, key)之间的某一个key
    // 用来作为新旧data_block的分隔
    // 这就是解释了last_key为什么是副本而不是指针
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    // 分配一块内存放置BlockHandle的编码
    std::string handle_encoding;
    // pending_handle的值在WriteRawBlock()中设置
    // 它指向最后一个刷写的data block
    // 也就是上一个data block
    r->pending_handle.EncodeTo(&handle_encoding);
    // data index block中加入item
    // key | BlockHandle
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}
// 看完Add()之后来想想为什么初始化时pending_index_entry必须为false
// 假设为true，那么data index block中就会加入item：
// | key1 | b1 |
// 意为：<=key1则在b1中，>key1则在下一个b中
// 显然这是不可能的




// 将data_block刷写进文件
// 注意，只是刷data_block
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;

  if (r->data_block.empty()) return;
  // 刷写时不能存在pending_index_entry
  assert(!r->pending_index_entry);
  // 刷写data_block
  // pending_handle将保存该block的位置，即它的指针
  // WriteBlock并没有落盘，只是把相关数据放入r->file字段中
  // 仍在内存里
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    // WriteBlock()完成后会data_block.Reset()
    // 即开始生成新的data_block
    // 故要给pending_index_entry设为true
    r->pending_index_entry = true;
    // 真正的刷盘
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    // 视情况判断filter_block是否需要生成新filter了
    // r->offset指sst文件已经写的偏移
    // 因为sst前一部分全是data block
    // 所以r->offset就是data block的总大小
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 获取block的原始内存内容
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 写内存
  WriteRawBlock(block_contents, type, handle);
  // 清除压缩后的内存
  r->compressed_output.clear();
  // 重置block
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  // 把内容Append到r->file后面
  r->status = r->file->Append(block_contents);

  if (r->status.ok()) {
    // 开始写block尾部的内容
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 更新r->offset
      r->offset += block_contents.size() + kBlockTrailerSize;
      // 此时r->offset应是下一个block的首址
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

// sst构建完成，生成最终的文件
// 前面一直在写data block
// 这里开始把meta block、meta index block、data index block、footer写下去
Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 把还没有刷写的data block刷写下去
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
    // 此时filter_block_handle就指向filter_block在sst中的首址
  }

  // Write metaindex block
  if (ok()) {
    // 以k-v的方式构架meta index block
    // key是filter策略的名字
    // value是filter block的指针（BlockHandle）
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      // filter block的指针
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    // 写入meta index block
    // metaindex_block_handle记录其位置
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    // 写入data index block
    // index_block_handle记录其位置
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 写入footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

// 放弃构建
// 只是简单的置位r->closed
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
