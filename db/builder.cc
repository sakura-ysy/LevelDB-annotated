// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

// 通过im的iter构建SST
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  // iter初次使用前都要先Seek一下
  iter->SeekToFirst();

  // 根据编号生成SST文件名
  std::string fname = TableFileName(dbname, meta->number);

  // 开始通过iter遍历im中的k-v
  if (iter->Valid()) {
    // SST文件
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    // SST的builder
    TableBuilder* builder = new TableBuilder(options, file);
    // 赋值最小的key
    // 因为memtale的iter是有序的（skiplist）
    // 因此iter的第一个key一定是最小的
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    // 将iter中的所有key都Add进builder中
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    // SST构建完成
    // 注意，Finish()包含落盘
    s = builder->Finish();
    // 此时，SST已经在磁盘中了
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    // SST已经生成并落盘
    // builder没用了，释放掉
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    // 释放文件指针
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
