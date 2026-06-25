/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <memory>
#include <vector>
#include "common/config.h"
#include "execution_defs.h"
#include "executor_abstract.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t len_;
    size_t left_tuple_len_;
    size_t join_buffer_capacity_;
    std::vector<ColMeta> cols_;
    std::vector<Condition> fed_conds_;

    std::vector<std::unique_ptr<RmRecord>> left_block_;
    size_t block_idx_;
    std::unique_ptr<RmRecord> record_;
    bool is_end_;

    std::unique_ptr<RmRecord> join_records(RmRecord *lrec, RmRecord *rrec) {
        auto rec = std::make_unique<RmRecord>(len_);
        memcpy(rec->data, lrec->data, left_tuple_len_);
        memcpy(rec->data + left_tuple_len_, rrec->data, right_->tupleLen());
        return rec;
    }

    void load_left_block() {
        left_block_.clear();
        size_t used = 0;
        while (!left_->is_end()) {
            if (!left_block_.empty() && used + left_tuple_len_ > join_buffer_capacity_) {
                break;
            }
            left_block_.push_back(left_->Next());
            used += left_tuple_len_;
            left_->nextTuple();
        }
    }

    bool try_match() {
        if (block_idx_ >= left_block_.size() || right_->is_end()) {
            return false;
        }
        auto right_rec = right_->Next();
        auto joined = join_records(left_block_[block_idx_].get(), right_rec.get());
        if (fed_conds_.empty() || eval_conds(fed_conds_, joined.get(), cols_)) {
            record_ = std::move(joined);
            is_end_ = false;
            return true;
        }
        right_->nextTuple();
        return false;
    }

    void find_next() {
        is_end_ = true;
        record_.reset();

        while (true) {
            if (left_block_.empty()) {
                load_left_block();
                if (left_block_.empty()) {
                    return;
                }
                block_idx_ = 0;
                right_->beginTuple();
            }

            while (block_idx_ < left_block_.size()) {
                while (!right_->is_end()) {
                    if (try_match()) {
                        return;
                    }
                }
                block_idx_++;
                right_->beginTuple();
            }

            left_block_.clear();
            block_idx_ = 0;
        }
    }

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        left_tuple_len_ = left_->tupleLen();
        len_ = left_tuple_len_ + right_->tupleLen();
        join_buffer_capacity_ = JOIN_BUFFER_SIZE;
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_tuple_len_;
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
        block_idx_ = 0;
        is_end_ = true;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return is_end_; }

    void beginTuple() override {
        left_->beginTuple();
        left_block_.clear();
        block_idx_ = 0;
        find_next();
    }

    void nextTuple() override {
        if (is_end_) {
            return;
        }
        right_->nextTuple();
        find_next();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end_) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*record_);
    }

    Rid &rid() override { return _abstract_rid; }
};
