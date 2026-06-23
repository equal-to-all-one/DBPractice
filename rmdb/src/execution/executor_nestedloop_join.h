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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t len_;
    std::vector<ColMeta> cols_;
    std::vector<Condition> fed_conds_;
    std::unique_ptr<RmRecord> left_rec_;
    std::unique_ptr<RmRecord> record_;
    bool is_end_;

    std::unique_ptr<RmRecord> join_records(RmRecord *lrec, RmRecord *rrec) {
        auto rec = std::make_unique<RmRecord>(len_);
        memcpy(rec->data, lrec->data, left_->tupleLen());
        memcpy(rec->data + left_->tupleLen(), rrec->data, right_->tupleLen());
        return rec;
    }

    bool try_current() {
        if (right_->is_end()) {
            return false;
        }
        auto right_rec = right_->Next();
        auto joined = join_records(left_rec_.get(), right_rec.get());
        if (fed_conds_.empty() || eval_conds(fed_conds_, joined.get(), cols_)) {
            record_ = std::move(joined);
            is_end_ = false;
            return true;
        }
        return false;
    }

    void find_next() {
        is_end_ = true;
        record_.reset();
        while (!left_->is_end()) {
            while (!right_->is_end()) {
                if (try_current()) {
                    return;
                }
                right_->nextTuple();
            }
            left_->nextTuple();
            if (left_->is_end()) {
                return;
            }
            left_rec_ = left_->Next();
            right_->beginTuple();
        }
    }

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
        is_end_ = true;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return is_end_; }

    void beginTuple() override {
        left_->beginTuple();
        if (left_->is_end()) {
            is_end_ = true;
            return;
        }
        left_rec_ = left_->Next();
        right_->beginTuple();
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
