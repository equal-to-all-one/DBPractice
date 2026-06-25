#pragma once

#include <algorithm>
#include <memory>
#include <vector>
#include "execution_defs.h"
#include "executor_abstract.h"
#include "index/ix_index_handle.h"

class SortExecutor : public AbstractExecutor {
   private:
    struct SortKeySpec {
        ColMeta col;
        bool is_desc;
    };

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::vector<SortKeySpec> sort_keys_;
    int limit_;
    std::vector<std::unique_ptr<RmRecord>> sorted_records_;
    size_t current_idx_;
    bool is_end_;

    static int compare_records(const RmRecord *a, const RmRecord *b, const std::vector<SortKeySpec> &keys) {
        for (auto &key : keys) {
            int cmp = ix_compare(a->data + key.col.offset, b->data + key.col.offset, key.col.type, key.col.len);
            if (cmp != 0) {
                return key.is_desc ? -cmp : cmp;
            }
        }
        return 0;
    }

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sort_cols,
                 const std::vector<bool> &is_desc, int limit) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        limit_ = limit;
        current_idx_ = 0;
        is_end_ = true;

        auto &prev_cols = prev_->cols();
        for (size_t i = 0; i < sort_cols.size(); ++i) {
            SortKeySpec spec;
            spec.col = *get_col(prev_cols, sort_cols[i]);
            spec.is_desc = is_desc[i];
            sort_keys_.push_back(spec);
        }
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return is_end_; }

    void beginTuple() override {
        sorted_records_.clear();
        prev_->beginTuple();
        while (!prev_->is_end()) {
            sorted_records_.push_back(prev_->Next());
            prev_->nextTuple();
        }

        std::sort(sorted_records_.begin(), sorted_records_.end(),
                  [this](const std::unique_ptr<RmRecord> &a, const std::unique_ptr<RmRecord> &b) {
                      return compare_records(a.get(), b.get(), sort_keys_) < 0;
                  });

        if (limit_ >= 0 && static_cast<int>(sorted_records_.size()) > limit_) {
            sorted_records_.resize(limit_);
        }

        current_idx_ = 0;
        is_end_ = sorted_records_.empty();
    }

    void nextTuple() override {
        if (is_end_) {
            return;
        }
        current_idx_++;
        if (current_idx_ >= sorted_records_.size()) {
            is_end_ = true;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end_) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*sorted_records_[current_idx_]);
    }

    Rid &rid() override { return _abstract_rid; }
};
