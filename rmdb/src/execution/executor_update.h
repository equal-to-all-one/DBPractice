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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    size_t idx_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
        idx_ = 0;
    }

    std::unique_ptr<RmRecord> Next() override {
        while (idx_ < rids_.size()) {
            Rid rid = rids_[idx_++];
            auto rec = fh_->get_record(rid, context_);
            std::vector<std::unique_ptr<char[]>> old_keys;
            old_keys.reserve(tab_.indexes.size());
            for (auto &index : tab_.indexes) {
                auto key = std::make_unique<char[]>(index.col_tot_len);
                int offset = 0;
                for (int j = 0; j < index.col_num; ++j) {
                    memcpy(key.get() + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                old_keys.push_back(std::move(key));
            }
            for (auto &set : set_clauses_) {
                auto col = tab_.get_col(set.lhs.col_name);
                memcpy(rec->data + col->offset, set.rhs.raw->data, col->len);
            }
            fh_->update_record(rid, rec->data, context_);
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_
                              .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                              .get();
                ih->delete_entry(old_keys[i].get(), context_->txn_);
                auto new_key = std::make_unique<char[]>(index.col_tot_len);
                int offset = 0;
                for (int j = 0; j < index.col_num; ++j) {
                    memcpy(new_key.get() + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->insert_entry(new_key.get(), rid, context_->txn_);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
