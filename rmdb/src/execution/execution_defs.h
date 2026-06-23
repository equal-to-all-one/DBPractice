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

#include "common/common.h"
#include "defs.h"
#include "errors.h"
#include "index/ix_index_handle.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

inline const ColMeta *find_col(const std::vector<ColMeta> &cols, const TabCol &target) {
    for (auto &col : cols) {
        if (col.tab_name == target.tab_name && col.name == target.col_name) {
            return &col;
        }
    }
    throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
}

inline bool eval_cond(const Condition &cond, const RmRecord *rec, const std::vector<ColMeta> &cols) {
    const ColMeta *lhs_col = find_col(cols, cond.lhs_col);
    const char *lhs_data = rec->data + lhs_col->offset;
    const char *rhs_data;
    if (cond.is_rhs_val) {
        rhs_data = cond.rhs_val.raw->data;
    } else {
        const ColMeta *rhs_col = find_col(cols, cond.rhs_col);
        rhs_data = rec->data + rhs_col->offset;
    }
    int cmp = ix_compare(lhs_data, rhs_data, lhs_col->type, lhs_col->len);
    switch (cond.op) {
        case OP_EQ: return cmp == 0;
        case OP_NE: return cmp != 0;
        case OP_LT: return cmp < 0;
        case OP_GT: return cmp > 0;
        case OP_LE: return cmp <= 0;
        case OP_GE: return cmp >= 0;
        default: throw InternalError("Unexpected comparison operator");
    }
}

inline bool eval_conds(const std::vector<Condition> &conds, const RmRecord *rec, const std::vector<ColMeta> &cols) {
    for (auto &cond : conds) {
        if (!eval_cond(cond, rec, cols)) {
            return false;
        }
    }
    return true;
}
