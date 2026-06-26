/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

void LockManager::update_group_lock_mode(LockRequestQueue &queue) {
    queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
    for (auto &req : queue.request_queue_) {
        if (!req.granted_) {
            continue;
        }
        switch (req.lock_mode_) {
            case LockMode::SHARED:
                if (queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
                    queue.group_lock_mode_ = GroupLockMode::S;
                } else if (queue.group_lock_mode_ == GroupLockMode::IS) {
                    queue.group_lock_mode_ = GroupLockMode::S;
                }
                break;
            case LockMode::EXLUCSIVE:
                queue.group_lock_mode_ = GroupLockMode::X;
                return;
            case LockMode::INTENTION_SHARED:
                if (queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
                    queue.group_lock_mode_ = GroupLockMode::IS;
                }
                break;
            case LockMode::INTENTION_EXCLUSIVE:
                if (queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
                    queue.group_lock_mode_ = GroupLockMode::IX;
                } else if (queue.group_lock_mode_ == GroupLockMode::IS) {
                    queue.group_lock_mode_ = GroupLockMode::IX;
                }
                break;
            case LockMode::S_IX:
                queue.group_lock_mode_ = GroupLockMode::SIX;
                return;
        }
    }
}

void LockManager::check_growing(Transaction *txn) {
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
}

bool LockManager::table_conflict(LockMode request, GroupLockMode held) {
    switch (request) {
        case LockMode::INTENTION_SHARED:
            return held == GroupLockMode::X;
        case LockMode::INTENTION_EXCLUSIVE:
            return held == GroupLockMode::S || held == GroupLockMode::X || held == GroupLockMode::SIX;
        case LockMode::SHARED:
            return held == GroupLockMode::IX || held == GroupLockMode::X || held == GroupLockMode::SIX;
        case LockMode::EXLUCSIVE:
            return held != GroupLockMode::NON_LOCK;
        case LockMode::S_IX:
            return held == GroupLockMode::IS || held == GroupLockMode::IX || held == GroupLockMode::S ||
                   held == GroupLockMode::X || held == GroupLockMode::SIX;
    }
    return true;
}

bool LockManager::record_conflict(LockMode request, LockMode held) {
    if (request == LockMode::SHARED) {
        return held == LockMode::EXLUCSIVE;
    }
    return held == LockMode::SHARED || held == LockMode::EXLUCSIVE;
}

bool LockManager::lock_on_table(Transaction *txn, int tab_fd, LockMode lock_mode) {
    check_growing(txn);
    std::lock_guard<std::mutex> guard(latch_);
    txn->set_state(TransactionState::GROWING);

    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_data_id];

    auto covers = [](LockMode held, LockMode request) {
        if (held == request || held == LockMode::EXLUCSIVE) {
            return true;
        }
        if (held == LockMode::S_IX) {
            return request == LockMode::SHARED || request == LockMode::INTENTION_SHARED ||
                   request == LockMode::INTENTION_EXCLUSIVE;
        }
        if (held == LockMode::SHARED) {
            return request == LockMode::INTENTION_SHARED;
        }
        if (held == LockMode::INTENTION_EXCLUSIVE) {
            return request == LockMode::INTENTION_SHARED;
        }
        return false;
    };

    auto combine = [](LockMode held, LockMode request) {
        if (held == LockMode::EXLUCSIVE || request == LockMode::EXLUCSIVE) {
            return LockMode::EXLUCSIVE;
        }
        if (held == LockMode::S_IX || request == LockMode::S_IX) {
            return LockMode::S_IX;
        }
        if ((held == LockMode::SHARED && request == LockMode::INTENTION_EXCLUSIVE) ||
            (held == LockMode::INTENTION_EXCLUSIVE && request == LockMode::SHARED)) {
            return LockMode::S_IX;
        }
        if (held == LockMode::SHARED || request == LockMode::SHARED) {
            return LockMode::SHARED;
        }
        if (held == LockMode::INTENTION_EXCLUSIVE || request == LockMode::INTENTION_EXCLUSIVE) {
            return LockMode::INTENTION_EXCLUSIVE;
        }
        return LockMode::INTENTION_SHARED;
    };

    auto single_group = [](LockMode mode) {
        switch (mode) {
            case LockMode::SHARED:
                return GroupLockMode::S;
            case LockMode::EXLUCSIVE:
                return GroupLockMode::X;
            case LockMode::INTENTION_SHARED:
                return GroupLockMode::IS;
            case LockMode::INTENTION_EXCLUSIVE:
                return GroupLockMode::IX;
            case LockMode::S_IX:
                return GroupLockMode::SIX;
        }
        return GroupLockMode::NON_LOCK;
    };

    auto own_req = queue.request_queue_.end();
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            own_req = it;
            if (it->granted_ && covers(it->lock_mode_, lock_mode)) {
                txn->get_lock_set()->insert(lock_data_id);
                return true;
            }
            break;
        }
    }

    LockMode target_mode = own_req != queue.request_queue_.end() && own_req->granted_
                               ? combine(own_req->lock_mode_, lock_mode)
                               : lock_mode;

    for (auto &req : queue.request_queue_) {
        if (!req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id() &&
            table_conflict(target_mode, single_group(req.lock_mode_))) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    bool found = false;
    for (auto &req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            req.lock_mode_ = target_mode;
            req.granted_ = true;
            found = true;
            break;
        }
    }
    if (!found) {
        queue.request_queue_.emplace_back(txn->get_transaction_id(), target_mode);
        queue.request_queue_.back().granted_ = true;
    }
    update_group_lock_mode(queue);
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

bool LockManager::lock_on_record(Transaction *txn, const Rid &rid, int tab_fd, LockMode lock_mode) {
    check_growing(txn);
    std::lock_guard<std::mutex> guard(latch_);
    txn->set_state(TransactionState::GROWING);

    LockDataId table_id(tab_fd, LockDataType::TABLE);
    auto &table_queue = lock_table_[table_id];
    if (table_queue.group_lock_mode_ == GroupLockMode::X) {
        for (auto &req : table_queue.request_queue_) {
            if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }
    if (lock_mode == LockMode::EXLUCSIVE &&
        (table_queue.group_lock_mode_ == GroupLockMode::S || table_queue.group_lock_mode_ == GroupLockMode::SIX)) {
        for (auto &req : table_queue.request_queue_) {
            if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto &queue = lock_table_[lock_data_id];

    for (auto &req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id() && req.granted_) {
            if (lock_mode == LockMode::EXLUCSIVE && req.lock_mode_ == LockMode::SHARED) {
                for (auto &other : queue.request_queue_) {
                    if (other.granted_ && other.txn_id_ != txn->get_transaction_id()) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                    }
                }
                req.lock_mode_ = LockMode::EXLUCSIVE;
                queue.group_lock_mode_ = GroupLockMode::X;
                txn->get_lock_set()->insert(lock_data_id);
                return true;
            }
            if (req.lock_mode_ == lock_mode) {
                txn->get_lock_set()->insert(lock_data_id);
                return true;
            }
        }
    }

    for (auto &req : queue.request_queue_) {
        if (!req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id() &&
            record_conflict(lock_mode, req.lock_mode_)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    bool found = false;
    for (auto &req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            req.lock_mode_ = lock_mode;
            req.granted_ = true;
            found = true;
            break;
        }
    }
    if (!found) {
        queue.request_queue_.emplace_back(txn->get_transaction_id(), lock_mode);
        queue.request_queue_.back().granted_ = true;
    }
    queue.group_lock_mode_ = (lock_mode == LockMode::SHARED) ? GroupLockMode::S : GroupLockMode::X;
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
    return lock_on_record(txn, rid, tab_fd, LockMode::SHARED);
}

bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
    return lock_on_record(txn, rid, tab_fd, LockMode::EXLUCSIVE);
}

bool LockManager::lock_shared_on_table(Transaction *txn, int tab_fd) {
    return lock_on_table(txn, tab_fd, LockMode::SHARED);
}

bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd) {
    return lock_on_table(txn, tab_fd, LockMode::EXLUCSIVE);
}

bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd) {
    return lock_on_table(txn, tab_fd, LockMode::INTENTION_SHARED);
}

bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd) {
    return lock_on_table(txn, tab_fd, LockMode::INTENTION_EXCLUSIVE);
}

bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id) {
    std::lock_guard<std::mutex> guard(latch_);
    txn->set_state(TransactionState::SHRINKING);

    auto iter = lock_table_.find(lock_data_id);
    if (iter == lock_table_.end()) {
        return false;
    }

    auto &queue = iter->second;
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            queue.request_queue_.erase(it);
            update_group_lock_mode(queue);
            txn->get_lock_set()->erase(lock_data_id);
            if (queue.request_queue_.empty()) {
                lock_table_.erase(iter);
            }
            return true;
        }
    }
    return false;
}
