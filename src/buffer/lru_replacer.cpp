//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_size_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  if (frame_list_.empty()) return false;
  *frame_id = frame_list_.back();
  frame_map_.erase(*frame_id);
  frame_list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  if (frame_map_.count(frame_id)) {
    frame_list_.erase(frame_map_[frame_id]);
    frame_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  if (frame_map_.count(frame_id) || frame_list_.size() > max_size_) return;
  frame_list_.push_front(frame_id);
  frame_map_[frame_id] = frame_list_.begin();
}

auto LRUReplacer::Size() -> size_t {
  std::scoped_lock scoped_lru_replacer_latch(lru_replacer_latch_);
  return frame_list_.size();
}

}  // namespace bustub
