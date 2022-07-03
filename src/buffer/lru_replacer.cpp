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

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (frame_list.empty()) return false;
  *frame_id = frame_list.back();
  frame_map.erase(*frame_id);
  frame_list.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (frame_map.count(frame_id)) {
    frame_list.erase(frame_map[frame_id]);
    frame_map.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frame_map.count(frame_id)) return;
  frame_list.push_front(frame_id);
  frame_map[frame_id] = frame_list.begin();
}

auto LRUReplacer::Size() -> size_t {
  return frame_list.size();
}

}  // namespace bustub
