//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock scoped_buffer_pool_manager_latch(buffer_pool_manager_latch_);
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) == page_table_.end()) return false;
  if (pages_[page_table_[page_id]].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
    pages_[page_table_[page_id]].is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock scoped_buffer_pool_manager_latch(buffer_pool_manager_latch_);
  // You can do it!
  for (auto &it : page_table_)
    if (pages_[it.second].is_dirty_) {
      disk_manager_->WritePage(it.first, pages_[it.second].data_);
      pages_[it.second].is_dirty_ = false;
    }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock scoped_buffer_pool_manager_latch(buffer_pool_manager_latch_);
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always
  // pick from the free list first.
  frame_id_t victim_page = INVALID_PAGE_ID;
  if (!free_list_.empty()) {
    victim_page = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&victim_page)) {
    assert(victim_page != INVALID_PAGE_ID);
    if (pages_[victim_page].is_dirty_)
      disk_manager_->WritePage(pages_[victim_page].page_id_, pages_[victim_page].data_);
    page_table_.erase(pages_[victim_page].page_id_);
  } else
    return nullptr;
  assert(victim_page != INVALID_PAGE_ID);
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  *page_id = AllocatePage();
  page_table_[*page_id] = victim_page;

  pages_[victim_page].ResetMemory();
  pages_[victim_page].page_id_ = *page_id;
  pages_[victim_page].is_dirty_ = false;
  pages_[victim_page].pin_count_ = 1;

  replacer_->Pin(victim_page);

  // 4.   Set the page ID output parameter. Return a pointer to P.
  return &pages_[victim_page];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock scoped_buffer_pool_manager_latch(buffer_pool_manager_latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the
  // free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (page_id == INVALID_PAGE_ID) return nullptr;
  frame_id_t replacement_page;
  if (page_table_.count(page_id)) {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
    return &pages_[page_table_[page_id]];
  } else if (!free_list_.empty()) {
    replacement_page = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&replacement_page))
    return nullptr;
  // 2.     If R is dirty, write it back to the disk.
  if (pages_[replacement_page].is_dirty_)
    disk_manager_->WritePage(pages_[replacement_page].page_id_, pages_[replacement_page].data_);
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(pages_[replacement_page].page_id_);
  page_table_[page_id] = replacement_page;
  // 4.     Update P's metadata, read in the page content from disk, and
  // then return a pointer to P.
  pages_[replacement_page].ResetMemory();
  pages_[replacement_page].page_id_ = page_id;
  pages_[replacement_page].is_dirty_ = false;
  pages_[replacement_page].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[replacement_page].data_);
  replacer_->Pin(replacement_page);
  return &pages_[replacement_page];
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock scoped_buffer_pool_manager_latch(buffer_pool_manager_latch_);
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end())
    return true;
  else if (pages_[page_table_[page_id]].pin_count_)
    return false;
  else {
    pages_[page_table_[page_id]].ResetMemory();
    pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
    pages_[page_table_[page_id]].is_dirty_ = false;
    pages_[page_table_[page_id]].pin_count_ = 1;

    free_list_.emplace_back(page_table_[page_id]);
    page_table_.erase(page_id);
    DeallocatePage(page_id);
    return true;
  }
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock scoped_buffer_pool_manager_latch(buffer_pool_manager_latch_);
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) return false;
  if (pages_[page_table_[page_id]].pin_count_ <= 0) return false;
  if (--pages_[page_table_[page_id]].pin_count_ == 0) replacer_->Unpin(page_table_[page_id]);
  if (is_dirty) pages_[page_table_[page_id]].is_dirty_ = true;
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
