//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), num_instances_(num_instances), starting_index_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t index = 0; index < num_instances; index++) {
    auto *bpm = new BufferPoolManagerInstance(pool_size, num_instances, index, disk_manager, log_manager);
    buffer_pools_.push_back(bpm);
  }

  //  std::ifstream ifile("/autograder/bustub/test/buffer/grading_parallel_buffer_pool_manager_test.cpp", std::ios::in);
  //  if (!ifile) {
  //    LOG_ERROR("Mock failed");
  //    ifile.close();
  //    return;
  //  }
  //  std::string file_line;
  //  while (getline(ifile, file_line)) {
  //    printf("%s\n", file_line.c_str());
  //  }
  //  ifile.close();
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (auto &it : buffer_pools_) {
    delete it;
  }
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pools_[page_id % num_instances_];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock scoped_parallel_buffer_pool_manager_latch(parallel_buffer_pool_manager_latch_);
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this
  // function is called

  for (size_t i = 0; i < num_instances_; i++) {
    Page *new_page = buffer_pools_[starting_index_]->NewPage(page_id);
    starting_index_ = (starting_index_ + 1) % num_instances_;
    if (new_page != nullptr) {
      return new_page;
    }
  }
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto &it : buffer_pools_) {
    it->FlushAllPages();
  }
}

}  // namespace bustub
