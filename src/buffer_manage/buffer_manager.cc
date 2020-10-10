#include "imlab/buffer_manage/buffer_manager.h"
#include <unordered_map>
#include <system_error>  // NOLINT
#include <string_view>  // NOLINT
#include <array>
#include <string>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <thread>  // NOLINT
#include <sstream>

//#define DEBUG 1
namespace imlab {

/**
 * Returns a pointer to this page's data.
 * @return data in char*
 */
char* BufferFrame::get_data() {
    return data;
}

/**
 * Open the file associated with the BufferFrame, create if necessary.
 * Allocate enough memory and filesize
 * (and read the data from the file into an internal buffer) <- not here
 * @param pageId the page ID of the frame
 * @param size how much space is needed
 * @throws runtime_error, if the file can't be opened, created or stated
 */
BufferFrame::BufferFrame(
        const ub8 pageId, char* data, list_position  fifo_position, list_position lru_position)
         : pid(pageId), data(data), fifo_position(fifo_position), lru_position(lru_position) {}

/**
 * Lock the shared_mutex
 * @param exclusive: if it should be exclusively locked
 */
void BufferFrame::lock(const bool exclusive_lock) {
    if (exclusive_lock) {
        shared_mutex.lock();
        this->exclusively_locked = true;
    } else {
        shared_mutex.lock_shared();
    }
}

/**
 * Unlock the shared_mutex
 */
void BufferFrame::unlock() {
    if (this->exclusively_locked) {
        this->exclusively_locked = false;
        shared_mutex.unlock();
    } else {
        shared_mutex.unlock_shared();
    }
}

/**
  * Set this BufferFrame state as Dirty
  */
void BufferFrame::set_dirty() {
    this->is_dirty = true;
}

/**
 * Increase the number of user of this Buffer Frame
 */
size_t BufferFrame::get_num_users() {
    return num_users;
}

/**
 * Decrease the number of user of this Buffer Frame
 */
void BufferFrame::inc_num_users() {
    num_users++;
}

/**
 * Get the number of user of this Buffer Frame
 * @return the number of user of this Buffer Frame
 */
void BufferFrame::dec_num_users() {
    num_users--;
}

/// Constructor.
/// @param[in] page_count Maximum number of pages that should reside in
///                       memory at the same time.
BufferManager::BufferManager(size_t page_count, unique_ptr_aligned<char[]> loaded_pages) : page_count(page_count), loaded_pages(std::move(loaded_pages)) {
  /// Check Data Alignment
  assert((uintptr_t)reinterpret_cast<void*>(loaded_pages.get()) % 32 == 0);
}


/// Constructor.
/// @param[in] page_size  Size in bytes that all pages will have.
/// @param[in] page_count Maximum number of pages that should reside in
///                       memory at the same time.
BufferManager::BufferManager(size_t page_size, size_t page_count, unique_ptr_aligned<char[]> loaded_pages)
: page_size(page_size), page_count(page_count), loaded_pages(std::move(loaded_pages)) {
  /// Check Data Alignment
  assert((uintptr_t)reinterpret_cast<void*>(loaded_pages.get()) % 32 == 0);
}

/// Destructor. Writes all dirty pages to disk.
BufferManager::~BufferManager() {
    unique_lock u_lock(global_mutex);  // can not be acquired by other thread! no shared mutex

    // write dirty pages back to file
    for (auto& bufferframe : bufferframes) {
        write_out_page(bufferframe.second, u_lock);
    }
}

/// Returns a reference to a `BufferFrame` object for a given page id. When
/// the page is not loaded into memory, it is read from disk. Otherwise the
/// loaded page is used.
/// When the page cannot be loaded because the buffer is full, throws the
/// exception `buffer_full_error`.
/// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
/// `unfix_page()`.
/// @param[in] page_id   Page id of the page that should be loaded.
/// @param[in] exclusive If `exclusive` is true, the page is locked
///                      exclusively. Otherwise it is locked
///                      non-exclusively (shared).
BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
#ifdef DEBUG
    std::stringstream stream;
    stream << "BufferManager.fix_page: " << page_id << "  threadID:" << std::this_thread::get_id();
    std::cout << stream.str();
#endif
    /// First: acquire the global latch
    /// so other threads can not modify the Hash Table when we are looking for an entry
    unique_lock u_lock(global_mutex);  /// can not be acquired by other thread! no shared mutex

    while (true) {
        if (auto it = bufferframes.find(page_id); it != bufferframes.end()) {
#ifdef DEBUG
            std::cout << " IN RAM " << std::endl;
#endif
            /// Page is buffered / loaded in RAM
            auto& page = it->second;
            page.inc_num_users();
            if (page.state == BufferFrame::EVICTING) {
#ifdef DEBUG
            std::cout << " BufferFrame::EVICTING " << std::endl;
#endif
                /// Page is being evicted by other thread
                /// BUT this thread want to (re)use this Page
                /// (Re)set the state to RELOADED
                page.state = BufferFrame::RELOADED;
            } else if (page.state == BufferFrame::NEW) {
#ifdef DEBUG
                std::cout << " BufferFrame::NEW " << std::endl;
#endif
                /// Another thread is trying to evict another page for this
                /// Wait for the other thread to finish by locking the page exclusively.
                u_lock.unlock();
                page.lock(true);
                page.unlock();
                u_lock.lock();
                if (page.state == BufferFrame::NEW) {
                    /// Other thread failed to evict another page
                    page.dec_num_users();
                    if (page.get_num_users() == 0) {
                        /// Remove the failed page
                        assert(page.fifo_position == fifo_list.end() && page.lru_position == lru_list.end());
                        bufferframes.erase(it);
                    }
                    continue;
                }
            }
            if (page.lru_position != lru_list.end()) {
                /// Page is in LRU List => Update it to the end of LRU List
                lru_list.erase(page.lru_position);
                page.lru_position = lru_list.insert(lru_list.end(), &page);
            } else {
                assert(page.fifo_position != fifo_list.end());
                /// Page is in the FIFO List and being fixed again => Hot Page => move it the the LRU List
                fifo_list.erase(page.fifo_position);
                page.fifo_position = fifo_list.end();
                page.lru_position = lru_list.insert(lru_list.end(), &page);
            }
            u_lock.unlock();
            page.lock(exclusive);
            return page;
        } else {
            break;
        }
    }
#ifdef DEBUG
        std::cout << " NEW PAGE ";
#endif
    /// Create a new page and don't insert it in the queues, yet.
    assert(bufferframes.find(page_id) == bufferframes.end());
    auto& page = bufferframes.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(page_id),
            std::forward_as_tuple(page_id, nullptr, fifo_list.end(), lru_list.end())).first->second;
    page.inc_num_users();
    page.lock(true);
    char* data;
    if (bufferframes.size() - 1 < page_count) {
#ifdef DEBUG
        std::cout << " STILL IN AVAILABLE IN RAM  " << std::endl;
#endif
        /// We still have space in the RAM => so load this new page
        data = &loaded_pages[(bufferframes.size() - 1) * page_size];
    } else {
        data = evict_page(u_lock);
#ifdef DEBUG
        std::cout << " EVICT = NO SPACE IN RAM  " << std::endl;
#endif
        if (data == nullptr) {
            /// No page could be evicted => throw a buffer_full_error
            page.dec_num_users();
            page.unlock();
            if (page.get_num_users() == 0) {
                assert(page.fifo_position == fifo_list.end() && page.lru_position == lru_list.end());
                bufferframes.erase(page_id);
            }
            throw buffer_full_error();
        }
    }
    page.state = BufferFrame::LOADING;
    page.data = data;
    page.fifo_position = fifo_list.insert(fifo_list.end(), &page);
    load_page(page, u_lock);
    page.unlock();
    u_lock.unlock();
    page.lock(exclusive);
    return page;
}

/// Takes a `BufferFrame` reference that was returned by an earlier call to
/// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
/// written back to disk eventually.
void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
#ifdef DEBUG
        std::stringstream stream;
        stream << "BufferManager.unfix_page: " << page.pid << "  threadID:" << std::this_thread::get_id() << std::endl;
        std::cout << stream.str();
#endif
    page.unlock();
    /// First lock global and frame
    unique_lock u_lock(global_mutex);  /// can not be acquired by other thread! no shared mutex

    if (is_dirty) {
        page.set_dirty();
    }
    page.dec_num_users();
}

/// Returns the page ids of all pages (fixed and unfixed) that are in the
/// FIFO list in FIFO order.
/// Is not thread-safe.
std::vector<uint64_t> BufferManager::get_fifo_list() const {
#ifdef DEBUG
    std::cout << "BufferManager::get_fifo_list() ";
    for (const auto& fifo : fifo_list) {
        std::cout << fifo->pid << ", ";
    }
    std::cout << std::endl;
#endif
    vector<ub8> v;
    v.reserve(fifo_list.size());
    for (const auto& fifo : fifo_list) {
        v.push_back(fifo->pid);
    }
    return v;
}

/// Returns the page ids of all pages (fixed and unfixed) that are in the
/// LRU list in LRU order.
/// Is not thread-safe.
std::vector<uint64_t> BufferManager::get_lru_list() const {
#ifdef DEBUG
    std::cout << "BufferManager::get_lru_list() ";
    for (const auto& lru : lru_list) {
        std::cout << lru->pid << ", ";
    }
    std::cout << std::endl;
#endif
    vector<ub8> v;
    v.reserve(lru_list.size());
    for (const auto& lru : lru_list) {
        v.push_back(lru->pid);
    }
    return v;
}

/**
 * Load the Page from Disk
 * @param page load this page
 * @param latch the locked global latch / directory latch => should be unlocked while doing I/O
 */
void BufferManager::load_page(BufferFrame& page, unique_lock<mutex>& latch) {
    assert(page.state == BufferFrame::LOADING);
    auto segment_id = get_segment_id(page.pid);
    auto segment_page_id = get_segment_page_id(page.pid);
    SegmentFile* segment_file;
    if (auto it = segment_files.find(segment_id); it != segment_files.end()) {
        /// File is opened already
        segment_file = &it->second;
    } else {
        auto filename = to_string(segment_id);
        /// Open file in WRITE Mode
        /// Because we have to write dirty pages to it
        segment_file = &segment_files.emplace(segment_id, File::open_file(filename.c_str(), File::WRITE)).first->second;
    }
    {
        unique_lock file_latch{segment_file->file_latch};
        auto& file = *segment_file->file;
        if (file.size() < (segment_page_id + 1) * page_size) {
            /// When the file is too small, resize it and zero out the data for it.
            /// As the bytes in the file are zeroed anyway. we don't have to read the zeroes from Disk
            file.resize((segment_page_id + 1) * page_size);
            file_latch.unlock();
            std::memset(page.data, 0, page_size);
        } else {
            file_latch.unlock();
            latch.unlock();
            file.read_block(segment_page_id * page_size, page_size, page.data);
            latch.lock();
        }
    }
    page.state = BufferFrame::LOADED;
    page.is_dirty = false;
}

/**
 * Write out the Page to Disk
 * @param page write out this page
 * @param latch the locked global latch / directory latch => should be unlocked while doing I/O
 */
void BufferManager::write_out_page(BufferFrame& page, unique_lock<mutex>& latch) {
    auto segment_id = get_segment_id(page.pid);
    auto segment_page_id = get_segment_page_id(page.pid);
    auto& file = *segment_files.find(segment_id)->second.file;
    latch.unlock();
    file.write_block(page.data, segment_page_id * page_size, page_size);
    latch.lock();
    page.is_dirty = false;
}

/**
 * Caller must hold the global latch / directory latch
 * @return the next page that can be evicted. When no page can be evicted, return nullptr
 */
BufferFrame* BufferManager::find_page_to_evict() {
    /// Try FIFO List first
    for (auto* page : fifo_list) {
        if (page->get_num_users() == 0 && page->state == BufferFrame::LOADED) {
            return page;
        }
    }
    /// If FIFO list is empty or all pages in FIFO List are fixed(used by at least Thread), try to evcit in LRU List
    for (auto* page : lru_list) {
        if (page->get_num_users() == 0 && page->state == BufferFrame::LOADED) {
            return page;
        }
    }
    return nullptr;
}

/**
 * Evicts a page from the buffer
 * @param latch must be the locked directory latch
 * @return the data pointer to the evicted page. When no page can be evicted, return nullptr
 */
char* BufferManager::evict_page(unique_lock<mutex>& latch) {
    BufferFrame* page_to_evict;
    while (true) {
        /// Need to evict another page. If no page can be evict, find_page_to_evict() returns nullptr
        page_to_evict = find_page_to_evict();
        if (page_to_evict == nullptr) {
            return nullptr;
        }
        assert(page_to_evict->state == BufferFrame::LOADED);
        page_to_evict->state = BufferFrame::EVICTING;
        if (!page_to_evict->is_dirty) {
            break;
        }
        /// Create a copy pf the page that is written to the file so that other threads can continue using it while it is being written
        {
            auto page_data = make_unique<char[]>(page_size);
            std::memcpy(page_data.get(), page_to_evict->data, page_size);
            BufferFrame page_copy{page_to_evict->pid, page_data.get(), fifo_list.end(), lru_list.end()};
            write_out_page(page_copy, latch);
        }
        assert(page_to_evict->state == BufferFrame::EVICTING || page_to_evict->state == BufferFrame::RELOADED);
        if (page_to_evict->state == BufferFrame::EVICTING) {
            /// Nobody claimed the page while we were evicting it
            /// Otherwise we have to retry
            break;
        }
        page_to_evict->state = BufferFrame::LOADED;
    }
    if (page_to_evict->lru_position != lru_list.end()) {
        lru_list.erase(page_to_evict->lru_position);
    } else {
        assert(page_to_evict->fifo_position != fifo_list.end());
        fifo_list.erase(page_to_evict->fifo_position);
    }
    char* data = page_to_evict->data;
    bufferframes.erase(page_to_evict->pid);
    return data;
}

/// Get a copy of needed pages in sequence
/// from the first page_numbers page
/// Done by malloc(), remember to free it!!!
/// @param page_numbers need page numbers
/// @return the needed copy data in pointer
char* BufferManager::get_first_pages_data(size_t page_numbers) {
  /// malloc needed size in memory
  /// we need the first page_number pages: so page_number * page_size bytes
  char* db_copy = reinterpret_cast<char *>(malloc(page_numbers << PAGE_SIZE_LOG2));

  /// offset for copy location
  size_t offset = 0;

  /// copy the first page_numbers pages
  for (size_t i = 0; i < page_numbers; ++i) {
    auto& bufferframe = fix_page(i, false);

    std::memcpy(db_copy + offset, bufferframe.get_data(), PAGE_SIZE);

    offset += PAGE_SIZE;

    unfix_page(bufferframe, false);
  }
  return db_copy;
}
}  // namespace imlab
