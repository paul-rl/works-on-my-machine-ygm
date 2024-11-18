// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <fstream>
#include <filesystem>
#include <cereal/archives/binary.hpp>
#include <cereal/archives/json.hpp>
#include <initializer_list>
#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/base_async_insert.hpp>
#include <ygm/container/detail/base_count.hpp>
#include <ygm/container/detail/base_iteration.hpp>
#include <ygm/container/detail/base_misc.hpp>
#include <ygm/container/detail/round_robin_partitioner.hpp>
#include <ygm/random.hpp>

namespace ygm::container {

/** @todo ----------------------------------------------unchanged---------------------------------------------- */
template <typename Item>
class oversized_bag : public detail::base_async_insert_value<oversized_bag<Item>, std::tuple<Item>>,
            public detail::base_count<oversized_bag<Item>, std::tuple<Item>>,
            public detail::base_misc<oversized_bag<Item>, std::tuple<Item>>,
            public detail::base_iteration_value<oversized_bag<Item>, std::tuple<Item>> {
  friend class detail::base_misc<oversized_bag<Item>, std::tuple<Item>>;

 public:
  using self_type      = oversized_bag<Item>;
  using value_type     = Item;
  using size_type      = size_t;
  using for_all_args   = std::tuple<Item>;
  using container_type = ygm::container::bag_tag;


  class ygm_file {
   public:
    static m_threshold = 1000000; //set to 1 million for now, we should let this be configurable

    std::fstream                    file_io;
    size_t id()   { return m_id; }
    size_t size() { return m_size; }
    bool isactive() { return m_active; }

    ygm_file(std::string filename, size_t file_id) : 
              file_io(filename, std::ios::binary), out(file_io), in(file_io), 
              file_id(file_id), file_size(0), active(true) {}
    
    ~ygm_file() { if (file_io.is_open()) file_io.close(); }

    bool in(Item &data) {
      if (!active) return false;

      if (!file_io.eof) file_io.seekg(0, std::ios::end);
      
      m_in(data);
      size++;

      if (size > m_threshold)
        active = false;

      return true;
    }

    bool out(Item &data) {
      if (file_io.peek() == EOF) return false;
      else {
        m_out(data);
        return true;
      }
    }

    void vector_from_file(std::vector<Item>& storage) {
      file_io.seekg(0, std::ios::beg);
      while(file_io.peek() != EOF) {
        Item temp;
        if (file.out(temp)) {
          storage.push_back(temp);
        }
      }
    }

    std::vector<Item> vector_from_file() {
      std::vector<Item> ret;
      vector_from_file(ret);
      return ret;
    }

    void vector_to_file(std::vector<Item>& storage) {
      m_file_io.seekg(0, std::ios::beg);
      while(file_io.peek() != EOF) {
        Item temp;
        local_insert(temp);
      }
    }

    /** 
     * @brief This function refills the file based on changes made elsewhere. Since the data can change we need to
     * reset and rewrite the file.
     * 
     * @todo this section may or may not be needed, its a bit expensive but it applies the function on
     * all the items in the file. Becausee the function could modify the contents stored we need to re-write
     * the items back into the file. For the moment, I'm using filesystem::remove() to delete the file, there
     * may be a better way to do this perfomance wise. The worry from seeking back the the start of the file is
     * in hte case an item had a std::vector or other container as a member, the size of the vector could drastically
     * shrink, then when we write back to the file there is data left at the end. this might be fixed by adding an EOF
     * character to the end of the file, then additional writes occur it would overwrite the entirety of the stale data.
     */
    void reset_file(std::vector<Item>& storage) {
      auto cur_file = m_files.back();
      std::string fname = generate_filename(m_id);
      cur_file.back.file_io.close();
      std::filesystem::remove(fname);
      cur_file.file_io.open(fname, std::ios::binary);
      cur_file.size = 0;
      cur_file.in  = cereal::BinaryInputArchive(cur_file.file_io);
      cur_file.out = cereal::BinaryOutputArchive(cur_file.file_io);
      for (auto &item : storage) {
        cur_file.in(item);
      }
    }

   private:
    cereal::BinaryOutputArchive     m_out;
    cereal::BinaryInputArchive      m_in;
    size_t                          m_id;
    size_t                          m_size;
    bool                            m_active;
  };
  
  oversized_bag(ygm::comm &comm) : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
  }

  oversized_bag(ygm::comm &comm, size_t threshold) : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    set_threshold(threshold)
  }

  oversized_bag(ygm::comm &comm, std::initializer_list<Item> l)
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    if (m_comm.rank0()) {
      for (const Item &i : l) {
        async_insert(i);
      }
    }
    m_comm.barrier();
  }

  oversized_bag(ygm::comm &comm, std::initializer_list<Item> l, size_t threshold)
      : m_comm(comm), pthis(this), partitioner(comm) {
    set_threshold(threshold);
    pthis.check(m_comm);
    if (m_comm.rank0()) {
      for (const Item &i : l) {
        async_insert(i);
      }
    }
    m_comm.barrier();
  }
  /** @todo --------------------------------------------end unchanged-------------------------------------------- */

  /**
   * @brief Construct a new oversized bag object from a directory of files, arguments ect could be different
   */
  oversized_bag(ygm::comm &comm, std::string dir)
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    /**
     * @todo Implement this
     */
  }

  oversized_bag(ygm::comm &comm, std::string dir, size_t threshold)
      : m_comm(comm), pthis(this), partitioner(comm) {
    set_threshold(threshold);
    pthis.check(m_comm);
    /**
     * @todo Implement this
     */
  }

  /** @todo ----------------------------------------------unchanged---------------------------------------------- */
  template <typename STLContainer>
  oversized_bag(ygm::comm          &comm,
      const STLContainer &cont) requires detail::STLContainer<STLContainer> &&
      std::convertible_to<typename STLContainer::value_type, Item>
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);

    for (const Item &i : cont) {
      this->async_insert(i);
    }
    m_comm.barrier();
  }

  template <typename YGMContainer>
  oversized_bag(ygm::comm          &comm,
      const YGMContainer &yc) requires detail::HasForAll<YGMContainer> &&
      detail::SingleItemTuple<typename YGMContainer::for_all_args>
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);

    yc.for_all([this](const Item &value) { this->async_insert(value); });

    m_comm.barrier();
  }
  /** @todo --------------------------------------------end unchanged-------------------------------------------- */

  /**
   * @todo we'll need to close all of the files we opened
   */
  ~oversized_bag() { 
    m_comm.barrier(); 
    for (auto &file : m_files) {
      file.file_io.close();
    }
  }

  /** @todo ----------------------------------------------unchanged---------------------------------------------- */
  oversized_bag(const self_type &other)  // If I remove const it compiles
      : m_comm(other.comm()), pthis(this), partitioner(other.comm()) {
    pthis.check(m_comm);
  }

  /** @todo */
  oversized_bag(self_type &&other) noexcept
      : m_comm(other.comm()),
        pthis(this),
        partitioner(other.comm()),
        m_local_bag(std::move(other.m_local_bag)) {
    pthis.check(m_comm);
  }

  oversized_bag &operator=(const self_type &other) { return *this = oversized_bag(other); }
  /** @todo --------------------------------------------end unchanged-------------------------------------------- */

  /** @todo This might be hard to reimplement, we'll need to look at if we can swap on fstream object or if we'll have 
   * to swap data then have other.open() and this->open() to reopen the insert file
   * 
   * i think this would work? but im not entirely sure why we'd ever actually want to do a swap on an oversized bag
   * it's set up to be file-backed so generally i'd assume we'd just want a massive collection. 
   */
  oversized_bag &operator=(self_type &&other) noexcept {
    /* ---This is the original code---
    std::swap(m_local_bag, other.m_local_bag);
    */
    this->m_files.swap(other.m_files); 
    std::swap(this->m_local_size, other.m_local_size);
    std::swap(this->m_base_filename, other.m_base_filename);
    return *this;
  }

  using detail::base_async_insert_value<oversized_bag<Item>, for_all_args>::async_insert;

  void async_insert(const Item &value, int dest) {
    auto inserter = [](auto pcont, const value_type &item) {
      pcont->local_insert(item);
    };

    m_comm.async(dest, inserter, this->get_ygm_ptr(), value);
  }

  /** @todo ----------------------------------------------unchanged---------------------------------------------- */
  void async_insert(const std::vector<Item> &values, int dest) {
    auto inserter = [](auto pcont, const std::vector<Item> &values) {
      for (const auto &v : values) {
        pcont->local_insert(v);
      }
    };

    m_comm.async(dest, inserter, this->get_ygm_ptr(), values);
  }
  /** @todo --------------------------------------------end unchanged-------------------------------------------- */

  /** @todo testing needed*/
  void local_insert(const Item &val) {
    if (m_files.empty() || !m_files.back().isactive()) 
      open_new_file();

    m_files.back().in(val);
    m_local_size++;
  }

  /** @todo */
  void local_clear() {
    for (auto &file : m_files) {
      file.file_io.close();
      std::filesystem::remove(file.file_io);
    }
    m_files.clear();
    m_local_size = 0;
  }

  /** @todo testing needed */
  size_t local_size() const { m_local_size; }

  /** @todo testing needed */
  size_t local_count(const value_type &val) const {
    size_t count = 0;
    for (auto &file : m_files) {
      if (file.isactive()) {
        file.file_io.seekg(0, std::ios::beg);
        while(file.file_io.peek() != EOF) {
          Item temp;
          if (file.in(temp)) {
            if (temp == val) count++;
          }
        }
      }
    }
  }

  /** @todo testing needed */
  template <typename Function>
  void local_for_all(Function fn) {
   std::vector<Item> temp_storage;
    for (auto &file : m_files) {
      file.file_io.seekg(0, std::ios::beg);
      while(file.file_io.peek() != EOF) {
        Item temp;
        if (file.out(temp)) {
          fn(temp);
          temp_storage.push_back(temp);
        }
      }
      file.vector_from_file(temp_storage);
      reset_file(temp_storage);
      temp_storage.clear();
    }
  }

  /** @todo testing needed */
  template <typename Function>
  void local_for_all(Function fn) const {
    for (auto &file : m_files) {
      file.file_io.seekg(0, std::ios::beg);
      while(file.file_io.peek() != EOF) {
        Item temp;
        if (file.out(temp)) {
          fn(temp);
        }
      }
    }
  }
  
  /** @todo testing needed */  
  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::JSONOutputArchive oarchive(os);
    for (auto &file : m_files) {
      file.vector_from_file(temp_storage);
      /**
       * @todo I'm not sure if this is the correct way to serialize this as JSON. The issue will be we are serializing
       * potentially huge quantities of data. In the regular bag example everything can likely be stored in a single
       * file, for our cases though we might need to see if we can attatch not only the comm_size, but also the file.id
       * as it may not be possible to store the entirety of data in memory when we try and deserialize it.
       */
      oarchive(temp_storage, m_comm.size());
    }
  }

  /** @todo testing needed */  
  void deserialize(const std::string &fname) {
    m_comm.barrier();
    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ifstream is(rank_fname, std::ios::binary);
    /**
     * @todo We should look at if we can split things by file id instead of just the comm_size given.
     */
    cereal::JSONInputArchive iarchive(is);
    int                      comm_size;
    std::vector<Item> temp_storage;
    iarchive(temp_storage, comm_size);

    for (Item &item : temp_storage) {
      local_insert(item);
    }

    if (comm_size != m_comm.size()) {
      m_comm.cerr0(
          "Attempting to deserialize bag_impl using communicator of "
          "different size than serialized with");
    }
  }

  /** @todo */  
  void rebalance() {
    /*---This is the original code---
    auto global_size = this->size();  // includes barrier

    // Find current rank's prefix val and desired target size
    size_t prefix_val  = ygm::prefix_sum(local_size(), m_comm);
    size_t target_size = std::ceil((global_size * 1.0) / m_comm.size());

    // Init to_send array where index is dest and value is the num to send
    // int to_send[m_comm.size()] = {0};
    std::unordered_map<size_t, size_t> to_send;

    size_t small_block_size = global_size / m_comm.size();
    size_t large_block_size =
        global_size / m_comm.size() + ((global_size / m_comm.size()) > 0);

    for (size_t i = 0; i < local_size(); i++) {
      size_t idx = prefix_val + i;
      size_t target_rank;

      // Determine target rank to match partitioning in ygm::container::array
      if (idx < (global_size % m_comm.size()) * large_block_size) {
        target_rank = idx / large_block_size;
      } else {
        target_rank = (global_size % m_comm.size()) +
                      (idx - (global_size % m_comm.size()) * large_block_size) /
                          small_block_size;
      }

      if (target_rank != m_comm.rank()) {
        to_send[target_rank]++;
      }
    }
    m_comm.barrier();

    // Build and send oversized_bag indexes as calculated by to_send
    for (auto &kv_pair : to_send) {
      async_insert(local_pop(kv_pair.second), kv_pair.first);
    }

    m_comm.barrier();
    */
  }

  /** @todo */  
  template <typename RandomFunc>
  void local_shuffle(RandomFunc &r) {
    /*---This is the original code---
    m_comm.barrier();
    std::shuffle(m_local_bag.begin(), m_local_bag.end(), r);
    */
  }

  /** @todo */
  void local_shuffle() {
    /*---This is the original code---
    ygm::default_random_engine<> r(m_comm, std::random_device()());
    local_shuffle(r);
    */
  }

  /** @todo */
  template <typename RandomFunc>
  void global_shuffle(RandomFunc &r) {
    /*---This is the original code---
    m_comm.barrier();
    std::vector<value_type> old_local_bag;
    std::swap(old_local_bag, m_local_bag);

    auto send_item = [](auto oversized_bag, const value_type &item) {
      oversized_bag->m_local_bag.push_back(item);
    };

    std::uniform_int_distribution<> distrib(0, m_comm.size() - 1);
    for (value_type i : old_local_bag) {
      m_comm.async(distrib(r), send_item, pthis, i);
    }
    */
  }

  /** @todo */
  void global_shuffle() {
    /*---This is the original code---
    ygm::default_random_engine<> r(m_comm, std::random_device()());
    global_shuffle(r);
    */
  }

  detail::round_robin_partitioner partitioner;

  void set_threshold(size_t thresh) { ygm_file::m_threshold = thresh; }

 private:
   /** @todo testing needed */
  std::vector<value_type> local_pop(int n) {
    YGM_ASSERT_RELEASE(n <= local_size());
    std::vector<value_type> ret;
    std::vector<value_type> temp_storage;

    vector_from_file(m_files.end(), temp_storage);
    size_t new_size  = m_local_size - n;
    auto pop_start = temp_storage.begin() + new_size;

    ret.assign(pop_start, temp_storage.end());
    reset_file(temp_storage);
    vector_to_file(m_files.back(), temp_storage);
    m_local_size = new_size;
    return ret;
  }

  /** @todo testing needed*/
  void local_swap(self_type &other) { 
    this->m_files.swap(other.m_files); 
    std::swap(this->m_local_size, other.m_local_size);
    std::swap(this->m_base_filename, other.m_base_filename);
  }

  inline void open_new_file() {
    m_files.push_back(ygm_file(generate_filename(m_files.size()), m_files.size()));
  }

  inline std::string generate_filename(size_t id) const {
    return std::string(m_base_filename + std::string(this->m_comm.rank()) + "_" + std::to_string(id));
  }

  ygm::comm                        &m_comm;
  std::vector<ygm_file>             m_files; 
  size_t                            m_local_size;

  std::string                       m_base_filename;
  typename ygm::ygm_ptr<self_type>  pthis;
};

}  // namespace ygm::container
