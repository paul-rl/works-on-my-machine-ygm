// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

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
  
  oversized_bag(ygm::comm &comm) : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
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
  ~oversized_bag() { m_comm.barrier(); }

  /** @todo ----------------------------------------------unchanged---------------------------------------------- */
  oversized_bag(const self_type &other)  // If I remove const it compiles
      : m_comm(other.comm()), pthis(this), partitioner(other.comm()) {
    pthis.check(m_comm);
  }

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
   */
  oversized_bag &operator=(self_type &&other) noexcept {
    /* ---This is the original code---
    std::swap(m_local_bag, other.m_local_bag);
    */
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

  /** @todo */
  void local_insert(const Item &val) { /*---This is the original code--- m_local_bag.push_back(val); */ }

  /** @todo */
  void local_clear() { /*---This is the original code--- m_local_bag.clear(); */ }

  /** @todo */
  size_t local_size() const { /*---This is the original code--- return m_local_bag.size(); */ }

  /** @todo */
  size_t local_count(const value_type &val) const {
    /*---This is the original code--- 
    return std::count(m_local_bag.begin(), m_local_bag.end(), val);
    */
  }

  /** @todo */
  template <typename Function>
  void local_for_all(Function fn) {
    /*---This is the original code---
    std::for_each(m_local_bag.begin(), m_local_bag.end(), fn);
    */
  }

  /** @todo */
  template <typename Function>
  void local_for_all(Function fn) const {
    /*---This is the original code---
    std::for_each(m_local_bag.cbegin(), m_local_bag.cend(), fn);
    */
  }
  
  /** @todo */  
  void serialize(const std::string &fname) {
    /*---This is the original code---
    m_comm.barrier();
    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::JSONOutputArchive oarchive(os);
    oarchive(m_local_bag, m_comm.size());
    */
  }

  /** @todo */  
  void deserialize(const std::string &fname) {
    /*---This is the original code---
    m_comm.barrier();

    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ifstream is(rank_fname, std::ios::binary);

    cereal::JSONInputArchive iarchive(is);
    int                      comm_size;
    // iarchive(m_local_bag, m_round_robin, comm_size);
    iarchive(m_local_bag, comm_size);

    if (comm_size != m_comm.size()) {
      m_comm.cerr0(
          "Attempting to deserialize bag_impl using communicator of "
          "different size than serialized with");
    }
    */
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

 private:
   /** @todo */
  std::vector<value_type> local_pop(int n) {
    /*---This is the original code---
    YGM_ASSERT_RELEASE(n <= local_size());

    size_t                  new_size  = local_size() - n;
    auto                    pop_start = m_local_bag.begin() + new_size;
    std::vector<value_type> ret;
    ret.assign(pop_start, m_local_bag.end());
    m_local_bag.resize(new_size);
    return ret;
    */
  }

  /** @todo */
  void local_swap(self_type &other) { /* ---This is the original code--- m_local_bag.swap(other.m_local_bag); */ }

  ygm::comm                       &m_comm;
  /** @todo the default bag uses an std::vector as storage, we'll need a few things to do the oversized version. 
   * My initial thoughts will be the following:
   * 1) std::fstream object to write to a file
   * 2) size_t local_size to keep track of the number of collective items in the local rank's files
   * 3) size_t cur_size to keep track of the number of items in the current file
   * 4) size_t threshold to keep track of the number of items before we write to a new file
   * 5) base_filename to keep track of the file name
   * 6) base_directory to keep track of the directory
   * 7) size_t file_number to keep track of the number of files we've created
   * 
   * Might need more or less than the above, this is just initial thoughts/design
  
  ---This is the original code---
  std::vector<value_type>          m_local_bag;
  */
  typename ygm::ygm_ptr<self_type> pthis;
};

}  // namespace ygm::container
