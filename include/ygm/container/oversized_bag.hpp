// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
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

#define DEFAULT_THRESHOLD 10000000

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

  struct file_base_info {
    size_t       file_threshold = 10000000;
    int          file_rank = 0;
    std::string  file_base_filename = "oversized_bag";
    std::string  file_base_dir = "";

    std::string to_string() const {
      return std::string("file_threshold: " + std::to_string(file_threshold) + "\n" +
                         "file_rank: " + std::to_string(file_rank) + "\n" +
                         "file_base_filename: " + file_base_filename + "\n" +
                         "file_base_dir: " + file_base_dir + "\n");
    }

    inline std::string generate_filename(size_t id) const {
      return std::string(std::string(file_base_dir + file_base_filename) + "_" + std::to_string(file_rank) + "_" + std::to_string(id) + ".binary_archive");
    }
  };
  
  class ygm_file {
   public:
    ygm_file(file_base_info& base_file, size_t file_id) : 
              m_id(file_id), m_size(0), m_active(true), m_file_info(base_file),
              m_file_io(std::fstream(m_file_info.generate_filename(m_id), std::ios::in | std::ios::out | std::ios::app | std::ios::binary)) {
      check_file_errors();
    }
    ygm_file(file_base_info& base_file, size_t file_id, size_t file_size) : 
              m_id(file_id), m_size(0), m_active(true), m_file_info(base_file),
              m_file_io(std::fstream(m_file_info.generate_filename(m_id), std::ios::in | std::ios::out | std::ios::app | std::ios::binary)) {
      check_file_errors();
    }
    
    ~ygm_file() { if (m_file_io.is_open()) m_file_io.close(); }

    ygm_file(const ygm_file &other)  // If I remove const it compiles
        : m_id(other.m_id), m_size(other.m_size), m_active(other.m_active), m_file_info(other.m_file_info) {  
      if(m_active) {
        m_file_io.open(m_file_info.generate_filename(m_id), std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
        check_file_errors();
      }
    }

    /** @todo */
    ygm_file(ygm_file &&other) noexcept
        : m_id(other.m_id),
          m_size(other.m_size),
          m_active(other.m_active),
          m_file_info(other.m_file_info) {
      if(m_active) {
        m_file_io = std::fstream(m_file_info.generate_filename(m_id), std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
        check_file_errors();
      }
    }

    ygm_file &operator=(const ygm_file &other) { return *this = ygm_file(other); }

    ygm_file &operator=(ygm_file &&other) noexcept {
      std::swap(m_file_io, other.m_file_io);
      std::swap(m_id, other.m_id);
      std::swap(m_size, other.m_size);
      std::swap(m_active, other.m_active);
      std::swap(m_file_info, other.m_file_info);
      return *this;
    }

    size_t id() const  { return m_id; }
    size_t size() const { return m_size; }
    bool isopen() const { return m_file_io.is_open(); }
    bool isactive() const { return m_active; }
    file_base_info get_file_info() const { return m_file_info; }

    inline void open() {
      if(!m_file_io.is_open())
        m_file_io.open(m_file_info.generate_filename(m_id), std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
      check_file_errors();
      m_active = true;
    }

    inline void close() {
      if(m_file_io.is_open())
        m_file_io.close();
      m_active = false;
    }

    inline void delete_file() {
      if(m_file_io.is_open())
        m_file_io.close();
      std::filesystem::remove(m_file_info.generate_filename(m_id));
    }

    bool to_file(const Item &data) {
      YGM_ASSERT_RELEASE(m_file_io.is_open());
      if (!m_active) return false;
      if (!m_file_io.eof()) m_file_io.seekg(0, std::ios::end);
      
      cereal::BinaryOutputArchive oarchive(m_file_io);
      oarchive(data);
      m_size++;

      if (m_size > m_file_info.file_threshold) {
        m_active = false;
        m_file_io.close();
      }

      return true;
    }

    bool from_file(Item &data) {
      YGM_ASSERT_RELEASE(m_file_io.is_open());
      if (m_file_io.peek() == EOF) return false;
      else {
        cereal::BinaryInputArchive iarchive(m_file_io);
        iarchive(data);
        return true;
      }
    }

    void vector_from_file(std::vector<Item>& storage) {
      YGM_ASSERT_RELEASE(m_file_io.is_open());
      cereal::BinaryInputArchive iarchive(m_file_io);
      m_file_io.seekg(0, std::ios::beg);
      while(m_file_io.peek() != EOF) {
        Item temp;
        iarchive(temp);
        std::cout << temp << std::endl;
        storage.push_back(temp);
      }
      //YGM_ASSERT_RELEASE(storage.size() == m_size);
    }

    std::vector<Item> vector_from_file() {
      std::vector<Item> ret;
      vector_from_file(ret);
      return ret;
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
    void write_vector_back(std::vector<Item>::iterator begin, std::vector<Item>::iterator end) {
      YGM_ASSERT_RELEASE(std::distance(begin, end) <= m_file_info.file_threshold);

      std::string fname = m_file_info.generate_filename(m_id);
      m_file_io.close();
      std::filesystem::remove(fname);
      m_file_io.open(fname, std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);
      check_file_errors();
      cereal::BinaryOutputArchive oarchive(m_file_io);
      m_size = 0;
      while(begin != end) {
        oarchive(*begin);
        m_size++;
        begin++;
      }
    }

    void check_file_errors() {
      if (!m_file_io.is_open()) {
        throw std::runtime_error(std::string("File failed to open: " + m_file_info.generate_filename(m_id) + " " + std::strerror(errno)));
      }

      if (m_file_io.fail()) {
        throw std::runtime_error(std::string("File failed to open: " + m_file_info.generate_filename(m_id) + " " + std::strerror(errno)));
      }

      if (m_file_io.bad()) {
        throw std::runtime_error(std::string("File failed to open with badbit set: " + m_file_info.generate_filename(m_id) + " " + std::strerror(errno)));
      }
    }
   private:
    size_t                          m_id;
    size_t                          m_size;
    bool                            m_active;
    file_base_info                 &m_file_info;
   public:
    std::fstream                    &m_file_io;

  };
  
  oversized_bag(ygm::comm &comm) : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    m_file_info.file_rank = comm.rank();
    load_existing_files();
  }

  oversized_bag(ygm::comm &comm, size_t file_threshold) : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    m_file_info.file_rank = comm.rank();
    m_file_info.file_threshold = file_threshold;
  }

  oversized_bag(ygm::comm &comm, std::initializer_list<Item> l)
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    m_file_info.file_rank = comm.rank();
    load_existing_files();

    if (m_comm.rank0()) {
      for (const Item &i : l) {
        async_insert(i);
      }
    }
    m_comm.barrier();
  }

  oversized_bag(ygm::comm &comm, std::initializer_list<Item> l, size_t file_threshold)
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    m_file_info.file_rank = comm.rank();
    m_file_info.file_threshold = file_threshold;
    load_existing_files();

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
    m_file_info.file_rank = comm.rank();
    m_file_info.file_base_dir = dir;
    load_existing_files();
  }

  oversized_bag(ygm::comm &comm, std::string dir, size_t file_threshold)
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    m_file_info.file_rank = comm.rank();
    m_file_info.file_threshold = file_threshold;
    m_file_info.file_base_dir = dir;
    load_existing_files();
  }

  /** @todo ----------------------------------------------unchanged---------------------------------------------- */
  template <typename STLContainer>
  oversized_bag(ygm::comm          &comm,
      const STLContainer &cont) requires detail::STLContainer<STLContainer> &&
      std::convertible_to<typename STLContainer::value_type, Item>
      : m_comm(comm), pthis(this), partitioner(comm) {
    pthis.check(m_comm);
    m_file_info.file_rank = comm.rank();
    load_existing_files();

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
    m_file_info.file_rank = comm.rank();
    load_existing_files();

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
      file.m_file_io.close();
    }
  }

  /** @todo ----------------------------------------------unchanged---------------------------------------------- */
  oversized_bag(const self_type &other)  // If I remove const it compiles
      : m_comm(other.comm()), pthis(this), partitioner(other.comm()), m_file_info{other.m_file_info} {
    pthis.check(m_comm);
  }

  /** @todo */
  oversized_bag(self_type &&other) noexcept
      : m_comm(other.comm()),
        pthis(this),
        partitioner(other.comm()),
        m_file_info{other.m_file_info} {
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
    std::swap(this->m_file_info, other.m_file_info);
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
    if (m_files.empty() || !m_files.back().isopen()) 
      open_new_file();

    m_files.back().to_file(val);
    m_local_size++;
  }

  /** @todo */
  void local_clear() {
    for (auto &file : m_files) {
      if(file.isopen()) {
        file.m_file_io.close();
      }
      file.delete_file();
    }
    m_files.clear();
    m_local_size = 0;
  }

  /** @todo testing needed */
  size_t local_size() const { return m_local_size; }

  /** @todo testing needed */
  size_t local_count(const value_type &val) const {
    size_t count = 0;
    std::ifstream is;
    for (auto &file : m_files) {
      is.open(m_file_info.generate_filename(file.id()), std::ios::in | std::ios::binary);
      is.seekg(0, std::ios::beg);
      if (!is.is_open()) {
        throw std::runtime_error("Failed to open file: " + m_file_info.generate_filename(file.id()));
      }
      cereal::BinaryInputArchive iarchive(is);
      while (is.peek() != EOF) {
        Item temp;
        iarchive(temp);
        m_comm.cout() << temp << " : " << val << std::endl;
        if (temp == val) {
          count++;
        }
      }
      is.close();
    }
    return count;
  }

  /** @todo testing needed */
  template <typename Function>
  void local_for_all(Function fn) {
   std::vector<Item> temp_storage;
   bool temp_open = false;
    for (auto &file : m_files) {
      if(!file.isopen()) {
        file.open();
        temp_open = true;
      }

      file.vector_from_file(temp_storage);
      for(auto &item : temp_storage) {
        fn(item);
      }
      file.write_vector_back(temp_storage.begin(), temp_storage.end());
      temp_storage.clear();

      if(temp_open) {
        file.m_file_io.close();
        temp_open = false;
      }
    }
  }

  /** @todo testing needed */
  template <typename Function>
  void local_for_all(Function fn) const {
    std::ifstream is;
    for (auto &file : m_files) {
      is.open(m_file_info.generate_filename(file.id()), std::ios::in | std::ios::binary);
      if (!is.is_open()) {
        throw std::runtime_error("Failed to open file: " + m_file_info.generate_filename(file.id()));
      }
      cereal::BinaryInputArchive iarchive(is);
      while (is.peek() != EOF) {
        Item temp;
        iarchive(temp);
        fn(temp);
      }
      is.close();
    }
  }
  
  /** @todo testing needed */  
  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::vector<Item> temp_storage;
    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::out | std::ios::binary);
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
    std::ifstream is(rank_fname, std::ios::in | std::ios::binary);
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

  void load_existing_files() {
    bool isValidFile = true;
    m_local_size = 0;
    int file_id = 0;
    while(isValidFile) {
      std::string file_name = m_file_info.generate_filename(file_id);
      if(std::filesystem::exists(file_name)) {
        ygm_file temp_file(m_file_info, file_id);
        size_t items_in_file = temp_file.vector_from_file().size(); 
        m_files.push_back(ygm_file(m_file_info, file_id, items_in_file)); // add this constructor to ygm_file
        m_local_size += items_in_file;
        temp_file.close();
        file_id++;
      } else {
        isValidFile = false;
      }
    }
  }

   /** @todo testing needed */
  std::vector<value_type> local_pop(int n) {
    YGM_ASSERT_RELEASE(n <= local_size());
    std::vector<value_type> ret;
    std::vector<value_type> temp_storage;

    while(temp_storage.size() < n) {
      m_files.back().vector_from_file(temp_storage);
      if(temp_storage.size() < n) {
        m_files.back().remove_file();
        m_files.pop_back();
        m_files.back().open();
      }
    }

    size_t new_size  = m_local_size - n;
    auto pop_start = temp_storage.begin() + new_size;

    ret.assign(pop_start, temp_storage.end());
    m_files.back().write_vector_back(temp_storage.begin(), temp_storage.end() - n);
    m_local_size = new_size;
    return ret;
  }

  /** @todo testing needed*/
  void local_swap(self_type &other) { 
    this->m_files.swap(other.m_files); 
    std::swap(this->m_local_size, other.m_local_size);
    std::swap(this->m_file_info, other.m_file_info);
  }

  inline void open_new_file() {
    m_files.push_back(ygm_file(this->m_file_info, m_files.size()));
  }

  ygm::comm                        &m_comm;
  std::vector<ygm_file>             m_files; 
  size_t                            m_local_size;
  file_base_info                    m_file_info;            
  typename ygm::ygm_ptr<self_type>  pthis;
};

}  // namespace ygm::container
