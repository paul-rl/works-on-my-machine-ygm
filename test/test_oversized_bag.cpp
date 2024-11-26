// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <set>
#include <string>
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/oversized_bag.hpp>
#include <ygm/random.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  // Test basic tagging
  {
    ygm::container::oversized_bag<std::string> bbag(world);
    static_assert(std::is_same_v<decltype(bbag)::self_type, decltype(bbag)>);
    static_assert(std::is_same_v<decltype(bbag)::value_type, std::string>);
    static_assert(std::is_same_v<decltype(bbag)::size_type, size_t>);
    static_assert(std::is_same_v<decltype(bbag)::for_all_args,
                                 std::tuple<decltype(bbag)::value_type>>);
  }

  //
  // Test Rank 0 async_insert
  {
    ygm::container::oversized_bag<std::string> bbag(world);
    if (world.rank0()) {
      bbag.async_insert("dog");
      bbag.async_insert("apple");
      bbag.async_insert("red");
    }
    world.barrier();
    // Count and size functions come from overarching ygm container,
    // rely on local functions
    //std::cout << "Dog: " << bbag.count("dog") << std::endl;
    //YGM_ASSERT_RELEASE(bbag.count("dog") == 1);
    // YGM_ASSERT_RELEASE(bbag.count("apple") == 1);
    // YGM_ASSERT_RELEASE(bbag.count("red") == 1);
    // YGM_ASSERT_RELEASE(bbag.size() == 3);
    // bbag.clear();
  }

  //
  // Test Rank 0 local insert on addition past file threshold
  {
    size_t threshold = 1;
    ygm::container::oversized_bag<std::string> bbag = ygm::container::oversized_bag<std::string>(world, threshold);
    if (world.rank0()) {
      bbag.local_insert("dog");
      YGM_ASSERT_RELEASE(bbag.num_local_files() == 1);
      bbag.local_insert("apple");
      YGM_ASSERT_RELEASE(bbag.num_local_files() == 2);
      bbag.local_insert("red");
      YGM_ASSERT_RELEASE(bbag.num_local_files() == 3);
    }
    world.barrier();
    
    // Count and size functions come from overarching ygm container,
    // rely on local functions
    YGM_ASSERT_RELEASE(bbag.count("dog") == 1);
    YGM_ASSERT_RELEASE(bbag.count("apple") == 1);
    YGM_ASSERT_RELEASE(bbag.count("red") == 1);
    YGM_ASSERT_RELEASE(bbag.size() == 3);
    bbag.clear();
  }


  //
  // Test Rebalance
  {
    ygm::container::oversized_bag<std::string> bbag(world);
    bbag.async_insert("begin", 0);
    bbag.async_insert("end", world.size() - 1);
    bbag.rebalance();
    YGM_ASSERT_RELEASE(bbag.local_size() == 2);
  }

  // Currently, shuffle functions are being ignored as per Roger
  // Test local_shuffle and global_shuffle
  // {
  //   ygm::container::oversized_bag<int> bbag(world);
  // 
  // }

  // 
  // Test for_all
  {
    ygm::container::oversized_bag<std::string> bbag(world);
    if (world.rank0()) {
      bbag.async_insert("dog");
      bbag.async_insert("apple");
      bbag.async_insert("red");
    }
    int count{0};
    bbag.for_all([&count](std::string& mstr) { ++count; });
    int global_count = world.all_reduce_sum(count);
    world.barrier();
    YGM_ASSERT_RELEASE(global_count == 3);
  }

  //
  // Test for_all (pair)
  {
    ygm::container::oversized_bag<std::pair<std::string, int>> pbag(world);
    if (world.rank0()) {
      pbag.async_insert({"dog", 1});
      pbag.async_insert({"apple", 2});
      pbag.async_insert({"red", 3});
    }
    int count{0};
    pbag.for_all(
      [&count] (std::pair<std::string, int>& mstr) { count += mstr.second; });
    int global_count = world.all_reduce_sum(count);
    world.barrier();
    YGM_ASSERT_RELEASE(global_count == 6);
  }

  //
  // Test for_all (split pair)
  // {
  //   ygm::container::oversized_bag<std::pair<std::string, int>> pbag(world);
  //   if (world.rank0()) {
  //     pbag.async_insert({"dog"}, 1);
  //     pbag.async_insert({"apple"}, 2);
  //     pbag.async_insert({"red"}, 3);
  //   }
  //   int count{0};
  //   pbag.for_all(
  //     [&count](std::string& first, int& second) { count += second; });
  //   int global_count = world.all_reduce_sum(count);
  //   world.barrier();
  //   YGM_ASSERT_RELEASE(global_count == 6);
  // }


  // Test rebalance
  {
    ygm::container::oversized_bag<std::string> bbag(world);
    bbag.async_insert("begin", 0);
    bbag.async_insert("end", world.size() - 1);
    bbag.rebalance();
    YGM_ASSERT_RELEASE(bbag.local_size() == 2);
  }

  //
  // Test rebalance with non-standard rebalance sizes
  {
    ygm::container::oversized_bag<std::string> bbag(world);
    bbag.async_insert("middle", world.size() / 2);
    bbag.async_insert("end", world.size() - 1);
    if (world.rank0()) bbag.async_insert("middle", world.size() / 2);
    bbag.rebalance();

    size_t target_size      = std::ceil((bbag.size() * 1.0) / world.size());
    size_t remainder        = bbag.size() % world.size();
    size_t small_block_size = bbag.size() / world.size();
    size_t large_block_size =
        bbag.size() / world.size() + (bbag.size() % world.size() > 0);

    if (world.rank() < remainder) {
      YGM_ASSERT_RELEASE(bbag.local_size() == large_block_size);
    } else {
      YGM_ASSERT_RELEASE(bbag.local_size() == small_block_size);
    }
  }

  //
  // Test output data after rebalance
  {
    ygm::container::oversized_bag<int> bbag(world);
    if (world.rank0()) {
      for (int i = 0; i < 100; i++) {
        bbag.async_insert(i, (i * 3) % world.size());
      }
      for (int i = 100; i < 200; i++) {
        bbag.async_insert(i, (i * 5) % world.size());
      }
    }
    bbag.rebalance();

    std::set<int> value_set;
    bbag.gather(value_set, 0);
    if (world.rank0()) {
      YGM_ASSERT_RELEASE(value_set.size() == 200);
      YGM_ASSERT_RELEASE(*std::min_element(value_set.begin(), value_set.end()) ==
                     0);
      YGM_ASSERT_RELEASE(*std::max_element(value_set.begin(), value_set.end()) ==
                     199);
    }
  }

}
