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
    // Count and size functions come from overarching ygm container,
    // rely on local functions
    YGM_ASSERT_RELEASE(bbag.count("dog") == 1);
    YGM_ASSERT_RELEASE(bbag.count("apple") == 1);
    YGM_ASSERT_RELEASE(bbag.count("red") == 1);
    YGM_ASSERT_RELEASE(bbag.size() == 3);
  }

  // TODO: Currently, shuffle functions are being ignored as per Ryan
  // Test local_shuffle and global_shuffle
  {
    ygm::container::oversized_bag<int> bbag(world);

  }

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
  {
    ygm::container::oversized_bag<std::pair<std::string, int>> pbag(world);
    if (world.rank0()) {
      pbag.async_insert({"dog"}, 1);
      pbag.async_insert({"apple"}, 2);
      pbag.async_insert({"red"}, 3);
    }
    int count{0};
    pbag.for_all(
      [&count](std::string& first, int& second) { count += second; });
    int global_count = world.all_reduce_sum(count);
    world.barrier();
    YGM_ASSERT_RELEASE(global_count == 6);
  }

  //
  // Test rebalance
  {
    ygm::container::oversized_bag<std::string> bbag(world);

  }

  //
  // Test rebalance with non-standard rebalance sizes
  {
    ygm::container::oversized_bag<std::string> bbag(world);

  }

  //
  // Test output data after rebalance
  {
    ygm::container::oversized_bag<int> bbag(world);

  }

}
