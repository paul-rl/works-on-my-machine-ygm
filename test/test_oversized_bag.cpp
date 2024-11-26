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
    std::cout << "Dog count: " << bbag.count("dog") << std::endl;
    YGM_ASSERT_RELEASE(bbag.count("dog") == 1);
    YGM_ASSERT_RELEASE(bbag.count("apple") == 1);
    YGM_ASSERT_RELEASE(bbag.count("red") == 1);
    YGM_ASSERT_RELEASE(bbag.size() == 3);
  }
}
