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

  }

  //
  // Test Rank 0 async_insert
  {
    ygm::container::oversized_bag<std::string> bbag(world);

  }

  //
  // Test local_shuffle and global_shuffle
  {
    ygm::container::oversized_bag<int> bbag(world);

  }

  //
  // Test for_all
  {
    ygm::container::oversized_bag<std::string> bbag(world);

  }

  //
  // Test for_all (pair)
  {
    ygm::container::oversized_bag<std::pair<std::string, int>> pbag(world);

  }

  //
  // Test for_all (split pair)
  {
    ygm::container::bag<std::pair<std::string, int>> pbag(world);
  
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
