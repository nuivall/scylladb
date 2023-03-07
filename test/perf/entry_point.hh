/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <functional>

namespace perf {

int scylla_fast_forward_main(int argc, char** argv);
int scylla_row_cache_update_main(int argc, char**argv);
int scylla_simple_query_main(int argc, char** argv);
int scylla_sstable_main(int argc, char** argv);
std::function<int(int, char**)> alternator_workloads(std::function<int(int, char**)> scylla_main);

} // namespace tools
