// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

#pragma once

#include "perspective/exports.h"
#include "perspective/raw_types.h"
#include "perspective/scalar.h"
#include "perspective/schema.h"
#include "perspective/table.h"
#include <map>
#include <memory>
#include <string>
#include <tsl/hopscotch_map.h>
#include <tsl/hopscotch_set.h>
#include <perspective.pb.h>

namespace perspective::server {

struct JoinDef {
    std::string left_table_id;
    std::string right_table_id;
    std::string on_column;
    std::string right_on_column;
    proto::JoinType join_type;
};

struct MakeJoinResult {
    std::shared_ptr<Table> table;
    std::string error;

    bool
    ok() const {
        return error.empty();
    }
};

struct JoinCache {
    tsl::hopscotch_map<t_tscalar, std::vector<t_uindex>> right_index;
    std::vector<std::pair<t_tscalar, t_uindex>> right_entries;
    bool valid = false;
};

class PERSPECTIVE_EXPORT JoinEngine {
public:
    using t_id = std::string;

    void register_join(
        const t_id& join_table_id,
        const t_id& left_table_id,
        const t_id& right_table_id,
        const std::string& on_column,
        const std::string& right_on_column,
        proto::JoinType join_type
    );

    void unregister_join(const t_id& join_table_id);
    bool is_join_table(const t_id& id) const;
    std::vector<t_id> get_dependent_join_tables(const t_id& source_table_id
    ) const;
    const JoinDef& get_join_def(const t_id& join_table_id) const;

    MakeJoinResult make_join_table(
        const std::string& on_column,
        const std::string& right_on_column,
        proto::JoinType join_type,
        const std::shared_ptr<Table>& left_table,
        const std::shared_ptr<Table>& right_table
    );

    void recompute(
        const t_id& join_table_id,
        const std::shared_ptr<Table>& left_table,
        const std::shared_ptr<Table>& right_table,
        const std::shared_ptr<Table>& join_table,
        bool left_changed = true,
        bool right_changed = true
    );

private:
    void build_right_index(
        JoinCache& cache,
        const std::shared_ptr<Table>& right_table,
        const std::string& on_column
    );

    tsl::hopscotch_map<t_id, JoinDef> m_join_defs;
    tsl::hopscotch_map<t_id, JoinCache> m_caches;
    std::multimap<t_id, t_id> m_table_to_join_tables;
};

} // namespace perspective::server
