/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/record_value.hh"

#include <limits>
#include <optional>

#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/get.hpp>
#include <boost/variant/static_visitor.hpp>

#include "mutation/converting_mutation_partition_applier.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "mutation/mutation_partition_view.hh"
#include "partition_builder.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"

namespace replica::logstor {

namespace {

// Writes the partition field of a record value. Both overloads write the same bytes: the first
// encodes a partition from memory, the second copies one that is already encoded.
template <typename Output>
void write_partition(Output& out, const schema& s, const mutation_partition& p) {
    mutation_partition_serializer(s, p).write(ser::writer_of_mutation_partition<Output>(out));
}

template <typename Output>
void write_partition(Output& out, const schema&, const ser::mutation_partition_view& p) {
    ser::serialize(out, p);
}

// The record value is the partition the record holds, encoded as a canonical_mutation stripped
// of what the record_header already carries: the table id and the partition key. What is
// left is the version of the schema the record was written under, the column mapping of that
// schema, and the IDL-serialized mutation_partition, each written by the serializer that
// canonical_mutation uses for it.
//
//   i64  schema_version, most significant half
//   i64  schema_version, least significant half
//        column_mapping       ser::serializer<column_mapping>, from idl/mutation.idl.hh
//        mutation_partition   ser::serializer<mutation_partition>, as mutation_partition_serializer
//                             writes it
template <typename Output, typename Partition>
void write_record_value(Output& out, const schema& s, const Partition& p) {
    ser::serializer<int64_t>::write(out, s.version().uuid().get_most_significant_bits());
    ser::serializer<int64_t>::write(out, s.version().uuid().get_least_significant_bits());
    ser::serialize(out, s.get_column_mapping());
    write_partition(out, s, p);
}

// Reads a record value field by field, in the order above. Construction reads the schema
// version; the caller then either reads or skips the mapping, and reads the partition.
template <typename Input>
class record_value_reader {
    Input& _in;
    table_schema_version _schema_version;

    static table_schema_version read_schema_version(Input& in) {
        const auto msb = ser::serializer<int64_t>::read(in);
        const auto lsb = ser::serializer<int64_t>::read(in);
        return table_schema_version(utils::UUID(msb, lsb));
    }

public:
    explicit record_value_reader(Input& in)
        : _in(in)
        , _schema_version(read_schema_version(in))
    { }

    table_schema_version schema_version() const noexcept { return _schema_version; }

    column_mapping read_mapping() {
        return ser::deserialize(_in, std::type_identity<column_mapping>{});
    }

    void skip_mapping() {
        ser::skip(_in, std::type_identity<column_mapping>{});
    }

    ser::mutation_partition_view read_partition() {
        return ser::deserialize(_in, std::type_identity<ser::mutation_partition_view>{});
    }
};

// Returns the serialized partition of a frozen mutation.
ser::mutation_partition_view frozen_partition(const frozen_mutation& m) {
    auto in = ser::as_input_stream(m.representation());
    return ser::deserialize(in, std::type_identity<ser::mutation_view>{}).partition();
}

// Returns the timestamp of a serialized row marker, or nullopt if the row has no marker. Follows
// read_row_marker() in mutation/mutation_partition_view.cc, but reads only the timestamp.
std::optional<api::timestamp_type> marker_timestamp(
        boost::variant<ser::live_marker_view, ser::expiring_marker_view, ser::dead_marker_view,
                ser::no_marker_view, ser::unknown_variant_type> marker) {
    struct visitor : boost::static_visitor<std::optional<api::timestamp_type>> {
        std::optional<api::timestamp_type> operator()(ser::live_marker_view& v) const {
            return v.created_at();
        }
        std::optional<api::timestamp_type> operator()(ser::expiring_marker_view& v) const {
            return v.lm().created_at();
        }
        std::optional<api::timestamp_type> operator()(ser::dead_marker_view& v) const {
            return v.tomb().timestamp();
        }
        std::optional<api::timestamp_type> operator()(ser::no_marker_view&) const {
            return std::nullopt;
        }
        std::optional<api::timestamp_type> operator()(ser::unknown_variant_type&) const {
            throw std::runtime_error("logstor: record has a row marker of an unknown type");
        }
    };
    return boost::apply_visitor(visitor(), marker);
}

// TEST-ONLY candidate fix: the body of both record_tiebreak() overloads.
constexpr uint32_t dead_tiebreak = std::numeric_limits<uint32_t>::max();

bool is_deleted_at(tombstone t, api::timestamp_type ts) {
    return t && t.timestamp >= ts;
}

// The first 4 bytes of a cell value, big-endian and zero-padded. The value is a bytes_view or a
// FragmentedView, depending on the stream it was deserialized from.
template <typename View>
uint32_t value_prefix(View v) {
    uint32_t prefix = 0;
    int n = 0;
    auto take = [&] (bytes_view frag) {
        size_t i = 0;
        for (; n < 4 && i < frag.size(); ++i, ++n) {
            prefix = (prefix << 8) | uint8_t(frag[i]);
        }
        return i;
    };
    if constexpr (std::is_same_v<View, bytes_view>) {
        take(v);
    } else {
        while (n < 4 && !v.empty()) {
            v.remove_prefix(take(v.current_fragment()));
        }
    }
    return n == 0 ? 0 : prefix << (8 * (4 - n));
}

using atomic_cell_variant = boost::variant<ser::live_cell_view, ser::expiring_cell_view, ser::dead_cell_view,
        ser::counter_cell_view, ser::unknown_variant_type>;

// The tiebreak of the first cell of a row, or nullopt if the cell is neither an atomic live
// cell nor a cell deleted at `ts` or later.
struct cell_tiebreak_visitor : boost::static_visitor<std::optional<uint32_t>> {
    api::timestamp_type ts;

    explicit cell_tiebreak_visitor(api::timestamp_type ts) : ts(ts) {}

    std::optional<uint32_t> operator()(atomic_cell_variant& acv) const {
        struct atomic_visitor : boost::static_visitor<std::optional<uint32_t>> {
            api::timestamp_type ts;

            explicit atomic_visitor(api::timestamp_type ts) : ts(ts) {}

            std::optional<uint32_t> operator()(ser::live_cell_view& v) const {
                return value_prefix(v.value().view());
            }
            std::optional<uint32_t> operator()(ser::expiring_cell_view& v) const {
                return value_prefix(v.c().value().view());
            }
            std::optional<uint32_t> operator()(ser::dead_cell_view& v) const {
                if (v.tomb().timestamp() >= ts) {
                    return dead_tiebreak;
                }
                return std::nullopt;
            }
            std::optional<uint32_t> operator()(ser::counter_cell_view&) const {
                return std::nullopt;
            }
            std::optional<uint32_t> operator()(ser::unknown_variant_type&) const {
                return std::nullopt;
            }
        };
        return boost::apply_visitor(atomic_visitor(ts), acv);
    }
    std::optional<uint32_t> operator()(ser::collection_cell_view&) const {
        return std::nullopt;
    }
    std::optional<uint32_t> operator()(ser::unknown_variant_type&) const {
        return std::nullopt;
    }
};

template <typename Input>
uint32_t do_compute_record_tiebreak(Input in, api::timestamp_type ts) {
    record_value_reader reader(in);
    reader.skip_mapping();
    auto partition = reader.read_partition();
    if (is_deleted_at(tombstone(partition.tomb()), ts)) {
        return dead_tiebreak;
    }
    for (auto row : partition.rows()) {
        auto marker = row.marker();
        if (auto* dead = boost::get<ser::dead_marker_view>(&marker); dead && is_deleted_at(tombstone(dead->tomb()), ts)) {
            return dead_tiebreak;
        }
        if (is_deleted_at(tombstone(row.deleted_at()), ts)) {
            return dead_tiebreak;
        }
        for (auto&& column : row.cells().columns()) {
            auto cell = column.c();
            // Only the first cell counts.
            auto key = boost::apply_visitor(cell_tiebreak_visitor(ts), cell);
            if (key == dead_tiebreak) {
                return dead_tiebreak;
            }
            return key ? std::min(*key, dead_tiebreak - 1) : 0;
        }
        // The partition has at most one row.
        break;
    }
    return 0;
}

// A value that does not parse gets the smallest key rather than failing the write or the recovery.
template <typename Input>
uint32_t compute_record_tiebreak(Input in, api::timestamp_type ts) noexcept {
    try {
        return do_compute_record_tiebreak(std::move(in), ts);
    } catch (...) {
        return 0;
    }
}

} // anonymous namespace

uint32_t record_tiebreak(const record_value& v, api::timestamp_type ts) {
    return compute_record_tiebreak(ser::as_input_stream(v.representation()), ts);
}

uint32_t record_tiebreak(bytes_view v, api::timestamp_type ts) {
    return compute_record_tiebreak(ser::as_input_stream(v), ts);
}

api::timestamp_type record_timestamp(const mutation& m) {
    const auto& partition = m.partition();

    for (const auto& row_entry : partition.clustered_rows()) {
        // mutation_partition_serializer does not write dummy rows, so the frozen overload has no
        // dummy rows to skip.
        if (row_entry.dummy()) {
            continue;
        }
        if (!row_entry.row().marker().is_missing()) {
            return row_entry.row().marker().timestamp();
        }
    }

    if (const auto partition_tombstone = partition.partition_tombstone(); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

api::timestamp_type record_timestamp(const frozen_mutation& m) {
    auto partition = frozen_partition(m);

    for (auto row : partition.rows()) {
        if (auto ts = marker_timestamp(row.marker())) {
            return *ts;
        }
    }

    if (const auto partition_tombstone = tombstone(partition.tomb()); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

record_value encode_record_value(const mutation& m) {
    bytes_ostream out;
    write_record_value(out, *m.schema(), m.partition());
    return record_value(std::move(out));
}

record_value encode_record_value(const frozen_mutation& m, const schema& s) {
    bytes_ostream out;
    write_record_value(out, s, frozen_partition(m));
    return record_value(std::move(out));
}

single_row_partition decode_record_value(const record_value& v, const schema& s, const record_header& h) {
    if (s.id() != h.table) {
        throw std::runtime_error(format("logstor: record of table {} decoded with the schema of table {} ({}.{})",
                h.table, s.id(), s.ks_name(), s.cf_name()));
    }

    auto in = ser::as_input_stream(v.representation());
    record_value_reader reader(in);

    // Decode into a mutation_partition, then move the row out of it.
    mutation_partition p(s);
    if (reader.schema_version() == s.version()) {
        reader.skip_mapping();
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_builder b(s, p);
        partition_view.accept(s, b);
    } else {
        column_mapping cm = reader.read_mapping();
        converting_mutation_partition_applier applier(cm, s, p);
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_view.accept(cm, applier);
    }
    return to_single_row_partition(std::move(p), s, h.key.key());
}

} // namespace replica::logstor
