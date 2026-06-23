import re
from collections import defaultdict

log_file_path = "/home/marcin/Downloads/logs/journactl-logs-10.138.71.206-2026-06-23.log"

# Find all events chronologically
# Format of lines:
# Jun 23 03:13:26 ... Initiating tablet streaming ...
# Jun 23 03:13:28 ... Initiating tablet cleanup ...

line_pattern = re.compile(
    r"^(?P<timestamp>Jun \d+ \d+:\d+:\d+) .* raft_topology - (?P<event>Initiating tablet streaming \(intranode_migration\) of|Initiating tablet cleanup of) (?P<table_uuid>[0-9a-f-]+):(?P<tablet_id>\d+) (to|on) (?P<node_uuid>[0-9a-f-]+):(?P<shard>\d+)"
)

events = []

with open(log_file_path, 'r') as f:
    for line in f:
        m = line_pattern.search(line)
        if m:
            timestamp = m.group("timestamp")
            is_stream = "streaming" in m.group("event")
            table_uuid = m.group("table_uuid")
            tablet_id = int(m.group("tablet_id"))
            node_uuid = m.group("node_uuid")
            shard = int(m.group("shard"))
            events.append({
                'time': timestamp,
                'is_stream': is_stream,
                'key': (node_uuid, table_uuid, tablet_id),
                'shard': shard
            })

print(f"Parsed {len(events)} chronological events.")

# Reconstruct chronological state transitions for each tablet
# A migration consists of:
# 1. 'stream' to DST
# 2. 'cleanup' on SRC
# Usually, these two events occur almost at the same time (within a few seconds).
# Let's group events into actual migrations.
tablet_migrations = defaultdict(list)

# We will trace the state of each tablet.
# To do this correctly, we can keep track of:
# - current_location: where the tablet is currently active
# - pending_migration: if we saw a stream to DST, we are pending the cleanup of SRC
pending_streams = {} # key -> pending_dst_shard

migrations_list = defaultdict(list) # key -> list of (src, dst, time)

for ev in events:
    key = ev['key']
    shard = ev['shard']
    t = ev['time']
    
    if ev['is_stream']:
        pending_streams[key] = (shard, t)
    else:
        # This is a cleanup on SRC
        if key in pending_streams:
            dst_shard, stream_time = pending_streams[key]
            src_shard = shard
            migrations_list[key].append((src_shard, dst_shard, stream_time))
            del pending_streams[key]
        else:
            # We missed the stream event (it was before the log start), so we don't have dst_shard
            pass

# Now let's analyze the migration paths for each tablet!
total_tablets = len(migrations_list)
looped_tablets = 0
immediate_loops = 0

print(f"Total tablets with fully tracked migrations: {total_tablets}")

for key, migrations in migrations_list.items():
    if len(migrations) < 2:
        continue
        
    # Reconstruct the sequence of shards visited.
    # For example, if we have:
    # (2, 111), (111, 52)
    # The sequence of shards is 2 -> 111 -> 52.
    # Let's verify that the destinations and sources match up.
    shards_visited = [migrations[0][0]] # Start with the first src
    for src, dst, _ in migrations:
        shards_visited.append(dst)
        
    # Check for any loops (visiting the same shard multiple times)
    if len(shards_visited) != len(set(shards_visited)):
        looped_tablets += 1
        
        # Check for immediate loop (e.g. A -> B -> A)
        has_immediate = False
        for i in range(len(shards_visited) - 2):
            if shards_visited[i] == shards_visited[i+2]:
                has_immediate = True
                break
        if has_immediate:
            immediate_loops += 1
            node, table, tid = key
            print(f"  Loop detected for tablet {table}:{tid} on node {node[:8]}: {' -> '.join(map(str, shards_visited))}")

print(f"\nReal Looping Tablets: {looped_tablets} ({looped_tablets/total_tablets*100:.1f}%)")
print(f"Real Immediate Ping-Pong (A -> B -> A): {immediate_loops} ({immediate_loops/total_tablets*100:.1f}%)")
