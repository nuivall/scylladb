\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

----------------------- MODULE apa_vnodes_tablets_migration -------------------
(***************************************************************************)
(* The vnodes-to-tablets migration of one keyspace, node by node: prepare  *)
(* (tablet maps mirroring the vnode replicas), marking a node's intended   *)
(* storage mode, the restart that reshards its sstables into that mode,    *)
(* the mode it publishes, and finalization forward (the keyspace switches  *)
(* to tablets) or back (the maps are dropped).  Optionally, the topology   *)
(* operations the code doesn't reject during a migration (bootstrap,       *)
(* vnodes cleanup), which the documentation forbids.                       *)
(*                                                                         *)
(* One DC, one rack, RF=1: each token range r has one vnode replica        *)
(* vowner[r], and after prepare one tablet replica towner[r] (the map is   *)
(* the vnode layout at prepare; only a replace updates it, and a replace   *)
(* isn't modelled).  A node's data is data[n][r], what its disk holds of   *)
(* range r.  Writes go to the vnode replica until finalization and to the  *)
(* tablet replica after it.                                                *)
(*                                                                         *)
(* A node's layout is the flavor its tables were built with at boot        *)
(* (replica::database::get_boot_storage_mode()), which is also how its     *)
(* sstables are split.  At boot the node reads its intended mode from its  *)
(* local system.topology before catching up with group0, so a mark it      *)
(* committed but didn't apply (it died in between) is missed until the    *)
(* next boot (update_topology_with_local_metadata()).  The direction is    *)
(* the one distributed_loader::populate_keyspace() picks:                  *)
(*   - forward (the table uses tablets, the keyspace doesn't): reshard by  *)
(*     tablet; data of a range whose tablet isn't on this node has no      *)
(*     storage group, and loading it fails (on_internal_error,             *)
(*     SCYLLADB-5816);                                                     *)
(*   - rollback or none, the keyspace using vnodes: plain resharding       *)
(*     reorganizes any tablet-split sstables by vnode shard;               *)
(*   - none, the keyspace using tablets: no resharding by tablet; an       *)
(*     sstable left in the vnode layout spans tablets, and loading it      *)
(*     fails.                                                              *)
(* A node that booted publishes current_storage_mode := its layout, if     *)
(* the TOPOLOGY_CURRENT_STORAGE_MODE feature is on and its row records an  *)
(* intent.  Prepare seeds "vnodes" for the normal nodes of that moment.    *)
(*                                                                         *)
(* Finalization (topology_coordinator, finalize_migration) first          *)
(* refreshes the load stats: a live node reports whether its tables are    *)
(* tablet-based; a down node keeps its cached entry (collect_tablet_load_  *)
(* stats(require_live_nodes::no)); a new coordinator starts with none.     *)
(* Forward needs every node's intent = tablets, its stats = tablets, and   *)
(* its published mode, if any, = tablets.  Rollback needs every intent =   *)
(* vnodes, no stats = tablets (a node without stats passes), and no        *)
(* published mode = tablets.                                               *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Apalache

CONSTANTS
    \* @type: Set(Str);
    Joiners,
    \* @type: Int;
    MaxWrites,
    \* @type: Int;
    MaxRestarts,
    \* @type: Int;
    MaxStops,
    \* @type: Int;
    MaxLeaderChanges,
    \* @type: Bool;
    CurrentColumn

Members0 == {"a", "b"}
Nodes == Members0 \cup Joiners
Ranges == {1, 2}
InitOwner == [r \in Ranges |-> IF r = 1 THEN "a" ELSE "b"]
JoinRange == 2
None == "none"

VARIABLES
    \* @type: Set(Str);
    member,
    \* @type: Set(Str);
    up,
    \* @type: Int -> Str;
    vowner,
    \* @type: Int -> Str;
    towner,
    \* @type: Bool;
    ksTablets,
    \* @type: Str -> Str;
    layout,
    \* @type: Str -> Str;
    intent,
    \* @type: Str -> Str;
    linten,
    \* @type: Str -> Str;
    cur,
    \* @type: Str -> Str;
    stats,
    \* @type: Str -> (Int -> Set(Int));
    data,
    \* @type: Set(<<Int, Int>>);
    written,
    \* @type: Set(Str);
    stuck,
    \* @type: Bool;
    usedTablets,
    \* @type: Bool;
    rolledBack,
    \* @type: Int;
    nw,
    \* @type: Int;
    nr,
    \* @type: Int;
    ns,
    \* @type: Int;
    nl

vars == <<member, up, vowner, towner, ksTablets, layout, intent, linten, cur,
          stats, data, written, stuck, usedTablets, rolledBack, nw, nr, ns, nl>>

Migrating == towner[1] # None /\ ~ksTablets
Route(r) == IF ksTablets THEN towner[r] ELSE vowner[r]

Init ==
    /\ member = Members0
    /\ up = Members0
    /\ vowner = InitOwner
    /\ towner = [r \in Ranges |-> None]
    /\ ksTablets = FALSE
    /\ layout = [n \in Nodes |-> "v"]
    /\ intent = [n \in Nodes |-> None]
    /\ linten = [n \in Nodes |-> None]
    /\ cur = [n \in Nodes |-> None]
    /\ stats = [n \in Nodes |-> None]
    /\ data = [n \in Nodes |-> [r \in Ranges |-> {}]]
    /\ written = {}
    /\ stuck = {}
    /\ usedTablets = FALSE
    /\ rolledBack = FALSE
    /\ nw = 0 /\ nr = 0 /\ ns = 0 /\ nl = 0

(* A write lands on the replica the coordinator routes it to.  A node      *)
(* whose tables are tablet-based has no storage for a tablet it doesn't    *)
(* hold, and the write fails.                                              *)
Write(r) ==
    LET o == Route(r) IN
    /\ nw < MaxWrites
    /\ o \in up
    /\ layout[o] = "t" => towner[r] = o
    /\ nw' = nw + 1
    /\ data' = [data EXCEPT ![o][r] = @ \cup {nw + 1}]
    /\ written' = written \cup {<<r, nw + 1>>}
    /\ UNCHANGED <<member, up, vowner, towner, ksTablets, layout, intent, linten,
                   cur, stats, stuck, usedTablets, rolledBack, nr, ns, nl>>

Prepare ==
    /\ ~ksTablets
    /\ \A r \in Ranges : towner[r] = None
    /\ towner' = vowner
    /\ cur' = IF CurrentColumn THEN [n \in Nodes |-> IF n \in member THEN "v" ELSE cur[n]] ELSE cur
    /\ UNCHANGED <<member, up, vowner, ksTablets, layout, intent, linten, stats,
                   data, written, stuck, usedTablets, rolledBack, nw, nr, ns, nl>>

(* nodetool migrate-to-tablets upgrade / downgrade, on the node itself.   *)
(* With applied = FALSE the node dies between committing the mark and      *)
(* applying it locally.                                                    *)
Mark(n, m, applied) ==
    /\ n \in up /\ n \in member
    /\ Migrating
    /\ intent[n] # m
    /\ intent' = [intent EXCEPT ![n] = m]
    /\ IF applied
       THEN /\ linten' = [linten EXCEPT ![n] = m]
            /\ UNCHANGED <<up, ns>>
       ELSE /\ ns < MaxStops
            /\ up' = up \ {n}
            /\ ns' = ns + 1
            /\ UNCHANGED linten
    /\ UNCHANGED <<member, vowner, towner, ksTablets, layout, cur, stats, data,
                   written, stuck, usedTablets, rolledBack, nw, nr, nl>>

Stop(n) ==
    /\ n \in up
    /\ ns < MaxStops
    /\ up' = up \ {n}
    /\ ns' = ns + 1
    /\ UNCHANGED <<member, vowner, towner, ksTablets, layout, intent, linten, cur,
                   stats, data, written, stuck, usedTablets, rolledBack, nw, nr, nl>>

Boot(n) ==
    LET mode == IF ksTablets THEN "t"
                ELSE IF Migrating /\ linten[n] = "t" THEN "t" ELSE "v"
        forward == ~ksTablets /\ mode = "t"
        ok == IF forward THEN \A r \in Ranges : data[n][r] # {} => towner[r] = n
              ELSE IF ksTablets THEN layout[n] = "t"
              ELSE TRUE
    IN
    /\ n \in member \ up
    /\ n \notin stuck
    /\ nr < MaxRestarts
    /\ nr' = nr + 1
    /\ IF ok
       THEN /\ up' = up \cup {n}
            /\ layout' = [layout EXCEPT ![n] = mode]
            /\ linten' = [linten EXCEPT ![n] = intent[n]]
            /\ cur' = IF CurrentColumn /\ intent[n] # None
                      THEN [cur EXCEPT ![n] = mode] ELSE cur
            /\ usedTablets' = (usedTablets \/ mode = "t")
            /\ UNCHANGED stuck
       ELSE /\ stuck' = stuck \cup {n}
            /\ UNCHANGED <<up, layout, linten, cur, usedTablets>>
    /\ UNCHANGED <<member, vowner, towner, ksTablets, intent, stats, data, written,
                   rolledBack, nw, ns, nl>>

Refreshed == [n \in Nodes |-> IF n \in up THEN layout[n] ELSE stats[n]]

RefreshStats ==
    /\ stats # Refreshed
    /\ stats' = Refreshed
    /\ UNCHANGED <<member, up, vowner, towner, ksTablets, layout, intent, linten,
                   cur, data, written, stuck, usedTablets, rolledBack, nw, nr, ns, nl>>

(* A new topology coordinator starts with no load stats.                  *)
LeaderChange ==
    /\ nl < MaxLeaderChanges
    /\ nl' = nl + 1
    /\ stats' = [n \in Nodes |-> None]
    /\ UNCHANGED <<member, up, vowner, towner, ksTablets, layout, intent, linten,
                   cur, data, written, stuck, usedTablets, rolledBack, nw, nr, ns>>

FinalizeForward ==
    LET st == Refreshed IN
    /\ Migrating
    /\ \A n \in member : intent[n] = "t" /\ st[n] = "t" /\ cur[n] \in {None, "t"}
    /\ stats' = st
    /\ ksTablets' = TRUE
    /\ intent' = [n \in Nodes |-> None]
    /\ cur' = [n \in Nodes |-> None]
    /\ UNCHANGED <<member, up, vowner, towner, layout, linten, data, written, stuck,
                   usedTablets, rolledBack, nw, nr, ns, nl>>

FinalizeRollback ==
    LET st == Refreshed IN
    /\ Migrating
    /\ \A n \in member : intent[n] \in {None, "v"} /\ st[n] # "t" /\ cur[n] \in {None, "v"}
    /\ stats' = st
    /\ rolledBack' = usedTablets
    /\ towner' = [r \in Ranges |-> None]
    /\ intent' = [n \in Nodes |-> None]
    /\ cur' = [n \in Nodes |-> None]
    /\ UNCHANGED <<member, up, vowner, ksTablets, layout, linten, data, written, stuck,
                   usedTablets, nw, nr, ns, nl>>

(* Bootstrap: the joiner takes JoinRange over and streams its data from    *)
(* the old replica, which keeps it until cleanup.  The tablet maps of a    *)
(* migrating table aren't updated, and the joiner gets no seeded mode.     *)
Bootstrap(c) ==
    LET old == vowner[JoinRange] IN
    /\ c \in Joiners \ member
    /\ ~ksTablets
    /\ old \in up
    /\ member' = member \cup {c}
    /\ up' = up \cup {c}
    /\ vowner' = [vowner EXCEPT ![JoinRange] = c]
    /\ data' = [data EXCEPT ![c][JoinRange] = data[old][JoinRange]]
    /\ UNCHANGED <<towner, ksTablets, layout, intent, linten, cur, stats, written,
                   stuck, usedTablets, rolledBack, nw, nr, ns, nl>>

(* nodetool cleanup of a node whose tables are still vnode-based: drops    *)
(* the ranges it no longer replicates.                                     *)
Cleanup(n) ==
    /\ n \in up
    /\ ~ksTablets
    /\ layout[n] = "v"
    /\ \E r \in Ranges : vowner[r] # n /\ data[n][r] # {}
    /\ data' = [data EXCEPT ![n] = [r \in Ranges |-> IF vowner[r] = n THEN @[r] ELSE {}]]
    /\ UNCHANGED <<member, up, vowner, towner, ksTablets, layout, intent, linten,
                   cur, stats, written, stuck, usedTablets, rolledBack, nw, nr, ns, nl>>

Next ==
    \/ \E r \in Ranges : Write(r)
    \/ Prepare
    \/ \E n \in Nodes, m \in {"v", "t"}, a \in BOOLEAN : Mark(n, m, a)
    \/ \E n \in Nodes : Stop(n) \/ Boot(n) \/ Cleanup(n)
    \/ RefreshStats
    \/ LeaderChange
    \/ FinalizeForward
    \/ FinalizeRollback
    \/ \E c \in Joiners : Bootstrap(c)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

\* No acknowledged write is gone from every disk.
Durable == \A w \in written : \E n \in Nodes : w[2] \in data[n][w[1]]

\* A live replica that reads are routed to has every acknowledged write.
Readable == \A w \in written : Route(w[1]) \in up => w[2] \in data[Route(w[1])][w[1]]

\* After finalization every node's tables are tablet-based; one that isn't
\* fails its next boot, and the keyspace can't go back to vnodes.
LayoutMatches == ksTablets => \A n \in member : layout[n] = "t"

\* No boot fails.
BootOK == stuck = {}

\* Witnesses: written to be violated.
W_Finalized == ~ksTablets
W_RolledBack == ~rolledBack
W_FinalizedAfterJoin == ~(ksTablets /\ \E c \in Joiners : c \in member)
\* Constant initializations for --cinit.
CInitBug == Joiners = {} /\ MaxWrites = 2 /\ MaxRestarts = 4 /\ MaxStops = 4 /\ MaxLeaderChanges = 1 /\ CurrentColumn = FALSE
CInitFix == Joiners = {} /\ MaxWrites = 2 /\ MaxRestarts = 4 /\ MaxStops = 4 /\ MaxLeaderChanges = 1 /\ CurrentColumn = TRUE
CInitTopo == Joiners = {"c"} /\ MaxWrites = 2 /\ MaxRestarts = 3 /\ MaxStops = 3 /\ MaxLeaderChanges = 1 /\ CurrentColumn = TRUE
CInitBugLarge == Joiners = {"c"} /\ MaxWrites = 2 /\ MaxRestarts = 4 /\ MaxStops = 4 /\ MaxLeaderChanges = 1 /\ CurrentColumn = FALSE
CInitFixLarge == Joiners = {"c"} /\ MaxWrites = 2 /\ MaxRestarts = 4 /\ MaxStops = 4 /\ MaxLeaderChanges = 1 /\ CurrentColumn = TRUE
\* MC_current checks all four; MC_topology_ops leaves BootOK out.
InvFix == Durable /\ Readable /\ LayoutMatches /\ BootOK
InvTopo == Durable /\ Readable /\ LayoutMatches
=============================================================================
