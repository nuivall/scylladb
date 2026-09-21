#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from cassandra.auth import PlainTextAuthProvider

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_current_version_description
from test.cluster.dtest.ccmlib import scylla_repository
from test.cluster.dtest.ccmlib.common import logger
from test.cluster.dtest.ccmlib.scylla_manager import ScyllaManager
from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode

if TYPE_CHECKING:
    from typing import Any


class ScyllaCluster:
    def __init__(self,  # noqa: PLR0913
                 manager: ScyllaClusterManager,
                 scylla_mode: str,
                 force_wait_for_cluster_start: bool = False,
                 scylla_version: str | None = None,
                 manager_install_dir: str | Path | None = None,
                 skip_manager_server: bool = False):
        self.manager = manager
        self.scylla_mode = scylla_mode
        self._config_options = {}
        # Cached ScyllaNode instances, keyed by name and in the order
        # _add_nodes() creates them from servers_add().
        self._nodes: dict[str, ScyllaNode] = {}

        # ccm's Cluster.seeds.  The in-tree manager decides which nodes a
        # starting server actually seeds from, so this list is bookkeeping for
        # the tests that maintain it (cluster_replacement_test, the repair-based
        # node operations); nothing here overrides the manager's choice.
        self.seeds: list = []
        self._next_node_num: int = 1

        # Which Scylla new nodes are started on.  Upgrade tests move this around
        # with set_install_dir(); everything else stays on the build under test.
        scylla_repository.set_build_mode(scylla_mode)
        self._install = scylla_repository.install(scylla_version or scylla_repository.current_version())

        if self.scylla_mode == "debug":
            self.default_wait_other_notice_timeout = 600
            self.default_wait_for_binary_proto = 900
        else:
            self.default_wait_other_notice_timeout = 120
            self.default_wait_for_binary_proto = 420

        self.force_wait_for_cluster_start = force_wait_for_cluster_start

        # Scylla Manager, when the test asked for one.  skip_manager_server
        # keeps the per-node agents but leaves the server to another cluster's
        # manager (see the secondary_cluster fixture).
        self.skip_manager_server = skip_manager_server
        self._scylla_manager = ScyllaManager(self, manager_install_dir) if manager_install_dir else None

    @property
    def current_scylla_exe(self) -> str:
        """The executable of the build under test, as the test runner resolved it.

        The path depends on --exe-path and the build mode, so the cluster manager
        is the only place that knows it; scylla_repository leaves it unset.
        """
        return str(self.manager.cluster.scylla_exe)

    def exe_for(self, install: scylla_repository.ScyllaInstall) -> str:
        return self.current_scylla_exe if install.is_current else str(install.exe)

    def version_description(self,
                            install: scylla_repository.ScyllaInstall | None = None) -> ScyllaVersionDescription:
        """How the cluster manager should start a node on this version.

        A released package gets the plain command line: SCYLLA_CMDLINE_OPTIONS is
        the 2025.1 baseline (see test/pylib/scylla_server.py), so it is what every
        version taking part in an upgrade understands.  Only the build under test
        adds options of its own, which older binaries would refuse to boot with.
        """
        install = self._install if install is None else install
        if install.is_current:
            return get_current_version_description(self.current_scylla_exe)
        return ScyllaVersionDescription(path=str(install.exe), config={}, argv=[])

    def set_install_dir(self, install_dir: str) -> ScyllaCluster:
        """Run nodes added from now on on the Scylla in this install dir.

        ccm's Cluster.set_install_dir(); upgrade tests call it (through
        UpgradeTester._change_cluster_version) to add a node on an older version
        than the rest of the cluster, or the other way round.
        """
        self._install = scylla_repository.install_for_dir(install_dir)
        self.debug(f"Cluster install dir is now {install_dir} (Scylla {self._install.version})")
        return self

    def upgrade_cluster(self, upgrade_version: str) -> None:
        """Upgrade every node to `upgrade_version`, one at a time."""
        for node in self.nodelist():
            node.upgrade(upgrade_to_version=upgrade_version)
        self._install = scylla_repository.install(upgrade_version)

    def get_path(self) -> str:
        """Return this cluster's own directory, beside the servers' work dirs.

        The in-tree cluster shim keeps nothing here itself -- every server has
        its own work dir under the suite log dir -- but Scylla Manager needs a
        per-cluster place for its config, binaries and log.
        """

        path = Path(self.manager.cluster.vardir) / f"dtest-cluster-{self.manager.cluster.name}"
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    def _add_nodes(self, servers: list) -> None:
        """Create ScyllaNode instances for the given servers and cache them."""
        for server in servers:
            name = f"node{self._next_node_num}"
            self._next_node_num += 1
            self._nodes[name] = ScyllaNode(cluster=self, server=server, name=name)

    @property
    def nodes(self) -> dict[str, ScyllaNode]:
        """The nodes by name.  Live, as ccm's is: a test that removes a node from
        the cluster drops it from here (see e.g. the topology-during-upgrade
        tests, which removenode() an old-version node and then forget it)."""
        return self._nodes

    def nodelist(self) -> list[ScyllaNode]:
        return list(self._nodes.values())

    def get_node_ip(self, nodeid: int) -> str:
        return self.nodelist()[nodeid-1].address()

    def repair(self, options: list[str] | None = None) -> None:
        """Repair every live node in the cluster (see ScyllaNode.repair)."""

        for node in self.nodelist():
            node.repair(options)

    def cleanup(self) -> None:
        """Clean up every live node in the cluster (see ScyllaNode.cleanup)."""

        for node in self.nodelist():
            node.cleanup()

    def compact(self, keyspace: str = "", tables: tuple | list = ()) -> None:
        """Compact every live node in the cluster (see ScyllaNode.compact)."""

        for node in self.nodelist():
            if node.is_running():
                node.compact(keyspace, tables)

    def wait_for_compactions(self,
                             keyspace: str = "",
                             column_family: str = "",
                             timeout: float = 300,
                             quiesce_time: float = 0.5) -> None:
        """Wait out the compactions on every live node (see ScyllaNode.wait_for_compactions)."""

        for node in self.nodelist():
            if node.is_running():
                node.wait_for_compactions(keyspace=keyspace,
                                          column_family=column_family,
                                          timeout=timeout,
                                          quiesce_time=quiesce_time)

    def balanced_tokens(self, node_count: int) -> list[str]:
        """Return `node_count` initial tokens, evenly spaced across Scylla's murmur3 ring."""

        return [str(((2**64 // node_count) * i) - 2**63) for i in range(node_count)]

    def add_seed(self, node: ScyllaNode | str) -> None:
        """Record a node (or address) as a seed, as ccm's Cluster.add_seed did."""

        address = node.address() if isinstance(node, ScyllaNode) else node
        if address not in self.seeds:
            self.seeds.append(address)

    def get_seeds(self) -> list[str]:
        """The recorded seed addresses (ccm's Cluster.get_seeds)."""

        return [s.address() if isinstance(s, ScyllaNode) else s for s in self.seeds]

    def set_partitioner(self, partitioner: str) -> ScyllaCluster:
        """Accept ccm's partitioner choice; there is nothing to configure here.

        ccm wrote a `partitioner:` key into scylla.yaml.  In this tree the
        option already defaults to Murmur3Partitioner (db/config.cc) and
        enable_deprecated_partitioners defaults to false, so Murmur3 is the
        only partitioner a node accepts.  Assert rather than silently ignore,
        so a caller asking for anything else fails loudly.
        """

        assert partitioner == "org.apache.cassandra.dht.Murmur3Partitioner", \
            f"scylla only supports Murmur3Partitioner, not {partitioner!r}"
        return self

    def _node_placements(self, nodes: int | list[int] | dict) -> list[dict[str, str]]:
        """Flatten a populate() topology into one {"dc", "rack"} per node.

        The order matches the order populate()'s batched path creates nodes in,
        which is the order ccm handed out initial tokens in.
        """

        match nodes:
            case int():
                return [{"dc": "dc1", "rack": f"rack{i + 1}"} for i in range(nodes)]
            case list():
                return [{"dc": f"dc{dc}", "rack": f"rack{i + 1}"}
                        for dc, n_nodes in enumerate(nodes, start=1)
                        for i in range(n_nodes)]
            case dict():
                placements = []
                for dc, dc_nodes in nodes.items():
                    if not isinstance(dc_nodes, dict):
                        raise RuntimeError(f"Unsupported topology specification: {nodes}")
                    for rack, rack_nodes in dc_nodes.items():
                        if not isinstance(rack_nodes, int):
                            raise RuntimeError(f"Unsupported topology specification: {nodes}")
                        placements += [{"dc": dc, "rack": rack} for _ in range(rack_nodes)]
                return placements
            case _:
                raise RuntimeError(f"Unsupported topology specification: {nodes}")

    def new_node(self,  # noqa: PLR0913
                 i: int,  # not used here: node names follow the order nodes are created
                 auto_bootstrap: bool = False,
                 debug: bool | None = None,  # not used in scylla-dtest
                 initial_token: str | None = None,
                 add_node: bool = True,
                 is_seed: bool = True,
                 data_center: str | None = None,
                 rack: str | None = None) -> ScyllaNode:
        """Add one more node to the cluster, stopped, and return it."""

        assert debug is None or not debug, "argument `debug` is not supported"
        assert add_node, "creating a node without adding it to the cluster is not supported"
        assert initial_token is None, "argument `initial_token` is not supported"

        if data_center and rack:
            placement = {"property_file": {"dc": data_center, "rack": rack}}
        else:
            # Without an explicit rack, let the harness pick one, in the given
            # datacenter or in the one the cluster already lives in.
            placement = {"auto_rack_dc": data_center or (self.nodelist()[0].data_center if self._nodes else "dc1")}

        self._add_nodes(self.manager.servers_add(
            servers_num=1,
            config=self._config_options,
            start=False,
            **placement,
        ))
        return self.nodelist()[-1]

    def populate(self, nodes: int | list[int], tokens: list[str] | None = None) -> ScyllaCluster:
        if self._config_options.get("alternator_enforce_authorization"):
            self.manager.auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        version = self.version_description()

        if tokens is not None:
            # ccm gave node i the token tokens[i-1] as its initial_token
            # (ccmlib/cluster.py populate()).  servers_add() applies one config
            # to a whole batch, so a token per node means adding them one by one.
            placements = self._node_placements(nodes)
            assert len(tokens) == len(placements), \
                f"populate() got {len(tokens)} tokens for {len(placements)} nodes"
            for placement, token in zip(placements, tokens, strict=True):
                self._add_nodes([self.manager.server_add(
                    config=self._config_options | {"initial_token": token},
                    version=version,
                    property_file=placement,
                    start=False,
                )])
            return self

        match nodes:
            case int():
                self._add_nodes(self.manager.servers_add(servers_num=nodes, config=self._config_options, version=version, start=False, auto_rack_dc="dc1"))
            case list():
                for dc, n_nodes in enumerate(nodes, start=1):
                    dc_name = f"dc{dc}"
                    self._add_nodes(self.manager.servers_add(
                        servers_num=n_nodes,
                        config=self._config_options,
                        version=version,
                        start=False,
                        auto_rack_dc=dc_name
                    ))
            case dict():
                # Supported spec: {"dc1": {"rack1": 3, "rack2": 2}, "dc2": {"rack1": 2}}
                for dc, dc_nodes in nodes.items():
                    if not isinstance(dc_nodes, dict):
                        raise RuntimeError(f"Unsupported topology specification: {nodes}")
                    for rack, rack_nodes in dc_nodes.items():
                        if not isinstance(rack_nodes, int):
                            raise RuntimeError(f"Unsupported topology specification: {nodes}")
                        self._add_nodes(self.manager.servers_add(
                            servers_num=rack_nodes,
                            config=self._config_options,
                            version=version,
                            property_file={
                                "dc": dc,
                                "rack": rack,
                            },
                            start=False,
                        ))
            case _:
                raise RuntimeError(f"Unsupported topology specification: {nodes}")

        return self

    def start_nodes(self,
                    nodes: list[ScyllaNode] | None = None,
                    no_wait: bool = False,
                    verbose: bool | None = None,  # not used in scylla-dtest
                    wait_for_binary_proto: bool | None = None,
                    wait_other_notice: bool | None = None,
                    wait_normal_token_owner: bool | None = None,
                    jvm_args: list[str] | None = None,
                    profile_options: dict[str, str] | None = None,  # not used in scylla-dtest
                    quiet_start: bool | None = None) -> list[ScyllaNode]:  # not used in scylla-dtest
        assert verbose is None, "argument `verbose` is not supported"
        assert profile_options is None, "argument `profile_options` is not supported"
        assert quiet_start is None, "argument `quiet_start` is not supported"

        self.debug(
            f"start_nodes: no_wait={no_wait} wait_for_binary_proto={wait_for_binary_proto}"
            f" wait_other_notice={wait_other_notice} wait_normal_token_owner={wait_normal_token_owner}"
            f" force_wait_for_cluster_start={self.force_wait_for_cluster_start}"
        )

        if nodes is None:
            nodes = self.nodelist()
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]
        started = []

        for node in nodes:
            if not node.is_running():
                node.start(
                    no_wait=no_wait,
                    wait_other_notice=wait_other_notice,
                    wait_normal_token_owner=wait_normal_token_owner,
                    wait_for_binary_proto=wait_for_binary_proto,
                    jvm_args=jvm_args,
                )
                started.append(node)

        return started

    def start(self,
              no_wait: bool = False,
              verbose: bool | None = None,  # not used in scylla-dtest
              wait_for_binary_proto: bool | None = None,
              wait_other_notice: bool | None = None,
              wait_normal_token_owner: bool | None = None,
              jvm_args: list[str] | None = None,
              profile_options: dict[str, str] | None = None,  # not used in scylla-dtest
              quiet_start: bool | None = None) -> list[ScyllaNode]:  # not used in scylla-dtest
        assert verbose is None, "argument `verbose` is not supported"
        assert profile_options is None, "argument `profile_options` is not supported"
        assert quiet_start is None, "argument `quiet_start` is not supported"

        started = self.start_nodes(
            no_wait=no_wait,
            wait_for_binary_proto=wait_for_binary_proto,
            wait_other_notice=wait_other_notice,
            wait_normal_token_owner=wait_normal_token_owner,
            jvm_args=jvm_args,
        )

        # The manager keeps its metadata in this cluster, so it can only start
        # once the nodes serve CQL.
        self.start_scylla_manager()

        return started

    def stop_nodes(self,
                   nodes: list[ScyllaNode] | None = None,
                   wait: bool = True,
                   gently: bool = True,
                   wait_other_notice: bool = False,
                   other_nodes: list[ScyllaNode] | None = None,
                   wait_seconds: int | None = None) -> list[ScyllaNode]:
        if nodes is None:
            nodes = self.nodelist()
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]

        for node in nodes:
            node.stop(
                wait=wait,
                wait_other_notice=wait_other_notice,
                other_nodes=other_nodes,
                gently=gently,
                wait_seconds=wait_seconds,
            )

        return [node for node in nodes if not node.is_running()]

    def stop(self,
             wait: bool = True,
             gently: bool = True,
             wait_other_notice: bool = False,
             other_nodes: list[ScyllaNode] | None = None,
             wait_seconds: int | None = None) -> list[ScyllaNode]:
        self.stop_scylla_manager(gently=gently)
        return self.stop_nodes(
            wait=wait,
            gently=gently,
            wait_other_notice=wait_other_notice,
            other_nodes=other_nodes,
            wait_seconds=wait_seconds,
        )

    def remove(self,
               node: ScyllaNode | None = None,
               wait_other_notice: bool = False,
               other_nodes: list[ScyllaNode] | None = None,
               remove_node_dir: bool = True) -> None:
        """Kill a node and drop it from the cluster, or kill the whole cluster.

        Mirrors scylla-ccm's Cluster.remove(): the node is killed, not
        decommissioned, so the rest of the ring still knows about it and
        reports it as down.  `remove_node_dir` also deletes its work
        directory, which is what leaves a node looking wiped to a later
        replace; the replace tests pass False to keep it.
        """

        if node is None:
            node_path = self.get_path()
            self.stop(gently=False, wait_other_notice=wait_other_notice, other_nodes=other_nodes)
        else:
            # _nodes is keyed by node name, as ccm's Cluster.nodes was.
            if node.name not in self._nodes:
                return
            node_path = node.get_path()
            del self._nodes[node.name]
            node.stop(gently=False, wait_other_notice=wait_other_notice, other_nodes=other_nodes)

        if remove_node_dir:
            self.remove_dir_with_retry(node_path)

    def sctool(self, cmd: list[str]) -> tuple[str, str]:
        if self._scylla_manager is None:
            raise RuntimeError("scylla manager not enabled - sctool command cannot be executed")
        return self._scylla_manager.sctool(cmd)

    def start_scylla_manager(self) -> None:
        if not self._scylla_manager or self.skip_manager_server:
            return
        self._scylla_manager.start()

    def stop_scylla_manager(self, gently: bool = True) -> None:
        if not self._scylla_manager or self.skip_manager_server:
            return
        self._scylla_manager.stop(gently)

    def stress(self, stress_options: list[str], **kwargs):
        """Run `cassandra-stress` against the first running node."""

        for node in self.nodelist():
            if node.is_running():
                return node.stress(stress_options, **kwargs)
        raise RuntimeError("no running node to run cassandra-stress against")

    def nodetool(self, nodetool_cmd: str) -> ScyllaCluster:
        for node in self.nodelist():
            if node.is_running():
                node.nodetool(nodetool_cmd)
        return self

    def version(self) -> str:
        """The Scylla version nodes added right now would run."""
        return self._install.version

    def set_configuration_options(self,
                                  values: dict[str, Any] | None = None,
                                  batch_commitlog: bool | None = None,
                                  nodes: ScyllaNode | list[ScyllaNode] | None = None) -> ScyllaCluster:
        values = {} if values is None else values.copy()
        if batch_commitlog is not None:
            if batch_commitlog:
                values["commitlog_sync"] = "batch"
                values["commitlog_sync_batch_window_in_ms"] = 5
                values["commitlog_sync_period_in_ms"] = None
            else:
                values["commitlog_sync"] = "periodic"
                values["commitlog_sync_period_in_ms"] = 10000
                values["commitlog_sync_batch_window_in_ms"] = None
        if values:
            if nodes is None:
                self._config_options.update(values)  # keep values as a cluster config for new nodes
                nodes = self.nodelist()
            elif isinstance(nodes, ScyllaNode):
                nodes = [nodes]
            for node in nodes:
                self.manager.server_update_config(server_id=node.server_id, config_options=values)
        return self

    def enable_internode_ssl(self, node_ssl_path: str, internode_encryption: str = "all") -> ScyllaCluster:
        """Configure server_encryption_options from certs generated by tools.misc.generate_ssl_stores().

        Mirrors scylla-ccm's Cluster.enable_internode_ssl() (ccmlib/cluster.py), adapted to
        Scylla's server_encryption_options schema (certificate/keyfile/truststore, see
        conf/scylla.yaml) rather than the Cassandra-only jks keystore options.
        """
        node_ssl_options = {
            "internode_encryption": internode_encryption,
            "certificate": os.path.join(node_ssl_path, "ccm_node.pem"),
            "keyfile": os.path.join(node_ssl_path, "ccm_node.key"),
            "truststore": os.path.join(node_ssl_path, "ccm_node.cer"),
        }
        return self.set_configuration_options(values={"server_encryption_options": node_ssl_options})

    def flush(self) -> None:
        for node in self.nodelist():
            if node.is_running():
                node.flush()

    @staticmethod
    def remove_dir_with_retry(path: str) -> None:
        """Remove a file or directory."""
        import shutil
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)

    @staticmethod
    def debug(message: str) -> None:
        logger.debug(message)

    @staticmethod
    def info(message: str) -> None:
        logger.info(message)

    @staticmethod
    def warning(message: str) -> None:
        logger.warning(message)

    @staticmethod
    def error(message: str) -> None:
        logger.error(message)
