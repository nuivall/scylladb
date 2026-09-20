import logging

from docker.errors import DockerException
from ldap3 import ALL, ALL_ATTRIBUTES, Connection, Server
from ldap3.core.exceptions import LDAPSocketOpenError

from tools.docker_utils import ContainerNotRunningError, container_exec_run, container_reload, container_remove, dump_container_logs, get_docker_client, get_ip_address_of_container, running_in_docker
from tools.retrying import retrying

logger = logging.getLogger(__name__)


class ContainerAlreadyStartedError(Exception):
    pass


class ContainerDoesNotExistError(Exception):
    pass


class LdapConnectionAlreadyStartedError(Exception):
    pass


class LdapConnectionDoesNotExistError(Exception):
    pass


class LdapServerNotReadyError(Exception):
    pass


# If the server has terminated the connection lets try to rebuild it
# once.
def dump_ldap_log_on_failure(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            try:
                logger.debug(f"LDAP SERVER LOG DUMP: {args[0].container.logs().decode('utf-8')}")
            except Exception as log_err:  # noqa: BLE001
                logger.debug(f"Failed to dump LDAP logs: {log_err}")
            raise

    return inner


class LdapDocker:
    def __init__(self):
        self.docker = get_docker_client()
        self.name = None
        self.ldap_port = None
        self.ldap_ssl_port = None
        self.container = None
        self.conn = None
        self.ldap_server = None
        self.ldap_base_object = None
        self.ldap_address = None

    def create_ldap_container(  # noqa: PLR0913
        self,
        name,
        ldap_port=None,
        ldap_ssl_port=None,
        image="osixia/openldap:1.4.0",
        organisation="ScyllaDB",
        domain="scylladb.com",
        password="scylla",
    ):
        if self.container:
            raise ContainerAlreadyStartedError("LDAP docker already exists for this instance")
        self.name = name

        ports = None if running_in_docker() else {"389/tcp": ("0.0.0.0", ldap_port), "636/tcp": ("0.0.0.0", ldap_ssl_port)}

        @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException)
        def start_ldap():
            existing_images = self.docker.images.list(filters={"reference": image})
            if not existing_images:
                self.docker.images.pull(image)

            try:
                self.container = self.docker.containers.run(ports=ports, name=name, environment=[f"LDAP_ORGANISATION={organisation}", f"LDAP_DOMAIN={domain}", f"LDAP_ADMIN_PASSWORD={password}"], image=image, detach=True, labels=["dtest"])
            except DockerException as e:
                logger.error(f"Failed to start LDAP container: {e}")
                self.docker.containers.get(name).remove(force=True)
                raise

        # osixia/openldap's slapd process occasionally crashes during its cold-start
        # bootstrap under CI host contention (DTEST-242), leaving the container "exited".
        # A dead container can't recover on its own, so recreate a fresh one and retry
        # the startup wait instead of failing the caller on the first crash.
        @retrying(num_attempts=3, sleep_time=1, allowed_exceptions=ContainerNotRunningError)
        def start_and_wait_for_ldap():
            start_ldap()
            container_reload(self.container)
            if running_in_docker():
                self.ldap_port = "389"
                self.ldap_ssl_port = "636"
                self.ldap_address = get_ip_address_of_container(self.container)
            else:
                self.ldap_port = self.container.ports["389/tcp"][0]["HostPort"]
                self.ldap_ssl_port = self.container.ports["636/tcp"][0]["HostPort"]
                self.ldap_address = "localhost"
            try:
                # We try to wait here for the startup, if the server haven't finished
                # after 30s we will continue with the wishfull thinking that by the time
                # scylla will try to connect to it, it will be up and running.
                # if it will not happen, the test will fail and we will need to increase
                # the timeout. But it is better than failing early.
                # for creating connections to the server we are covered since the connection
                # creation function also waits for the server to be up.
                self.wait_for_ldap_server_startup()
            except ContainerNotRunningError:
                container_remove(self.container, force=True)
                self.container = None
                raise

        start_and_wait_for_ldap()

    def is_container_running(self):
        if not self.container:
            raise ContainerDoesNotExistError("LDAP docker does not exists for this instance")
        return "running" in self.container.status

    def remove_container(self, force=True):
        if not self.container:
            raise ContainerDoesNotExistError("LDAP docker does not exists for this instance")
        if self.conn:
            self.disconnect_ldap()
        container_remove(self.container, force=force)
        self.container = None

    @retrying(num_attempts=5, sleep_time=1, allowed_exceptions=LdapServerNotReadyError)
    def wait_for_ldap_server_startup(self, timeout=30):
        container_reload(self.container)
        if self.container.status != "running":
            dump_container_logs(self.container, tail=500)
            raise ContainerNotRunningError(f"LDAP container {self.name} died during startup (status: {self.container.status}). Logs dumped above.")
        if container_exec_run(self.container, f"timeout {timeout}s container/tool/wait-process")[0] != 0:
            raise LdapServerNotReadyError("LDAP server didn't finish its startup yet...")

    @retrying(num_attempts=5, sleep_time=2, allowed_exceptions=(LDAPSocketOpenError, LdapServerNotReadyError), message="Trying to create LDAP connection")
    def create_ldap_connection(self, user="cn=admin,dc=scylladb,dc=com", password="scylla"):
        self.wait_for_ldap_server_startup(5)
        if not self.ldap_server:
            self.ldap_server = Server(host=f"ldap://{self.ldap_address}:{self.ldap_port}", get_info=ALL)
        if not self.conn:
            self.conn = Connection(server=self.ldap_server, user=user, password=password)
        self.conn.bind()
        self.ldap_base_object = self.ldap_server.info.naming_contexts[0]

    def is_ldap_connection_bound(self):
        return self.conn.bound

    def disconnect_ldap(self):
        if not self.conn:
            raise LdapConnectionDoesNotExistError("LDAP connection does not exist for this instance")
        self.conn.unbind()
        self.conn = None

    @dump_ldap_log_on_failure
    def add_ldap_object(self, *args, **kwargs):
        self.conn.add(*args, **kwargs)
        return self.conn.result

    @dump_ldap_log_on_failure
    def delete_ldap_object(self, *args, **kwargs):
        self.conn.delete(*args, **kwargs)
        return self.conn.result

    @dump_ldap_log_on_failure
    def search_ldap_object(self, search_base, search_filter):
        self.conn.search(search_base=search_base, search_filter=search_filter, attributes=ALL_ATTRIBUTES)
        return self.conn.entries

    @dump_ldap_log_on_failure
    def modify_ldap_object(self, *args, **kwargs):
        return self.conn.modify(*args, **kwargs)
