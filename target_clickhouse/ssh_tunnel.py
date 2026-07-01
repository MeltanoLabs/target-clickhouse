"""SSH tunnel support for connecting to ClickHouse behind a bastion host.

Ported from MeltanoLabs/tap-postgres's SSHTunnelForwarder, which implements the
same `ssh_tunnel.*` settings convention (enable/host/username/port/private_key/
private_key_password) shared across MeltanoLabs connectors.
"""

from __future__ import annotations

import io
import socket
import threading
from contextlib import suppress
from typing import Any

import paramiko


class SSHTunnelForwarder:
    """SSH Tunnel forwarder using paramiko.

    This class provides SSH tunnel functionality similar to the `sshtunnel`
    package, but implemented directly with paramiko.
    """

    def __init__(
        self,
        ssh_address_or_host: tuple[str, int],
        ssh_username: str,
        ssh_pkey: paramiko.PKey,
        ssh_private_key_password: str | None,
        remote_bind_address: tuple[str, int],
    ) -> None:
        """Initialize SSH tunnel forwarder.

        Args:
            ssh_address_or_host: Tuple of (ssh_host, ssh_port)
            ssh_username: SSH username
            ssh_pkey: Paramiko private key object
            ssh_private_key_password: Private key password (optional)
            remote_bind_address: Tuple of (remote_host, remote_port)

        """
        self.ssh_host, self.ssh_port = ssh_address_or_host
        self.ssh_username = ssh_username
        self.ssh_pkey = ssh_pkey
        self.ssh_private_key_password = ssh_private_key_password
        self.remote_bind_host, self.remote_bind_port = remote_bind_address

        self.ssh_client: paramiko.SSHClient | None = None
        self.local_bind_host = "127.0.0.1"
        self.local_bind_port: int | None = None
        self._server_socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the SSH tunnel."""
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.ssh_client.connect(
            hostname=self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_username,
            pkey=self.ssh_pkey,
            passphrase=self.ssh_private_key_password,
        )

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.local_bind_host, 0))
        self._server_socket.listen(5)

        self.local_bind_port = self._server_socket.getsockname()[1]

        self._thread = threading.Thread(target=self._forward_tunnel, daemon=True)
        self._thread.start()

    def _forward_tunnel(self) -> None:
        """Forward connections through the SSH tunnel."""
        if self._server_socket is None or self.ssh_client is None:
            return

        while not self._stop_event.is_set():
            try:
                self._server_socket.settimeout(1.0)
                try:
                    local_socket, _ = self._server_socket.accept()
                except TimeoutError:
                    continue

                transport = self.ssh_client.get_transport()
                if transport is None:
                    local_socket.close()
                    continue

                try:
                    channel = transport.open_channel(
                        "direct-tcpip",
                        (self.remote_bind_host, self.remote_bind_port),
                        local_socket.getpeername(),
                    )
                except paramiko.SSHException:
                    # A single failed channel (e.g. a transient bastion
                    # hiccup, or forwarding briefly disallowed) shouldn't
                    # kill the whole tunnel -- close this connection attempt
                    # and keep accepting new ones.
                    local_socket.close()
                    continue

                threading.Thread(
                    target=self._forward_data,
                    args=(local_socket, channel),
                    daemon=True,
                ).start()
            except OSError:
                if not self._stop_event.is_set():
                    break

    def _forward_data(
        self,
        local_socket: socket.socket,
        channel: paramiko.Channel,
    ) -> None:
        """Forward data between local socket and SSH channel.

        Args:
            local_socket: Local socket
            channel: SSH channel

        """
        try:

            def forward_local_to_remote() -> None:
                # Exceptions raised here run in their own thread and are not
                # visible to this method's own try/except, so each direction
                # catches its own errors -- expected whenever the tunnel is
                # torn down while a connection is still forwarding data.
                try:
                    while True:
                        data = local_socket.recv(4096)
                        if len(data) == 0:
                            break
                        channel.send(data)
                    channel.close()
                except (OSError, EOFError):
                    pass

            def forward_remote_to_local() -> None:
                try:
                    while True:
                        data = channel.recv(4096)
                        if len(data) == 0:
                            break
                        local_socket.send(data)
                    local_socket.close()
                except (OSError, EOFError):
                    pass

            t1 = threading.Thread(target=forward_local_to_remote, daemon=True)
            t2 = threading.Thread(target=forward_remote_to_local, daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        except OSError:
            pass
        finally:
            with suppress(OSError):
                local_socket.close()
            with suppress(OSError):
                channel.close()

    def stop(self) -> None:
        """Stop the SSH tunnel."""
        self._stop_event.set()

        if self._server_socket:
            with suppress(OSError):
                self._server_socket.close()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

        if self.ssh_client:
            self.ssh_client.close()


def guess_key_type(key_data: str) -> paramiko.PKey:
    """Guess the type of a private key.

    Note: DSS keys are not supported as they were removed in paramiko 4.0
    due to being cryptographically weak.

    Args:
        key_data: The private key data to guess the type of.

    Returns:
        The private key object.

    Raises:
        ValueError: If the key type could not be determined.

    """
    for key_class in (
        paramiko.RSAKey,
        paramiko.ECDSAKey,
        paramiko.Ed25519Key,
    ):
        try:
            return key_class.from_private_key(io.StringIO(key_data))
        except paramiko.SSHException:  # noqa: PERF203
            continue

    msg = (
        "Could not determine the key type. Supported key types are RSA, ECDSA, "
        "and Ed25519. DSS keys are not supported as they were removed in "
        "paramiko 4.0 due to being cryptographically weak."
    )
    raise ValueError(msg)


def start_tunnel_if_enabled(config: dict[str, Any]) -> SSHTunnelForwarder | None:
    """Start an SSH tunnel to `config["host"]:config["port"]` if configured.

    Args:
        config: The connector config. Must contain `host`/`port` and may
            contain an `ssh_tunnel` object with `enable: true`.

    Returns:
        The started SSHTunnelForwarder, or None if `ssh_tunnel.enable` is not set.

    Raises:
        ValueError: If SSH tunneling is enabled but `host`/`port` are unset.

    """
    ssh_config = config.get("ssh_tunnel") or {}
    if not ssh_config.get("enable", False):
        return None

    host = config.get("host")
    port = config.get("port")
    if host is None or port is None:
        msg = "Database host and port must be specified when using SSH tunnel"
        raise ValueError(msg)

    tunnel = SSHTunnelForwarder(
        ssh_address_or_host=(ssh_config["host"], ssh_config["port"]),
        ssh_username=ssh_config["username"],
        ssh_pkey=guess_key_type(ssh_config["private_key"]),
        ssh_private_key_password=ssh_config.get("private_key_password"),
        remote_bind_address=(host, port),
    )
    tunnel.start()
    return tunnel
