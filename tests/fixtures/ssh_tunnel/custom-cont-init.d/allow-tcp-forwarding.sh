#!/usr/bin/with-contenv bash
# linuxserver/openssh-server disables TCP forwarding by default; the ssh_tunnel
# tests need it enabled to reach the network-isolated ClickHouse target through
# this bastion. custom-cont-init.d scripts run before sshd starts, so no
# restart is needed (unlike patching this by hand after the container is up).
sed -i 's/AllowTcpForwarding no/AllowTcpForwarding yes/' /config/sshd/sshd_config
