FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates openssh-client openssh-server util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/sh fabric \
    && install -d -m 0700 -o fabric -g fabric /home/fabric/.ssh

COPY machine-fabric /usr/local/bin/machine-fabric
COPY sshd_config /etc/ssh/sshd_config.d/90-machine-fabric.conf
COPY ssh_config /etc/machine-fabric/ssh_config
COPY node-entrypoint.sh /usr/local/bin/node-entrypoint

RUN chmod 0755 /usr/local/bin/machine-fabric /usr/local/bin/node-entrypoint

ENTRYPOINT ["/usr/local/bin/node-entrypoint"]
