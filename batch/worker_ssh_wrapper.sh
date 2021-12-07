#!/bin/bash

echo "Port 2222" >> /etc/ssh/sshd_config
echo "ListenAddress 0.0.0.0" >> /etc/ssh/sshd_config
service ssh start
/usr/sbin/sshd -D

echo "running python worker"
python3 -u -m batch.worker.worker
