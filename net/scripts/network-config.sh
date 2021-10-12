#!/usr/bin/env bash
set -ex

[[ $(uname) = Linux ]] || exit 1
[[ $USER = root ]] || exit 1

sudo sysctl -w net.core.rmem_default=134217728
sudo sysctl -w net.core.rmem_max=134217728

sudo sysctl -w net.core.wmem_default=134217728
sudo sysctl -w net.core.wmem_max=134217728

echo "MaxAuthTries 60" | sudo tee -a /etc/ssh/sshd_config
# sudo ln -sTf /lib/systemd/system/ssh.service \
#   /etc/systemd/system/sshd.service
sudo service ssh restart
sudo systemctl restart ssh
