#!/bin/bash
#
# Use this script to bootstrap an OpenStack test environment.

HOSTS=(weatherapi1 weatherapi2 weatherapi3 wdb1 wdb2 wdb3 modelstatus1 modelstatus2)

if [ -z "$OS_PASSWORD" ]; then
    echo "Please source your OpenStack configuration file."
    exit 1
fi

# 4 GB RAM, 2 VCPU, 40 GB disk
[ -z "$FLAVOR" ] && FLAVOR=3

# Ubuntu 12.04
[ -z "$IMAGE" ] && IMAGE=c1fa6cf0-d199-4264-98c6-38892387a077

# SSH key for "ubuntu" user
[ -z "$KEY" ] && KEY=$OS_USERNAME

IPS=(`nova floating-ip-list | grep -E "\|\s+-\s+\|" | awk '{print $2}'`)
NUM_IPS=${#IPS[@]}
NUM_HOSTS=${#HOSTS[@]}

if [ $NUM_IPS -lt $NUM_HOSTS ]; then
    echo "There are not enough floating IP addresses available. required=$NUM_HOSTS, available=$NUM_IPS"
    exit 1
fi

set -ex

for ((i=0; i < $NUM_HOSTS; ++i)); do
    nova boot --poll --flavor $FLAVOR --key_name $KEY --image $IMAGE ${HOSTS[i]}
    nova floating-ip-associate ${HOSTS[i]} ${IPS[i]}
done

echo "Done, these are the created hosts and their IP addresses:"
echo
for ((i=0; i < $NUM_HOSTS; ++i)); do
    echo "${HOSTS[i]} ${IPS[i]}"
done
echo
