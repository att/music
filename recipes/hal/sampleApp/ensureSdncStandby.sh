#!/bin/bash

# query SDN-C cluster status
clusterStatus=$( /opt/sdnc/sdnc.cluster )

if [ "ACTIVE" = "$clusterStatus" ];then
  # assume transient error as other side transitions to ACTIVE
  echo "Cluster is ACTIVE but HAL wants STANDBY! Panic!"
  exit 0

elif [ "STANDBY" = "$clusterStatus" ]; then
  echo "Cluster is standing by"
  exit 0
fi

echo "Unknown cluster status '$clusterStatus'"
exit 1
