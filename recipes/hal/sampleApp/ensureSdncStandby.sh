#!/bin/bash

# query SDN-C cluster status
clusterStatus=$( /opt/sdnc/sdnc.cluster )

if [ "ACTIVE" = "$clusterStatus" ];then
  # check that standby cluster is healthy
  health=$( /opt/sdnc/sdnc.monitor )
  if [ "FAILURE" = "$health" ];then
    echo "Backup site is unhealthy - can't accept traffic!"
    exit 1
  fi

  # assume transient error as other side transitions to ACTIVE
  echo "Cluster is ACTIVE but HAL wants STANDBY! Panic!"
  exit 0

elif [ "STANDBY" = "$clusterStatus" ]; then
  echo "Cluster is standing by"
  exit 0
fi

echo "Unknown cluster status '$clusterStatus'"
exit 1
