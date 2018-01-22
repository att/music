#!/bin/bash

# query SDN-C cluster status
clusterStatus=$( /opt/sdnc/sdnc.cluster )

if [ "ACTIVE" = "$clusterStatus" ];then
  # peform health-check
  health=$( /opt/sdnc/sdnc.monitor )
  
  if [ "HEALTHY" = "$health" ]; then
    echo "Cluster is ACTIVE and HEALTHY"
    exit 0
  fi
  echo "Cluster is ACTIVE and UNHEALTHY"
  exit 1

elif [ "STANDBY" = "$clusterStatus" ]; then
  # perform takeover
  echo "Cluster is STANDBY - taking over"
  takeoverResult=$( /opt/sdnc/sdnc.failover )
  if [ "SUCCESS" = "$takeoverResult" ]; then
    exit 0
  fi
  echo "Cluster takeover failed"
  exit 1
fi

echo "Unknown cluster status '$clusterStatus'"
exit 1
