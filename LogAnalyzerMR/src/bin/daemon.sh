#!/bin/bash

cd /root
WORKING_DIR=`pwd`
echo "${WORKING_DIR}"

pids=`jps | grep ThriftServer | awk '{ print $1 }'`
pid=${pids[0]}

if [ -z $pid ];then
    echo "ThriftSerfer has dead"
    nohup hbase thrift start &
fi


