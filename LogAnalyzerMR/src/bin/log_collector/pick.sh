#!/bin/bash

events=(100001 100002 100003 100004 200001 200002 200003 200004 200005 200006 200007 200008 200009) 
month=`date -d yesterday +"%m"`
day=`date -d yesterday +"%d"`

#for event in "${events[@]}"; do
for event in $(ll /Data/webapps/stat.anquanxia.com/tjlogs | awk '{print $9}'); do
       /root/logs/log_collector.sh "${event}" "${month}" "${day}"
done
