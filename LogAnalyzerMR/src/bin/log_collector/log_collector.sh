#!/bin/bash

log="/root/logs/collector.log"
eventId="$1"
year=`date +"%Y"`
month="$2"
day="$3"

rootDir="/Data/webapps/stat.anquanxia.com/logs/${eventId}/"

#destDir="/root/logs/${eventId}/"
destDir="/root/logs/"
hdfsDir="/apilogs/src/${eventId}"

echo "*************  start time : `date +"%F %T"` ******************" >> ${log}

starttime=`date +"%s"`

hadoop fs -mkdir "${hdfsDir}"


logDir="${rootDir}${year}/${month}/${day}/"
tmplog="${destDir}${year}${month}${day}.txt"
rm -f "${tmplog}"

find "${logDir}" -name "*.txt" -type f -exec cat >> "${tmplog}" {} \;

hadoop fs -rm "${hdfsDir}/${year}${month}${day}.txt"
hadoop fs -put "${tmplog}" "${hdfsDir}"

rm -f "${tmplog}"

endtime=`date +"%s"`
let "cost=endtime-starttime"

echo "cost time : ${cost}s" >> ${log}

echo "*************  end time : `date +"%F %T"` ******************" >> ${log}


