#!/bin/bash


destDir="/home/hdfs/shell/"

log="${destDir}collector.log"

eventId="$1"
year=`date +"%Y"`
month="$2"
day="$3"

rootDir="/tjlogs/${eventId}/"
hdfsDir="/apilogs/src/${eventId}"

echo "*************  start time : `date +"%F %T"` ******************" >> ${log}

starttime=`date +"%s"`

sudo -u hdfs hadoop fs -mkdir "${hdfsDir}"


logDir="${rootDir}${year}/${month}/${day}/"
tmplog="${destDir}${year}${month}${day}.txt"
rm -f "${tmplog}"

find "${logDir}" -name "*.txt" -type f -exec cat >> "${tmplog}" {} \;

sudo -u hdfs hadoop fs -rm "${hdfsDir}/${year}${month}${day}.txt"
sudo -u hdfs hadoop fs -put "${tmplog}" "${hdfsDir}"

rm -f "${tmplog}"

endtime=`date +"%s"`
let "cost=endtime-starttime"

echo "cost time : ${cost}s" >> ${log}

echo "*************  end time : `date +"%F %T"` ******************" >> ${log}


