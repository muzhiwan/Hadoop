#!/bin/bash


log="/root/logs/collector.log"

rootDir="/Data/webapps/stat.anquanxia.com/logs/100001/"

destDir="/root/logs/100001/"
hdfsDir="/apilogs/src/100001"

echo "*************  start time : `date +"%F %T"` ******************" >> ${log}

starttime=`date +"%s"`

mkdir -p "${destDir}"
hadoop fs -mkdir "${hdfsDir}"

year=`date +"%Y"`
month=`date +"%m"`
day=`date -d yesterday +"%d"`

logDir="${rootDir}${year}/${month}/${day}/"
tmplog="${destDir}${year}${month}${day}.txt"
rm -f "${tmplog}"

find "${logDir}" -name "*.txt" -type f -exec cat >> "${tmplog}" {} \;

hadoop fs -rm "${hdfsDir}/${year}${month}${day}.txt"
hadoop fs -put "${tmplog}" "${hdfsDir}"

endtime=`date +"%s"`
let "cost=endtime-starttime"

echo "cost time : ${cost}s" >> ${log}

echo "*************  end time : `date +"%F %T"` ******************" >> ${log}


