#!/bin/bash


srcDir="/apilogs/src/100001"
destDir="/user/root/out/MobileSDKUserInfo"

hadoop fs -mkdir "${destDir}"

starttime=`date +"%s"`
outputPath="${destDir}/${starttime}"

hadoop jar /root/LogAnalyzerMR.jar com.muzhiwan.hadoop.LogAnalyzerMR MobileSDKUserInfo "${srcDir}" "${outputPath}"

hive -e "
	drop table sdk_user_info;
	create table if not exists sdk_user_info (
		CELL_PHONE_DEVICE_ID string,
		first_time bigint,
		CELL_PHONE_BRAND string,
		CELL_PHONE_MODEL string,
		CELL_PHONE_CPU string,
		CELL_PHONE_DENSITY string,
		CELL_PHONE_SCREEN_WIDTH string,
		CELL_PHONE_SCREEN_HEIGHT string
	)
	Row Format Delimited
	Fields Terminated By '\t'
	stored as textfile;

	Load Data Inpath '${outputPath}/part-r-00000' Overwrite Into Table sdk_user_info;
	
	drop table sdk_new_user_stat;
	create table if not exists sdk_new_user_stat (
		stat_day string,
		new_user_count bigint
	)
	Row Format Delimited
	Fields Terminated By ','
	stored as textfile;

	insert overwrite table sdk_new_user_stat select from_unixtime(floor(first_time/1000),'yyyyMMdd') as stat_day,count(DISTINCT CELL_PHONE_DEVICE_ID ) as new_user_count from sdk_user_info group by from_unixtime(floor(first_time/1000),'yyyyMMdd');

"

