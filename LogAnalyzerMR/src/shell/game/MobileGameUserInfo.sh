#!/bin/bash

srcDir="/apilogs/src/100001"
destDir="/user/root/out/MobileGameUserInfo"

hadoop fs -mkdir "${destDir}"

starttime=`date +"%s"`
outputPath="${destDir}/${starttime}"

hadoop jar /root/LogAnalyzerMR.jar com.muzhiwan.hadoop.LogAnalyzerMR MobileGAMEUserInfo "${srcDir}" "${outputPath}"

hive -e "
	drop table game_user_info;
	create table if not exists game_user_info (
		CELL_PHONE_DEVICE_ID string,
		PACKAGE_NAME string,
		VERSION_CODE string,
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

	Load Data Inpath '${outputPath}/part-r-00000' Overwrite Into Table game_user_info;
	
	drop table game_new_user_stat;
	create table if not exists game_new_user_stat (
		stat_day string,
		PACKAGE_NAME string,
		VERSION_CODE string,
		user_count bigint
	)
	Row Format Delimited
	Fields Terminated By ','
	stored as textfile;

	insert overwrite table game_new_user_stat select from_unixtime(floor(first_time/1000),'yyyyMMdd') as stat_day,PACKAGE_NAME,VERSION_CODE,count(DISTINCT CELL_PHONE_DEVICE_ID ) as user_count from game_user_info group by from_unixtime(floor(first_time/1000),'yyyyMMdd'),PACKAGE_NAME,VERSION_CODE;

"

