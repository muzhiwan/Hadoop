#!/bin/bash

srcDir="/apilogs/src/100001"
destDir="/user/hdfs/out/MobileGameUserInfo"

sudo -u hdfs hadoop fs -mkdir "${destDir}"

starttime=`date +"%s"`
outputPath="${destDir}/${starttime}"

sudo -u hdfs hadoop jar /home/hdfs/LogAnalyzerMR.jar com.muzhiwan.hadoop.LogAnalyzerMR MobileGAMEUserInfo "${srcDir}" "${outputPath}"

sudo -u hdfs hive -e "
    drop table sdk_game_user_info;
    create table if not exists sdk_game_user_info (
        device_id string,
        package string,
        versioncode int,
        first_time bigint,
        versionname string,
        brand string,
        model string,
        cpu string,
        density string,
        screen_width int,
        screen_height int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    Load Data Inpath '${outputPath}/part-r-*' Overwrite Into Table sdk_game_user_info;
    
    drop table sdk_game_new_user_stat_tmp;
    create table if not exists sdk_game_new_user_stat_tmp (
        stat_day string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        versionname string,
        user_count int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_game_new_user_stat_tmp 
    select from_unixtime(floor(first_time/1000),'yyyy-MM-dd') as stat_day,package,versioncode,versionname,count(DISTINCT device_id ) as user_count 
    from sdk_game_user_info 
    group by from_unixtime(floor(first_time/1000),'yyyy-MM-dd'),package,versioncode,versionname;

     drop table sdk_game_new_user_stat;
    create table if not exists sdk_game_new_user_stat(
        day string,
        time int,
        package string,
        versioncode int,
        versionname string,
        total int
    )
    Row Format Delimited 
    Fields Terminated By '\t' 
    stored as textfile;
    
    insert overwrite table sdk_game_new_user_stat 
     SELECT stat_day as day,unix_timestamp(stat_day,'yyyy-MM-dd')  as time,PACKAGE_NAME as package,VERSION_CODE as versioncode,versionname,user_count as total FROM sdk_game_new_user_stat_tmp where user_count>0 order by total desc;
    
    drop table sdk_game_new_user_stat_tmp;
    
    
    create EXTERNAL table if not exists sdk100001 (
        SERVER_TIME bigint,
        CLIENT_IP string,
        CLIENT_AREA string,
        EVENT_CLASS_ID string,
        CLIENT_TIME bigint,
        EVENT_TAG string,
        CELL_PHONE_BRAND string,
        CELL_PHONE_MODEL string,
        CELL_PHONE_CPU string,
        CELL_PHONE_DENSITY string,
        CELL_PHONE_SCREEN_WIDTH bigint,
        CELL_PHONE_SCREEN_HEIGHT bigint,
        CELL_PHONE_NETWORK string,
        CELL_PHONE_SYSTEM_VERSION string,
        MUZHIWAN_VERSION string,
        CELL_PHONE_FIRMWARE string,
        CELL_PHONE_DEVICE_ID string,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
        SESSION_ID string,
        UID string,
        FLAG_ROOT string,
        MEMORY_CARD_SIZE string,
        AVAILABLE_MEMORY_SIZE string,
        FLAG_D_CN string,
        FLAG_WDJ string,
        FLAG_360 string,
        FLAG_YYB string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/100001/';
  
    drop table sdk_game_active_user_stat_tmp;
    create table if not exists sdk_game_active_user_stat_tmp(
        stat_day string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
        user_count int
    )
    Row Format Delimited 
    Fields Terminated By '\t' 
    stored as textfile;

    insert overwrite table sdk_game_active_user_stat_tmp 
    select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as stat_day,
        PACKAGE_NAME,VERSION_CODE,VERSION,count(DISTINCT CELL_PHONE_DEVICE_ID) as user_count 
    from sdk100001 
     where VERSION_CODE<10000000000 and length(VERSION)<100
    group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,PACKAGE_NAME,VERSION_CODE,VERSION;

    drop table sdk_game_active_user_stat;
    create table if not exists sdk_game_active_user_stat(
        day string,
        time int,
        package string,
        versioncode int,
        versionname string,
        total int
    )
    Row Format Delimited 
    Fields Terminated By '\t' 
    stored as textfile;
    
    insert overwrite table sdk_game_active_user_stat 
     SELECT stat_day as day,unix_timestamp(stat_day,'yyyy-MM-dd')  as time,PACKAGE_NAME as package,VERSION_CODE as versioncode,VERSION as versionname,user_count as total 
     FROM sdk_game_active_user_stat_tmp 
     where user_count>0 and stat_day is not null 
     order by total desc;
    
    drop table sdk_game_active_user_stat_tmp;
    
"

sudo -u hdfs hadoop fs -rmr "${destDir}"

mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF
	DROP TABLE IF EXISTS sdk_game_user_info;
	CREATE TABLE sdk_game_user_info (
		  device_id varchar(255) NOT NULL,
		  package varchar(255) NOT NULL,
		  versioncode int(10) NOT NULL,
		  first_time bigint(10) NOT NULL,
		  versionname varchar(255) NOT NULL,
		  brand varchar(255) NOT NULL,
		  model varchar(255) NOT NULL,
		  cpu varchar(255) NOT NULL,
		  density varchar(255) NOT NULL,
		  screen_width int(10) NOT NULL,
		  screen_height int(10) NOT NULL
	);
	
	DROP TABLE IF EXISTS sdk_game_active_user_stat;
	CREATE TABLE sdk_game_active_user_stat (
		  day varchar(255) NOT NULL,
		  time int(10) NOT NULL,
		  package varchar(255) NOT NULL,
		  versioncode int(10) NOT NULL,
		  versionname varchar(255) NOT NULL,
		  total int(10) NOT NULL,
		  KEY index_time (time),
		  KEY index_total (total),
		  KEY index_package (package),
		  KEY index_package_versioncode (package,versioncode)
	);
	
	DROP TABLE IF EXISTS sdk_game_new_user_stat;
	CREATE TABLE sdk_game_new_user_stat (
		  day varchar(255) NOT NULL,
		  time int(10) NOT NULL,
		  package varchar(255) NOT NULL,
		  versioncode int(10) NOT NULL,
		  versionname varchar(255) NOT NULL,
		  total int(10) NOT NULL,
		  KEY index_time (time),
		  KEY index_total (total),
		  KEY index_package (package),
		  KEY index_package_versioncode (package,versioncode)
	) ;

EOF

sudo -u hdfs  sqoop export --connect jdbc:mysql://114.112.50.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_game_user_info --export-dir /user/hive/warehouse/sdk_game_user_info --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
sudo -u hdfs  sqoop export --connect jdbc:mysql://114.112.50.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_game_new_user_stat --export-dir /user/hive/warehouse/sdk_game_new_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
sudo -u hdfs  sqoop export --connect jdbc:mysql://114.112.50.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_game_active_user_stat --export-dir /user/hive/warehouse/sdk_game_active_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_game_user_info  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_game_active_user_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_game_new_user_stat 	ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"


