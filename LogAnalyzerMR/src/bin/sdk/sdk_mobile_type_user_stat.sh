#!/bin/bash

sudo -u hdfs hive -e "
    drop table sdk_mobile_type_active_user_stat_tmp;
    create table if not exists sdk_mobile_type_active_user_stat_tmp(
        day string,
        package string,
        versioncode int,
        versionname string,
        brand string,
        model string,
        total int
    )
    Row Format Delimited 
    Fields Terminated By '\t' 
    stored as textfile;

    insert overwrite table sdk_mobile_type_active_user_stat_tmp 
       select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
              PACKAGE_NAME,VERSION_CODE,VERSION,CELL_PHONE_BRAND,CELL_PHONE_MODEL,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
       from sdk100001 
       group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,PACKAGE_NAME,VERSION_CODE,VERSION,CELL_PHONE_BRAND,CELL_PHONE_MODEL;
    
    drop table sdk_mobile_type_active_user_stat;
    create table if not exists sdk_mobile_type_active_user_stat (
        day string,
        time int,
        package string,
        versioncode int,
        versionname string,
        brand string,
        model string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table sdk_mobile_type_active_user_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,package,versioncode,versionname,brand,model,total 
     FROM sdk_mobile_type_active_user_stat_tmp 
     where total>0 and length(versionname)<100 order by total desc;
    
    drop table sdk_mobile_type_active_user_stat_tmp;
    
    drop table sdk_mobile_type_new_user_stat_tmp;
    create table if not exists sdk_mobile_type_new_user_stat_tmp (
        day string,
        package string,
        versioncode int,
        versionname string,
        brand string,
        model string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table sdk_mobile_type_new_user_stat_tmp 
    select from_unixtime(floor(first_time/1000),'yyyy-MM-dd') as day,package,versioncode,versionname,brand,model,count(DISTINCT device_id ) as total 
    from sdk_game_user_info 
    group by from_unixtime(floor(first_time/1000),'yyyy-MM-dd'),package,versioncode,versionname,brand,model;
    
    drop table sdk_mobile_type_new_user_stat;
    create table if not exists sdk_mobile_type_new_user_stat (
        day string,
        time int,
        package string,
        versioncode int,
        versionname string,
        brand string,
        model string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table sdk_mobile_type_new_user_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time, package,versioncode,versionname,brand,model,total
     FROM sdk_mobile_type_new_user_stat_tmp 
     where total>0 and length(versionname)<100 order by total desc;
    
    drop table sdk_mobile_type_new_user_stat_tmp; 
"
mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

	DROP TABLE IF EXISTS sdk_mobile_type_active_user_stat;
	CREATE TABLE sdk_mobile_type_active_user_stat (
		  day varchar(255) NOT NULL,
		  time int(10) NOT NULL,
		  package varchar(255) NOT NULL,
		  versioncode int(10) NOT NULL,
		  versionname varchar(255) NOT NULL,
		  brand varchar(255) NOT NULL,
		  model varchar(255) NOT NULL,
		  total int(10) NOT NULL,
		  KEY index_time (time),
		  KEY index_total (total),
		  KEY index_model (model)
	);
	
	DROP TABLE IF EXISTS sdk_mobile_type_new_user_stat;
	CREATE TABLE sdk_mobile_type_new_user_stat (
		  day varchar(255) NOT NULL,
		  time int(10) NOT NULL,
		  package varchar(255) NOT NULL,
		  versioncode int(10) NOT NULL,
		  versionname varchar(255) NOT NULL,
		  brand varchar(255) NOT NULL,
		  model varchar(255) NOT NULL,
		  total int(10) NOT NULL,
		  KEY index_time (time),
		  KEY index_total (total),
		  KEY index_model (model)
	);

EOF

sudo -u hdfs  sqoop export --connect jdbc:mysql://114.112.50.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_mobile_type_active_user_stat --export-dir /user/hive/warehouse/sdk_mobile_type_active_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
sudo -u hdfs  sqoop export --connect jdbc:mysql://114.112.50.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_mobile_type_new_user_stat --export-dir /user/hive/warehouse/sdk_mobile_type_new_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e"ALTER TABLE  sdk_mobile_type_active_user_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_mobile_type_new_user_stat     ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"


