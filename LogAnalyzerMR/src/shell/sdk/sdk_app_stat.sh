#!/bin/bash

/opt/hive/bin/hive -e "
    create EXTERNAL table if not exists sdk200007 (
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
        VERSION_NAME string,
        VERSION_CODE bigint
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200007/';
  
    drop table sdk_app_stat_tmp;
    create table if not exists sdk_app_stat_tmp (
        brand string,
        model string,
        package string,
        versioncode bigint,
        versionname string,
        total bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table sdk_app_stat_tmp 
        select CELL_PHONE_BRAND,CELL_PHONE_MODEL,PACKAGE_NAME,VERSION_CODE,VERSION_NAME,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from sdk200007
        where VERSION_CODE<10000000000 and length(VERSION_NAME)<100
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,PACKAGE_NAME,VERSION_CODE,VERSION_NAME;
    
    drop table sdk_app_stat;
    create table if not exists sdk_app_stat (
        brand string,
        model string,
        package string,
        versioncode bigint,
        versionname string,
        total bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table sdk_app_stat 
     SELECT brand,model,package,versioncode,versionname,total  FROM sdk_app_stat_tmp 
     where total>0 order by total desc;
    
    drop table sdk_app_stat_tmp;
    
 "
 mysql -h10.1.1.2 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

	DROP TABLE IF EXISTS sdk_app_stat;
	CREATE TABLE IF NOT EXISTS sdk_app_stat (
		  brand varchar(255) DEFAULT NULL,
		  model varchar(255) DEFAULT NULL,
		  package varchar(255) DEFAULT NULL,
		  versioncode int(10) DEFAULT NULL,
		  versionname varchar(255) DEFAULT NULL,
		  total int(10) DEFAULT NULL,
		  KEY index_total (total),
		  KEY index_package (package),
		  KEY index_package_versioncode (package,versioncode)
	) ;

EOF

/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_app_stat --export-dir /user/hive/warehouse/sdk_app_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.2 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_app_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"



 
 
 