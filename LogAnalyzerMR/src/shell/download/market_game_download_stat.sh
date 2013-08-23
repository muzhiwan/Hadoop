#!/bin/bash

/opt/hive/bin/hive -e "
    create EXTERNAL table if not exists sdk200003 (
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
        VERSION string,
        VERSION_CODE bigint,
        APK_LINK string,
        APK_ID bigint,
        APK_BAIDU_LINK string,
        APK_SIZE bigint,
        FLAG_GOOGLE_PLAY bigint,
        FLAG_LOGIN_ACCOUNT bigint,
        OPERATION_TAG string,
        CHANNEL string,
        APK_SOURCE string,
        CATEGORY string,
        COST_TIME bigint,
        SERVER_IP string,
        PROGRESS bigint,
        REALURL string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200003/';
  
    drop table market_game_download_stat_tmp;
    
    create table if not exists market_game_download_stat_tmp (
        day string,
        apkid bigint,
        package string,
        versioncode bigint,
        versionname string,
        total bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_stat_tmp 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            APK_ID,PACKAGE_NAME,VERSION_CODE,VERSION,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from sdk200003 
        where VERSION_CODE<10000000000 and length(VERSION)<100
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID,PACKAGE_NAME,VERSION_CODE,VERSION;
    
    drop table market_game_download_stat;
    create table if not exists market_game_download_stat (
         day string,
         time bigint,
         apkid bigint,
         package string,
         versioncode bigint,
         versionname string,
         total bigint
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_game_download_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,package,versioncode,versionname,total 
     FROM market_game_download_stat_tmp 
     where total>0 and apkid<10000000000
     order by total desc;
    
    drop table market_game_download_stat_tmp;
    
 "
 
 mysql -h10.1.1.2 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

	DROP TABLE IF EXISTS market_game_download_stat;
	CREATE TABLE market_game_download_stat (
		  day varchar(255) NOT NULL,
		  time int(10) NOT NULL,
		  apkid int(10) NOT NULL,
		  package varchar(255) NOT NULL,
		  versioncode int(10) NOT NULL,
		  versionname varchar(255) NOT NULL,
		  total int(10) NOT NULL,
		  KEY index_apkid (apkid),
		  KEY index_package (package),
		  KEY index_total (total),
		  KEY index_package_versioncode (package,versioncode)
	);

EOF

 /opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_game_download_stat --export-dir /user/hive/warehouse/market_game_download_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.2 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_game_download_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 