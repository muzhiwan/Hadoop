#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists sdk200002 (
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
        errorcode int,
        errormsg string,
        responsecode int,
        savepath string,
        url string,
        sdcardavaliablesize int,
        sdcardtotalsize int,
        sdcardmounted int,
        serverip string,
        contact string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200002/';
  
    drop table market_game_download_stat_tmp;
    
    create table if not exists market_game_download_error_stat_tmp (
        day string,
        apkid int,
        session string,
        errorcode int,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_error_stat_tmp 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            APK_ID,OPERATION_TAG,responsecode,count(*) as total 
        from sdk200002
        where VERSION_CODE<10000000000
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID,OPERATION_TAG,responsecode;
    
    
    
    drop table market_game_download_error_stat;
    create table if not exists market_game_download_error_stat (
         day string,
         time int,
         apkid int,
         package string,
         versioncode int,
         total int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_game_download_error_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,package,versioncode,total 
     FROM market_game_download_error_stat_tmp 
     where total>0 and apkid<10000000000
     order by total desc;
    
    drop table market_game_download_error_stat_tmp;
    
 "
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS market_game_download_error_stat;
    CREATE TABLE market_game_download_error_stat (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          apkid int(10) NOT NULL,
          package varchar(255) NOT NULL,
          versioncode int(10) NOT NULL,
          total int(10) NOT NULL,
          KEY index_apkid (apkid),
          KEY index_package (package),
          KEY index_total (total),
          KEY index_package_versioncode (package,versioncode)
    );

EOF
    
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_game_download_error_stat --export-dir /user/hive/warehouse/market_game_download_error_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_game_download_error_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 