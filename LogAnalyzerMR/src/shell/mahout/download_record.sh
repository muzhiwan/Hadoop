#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists sdk200001 (
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
        CATEGORY string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200001/';
  
    drop table download_record;
    create table if not exists download_record (
        deviceid string,
        package_name string
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table download_record 
        select CELL_PHONE_DEVICE_ID, PACKAGE_NAME
        from sdk200001 
        where CELL_PHONE_DEVICE_ID is not null and PACKAGE_NAME is not null;
        
    drop table mahout_record;
    create table if not exists mahout_record (
        deviceid string,
        package_names string
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
   
   insert overwrite table mahout_record 
       SELECT deviceid,concat_ws('|', collect_set(package_name)) FROM download_record GROUP BY deviceid;

 INSERT OVERWRITE DIRECTORY '/user/hdfs/mahout/src' SELECT package_names FROM mahout_record;
    
 "
 
 sudo -u hdfs mahout fpg -i /user/hdfs/mahout/src -o /user/hdfs/mahout/out  -method mapreduce -regex '[\|]'  -s 2 -k 10
 
 
 
 
 
 
 
 
 
 