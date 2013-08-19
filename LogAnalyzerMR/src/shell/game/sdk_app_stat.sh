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
        CELL_PHONE_SCREEN_WIDTH string,
        CELL_PHONE_SCREEN_HEIGHT string,
        CELL_PHONE_NETWORK string,
        CELL_PHONE_SYSTEM_VERSION string,
        MUZHIWAN_VERSION string,
        CELL_PHONE_FIRMWARE string,
        CELL_PHONE_DEVICE_ID string,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_NAME string,
        VERSION_CODE string,
        APK_ID string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200007/';
  
    drop table sdk_app_stat;
    create table if not exists sdk_app_stat (
        CELL_PHONE_BRAND string,
        CELL_PHONE_MODEL string,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_NAME string,
        count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table sdk_app_stat 
		select CELL_PHONE_BRAND,CELL_PHONE_MODEL,TITLE,PACKAGE_NAME,VERSION_NAME,count(DISTINCT CELL_PHONE_DEVICE_ID) as count 
        from sdk200007
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,TITLE,PACKAGE_NAME,VERSION_NAME;
    
 "
 
 
 