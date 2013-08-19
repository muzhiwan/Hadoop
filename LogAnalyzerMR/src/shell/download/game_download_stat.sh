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
        CELL_PHONE_SCREEN_WIDTH string,
        CELL_PHONE_SCREEN_HEIGHT string,
        CELL_PHONE_NETWORK string,
        CELL_PHONE_SYSTEM_VERSION string,
        MUZHIWAN_VERSION string,
        CELL_PHONE_FIRMWARE string,
        CELL_PHONE_DEVICE_ID string,
        TITLE string,
        PACKAGE_NAME string,
        VERSION string,
        VERSION_CODE string,
        APK_LINK string,
        APK_ID string,
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
  
    drop table game_download_stat;
    create table if not exists game_download_stat (
        stat_day string,
        APK_ID string,
        PACKAGE_NAME string,
        VERSION_CODE string,
        download_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table game_download_stat 
		select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as stat_day,
            APK_ID,PACKAGE_NAME,VERSION_CODE,count(DISTINCT CELL_PHONE_DEVICE_ID) as download_count 
        from sdk200003 group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID,PACKAGE_NAME,VERSION_CODE;
    
 "
 
 
 