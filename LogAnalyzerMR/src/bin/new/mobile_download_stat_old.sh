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
    

    drop table market_game_download_click_stat_tmp;
    create table if not exists market_game_download_click_stat_tmp (
        day string,
        apkid int,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_click_stat_tmp 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            APK_ID,count(*) as total 
        from sdk200001 
        where VERSION_CODE<10000000000
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID;
    
    drop table market_game_download_click_stat;
    create table if not exists market_game_download_click_stat (
         day string,
         time int,
         apkid int,
         click int,
         success int,
         error int,
         cancel int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_game_download_click_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,total,0,0,0
     FROM market_game_download_click_stat_tmp 
     where total>0 and apkid<10000000000 and apkid>0
     order by total desc;
    
    drop table market_game_download_click_stat_tmp;
    
    
    
    
    
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
  
    drop table market_game_download_success_stat_tmp;
    
    create table if not exists market_game_download_success_stat_tmp (
        day string,
        apkid int,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_success_stat_tmp 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            APK_ID,count(*) as total 
        from sdk200003 
        where VERSION_CODE<10000000000
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID;
    
    drop table market_game_download_success_stat;
    create table if not exists market_game_download_success_stat (
         day string,
         time int,
         apkid int,
         click int,
         success int,
         error int,
         cancel int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_game_download_success_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,total,0,0 
     FROM market_game_download_success_stat_tmp 
     where total>0 and apkid<10000000000 and apkid>0
     order by total desc;
    
    drop table market_game_download_success_stat_tmp;
    
    
    
    
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
  
    drop table market_game_download_error_stat_tmp;
    create table if not exists market_game_download_error_stat_tmp (
        day string,
        apkid int,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_error_stat_tmp 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            APK_ID,count(*) as total 
        from sdk200002
        where VERSION_CODE<10000000000
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID;
    
    drop table market_game_download_error_stat;
    create table if not exists market_game_download_error_stat (
         day string,
         time int,
         apkid int,
         click int,
         success int,
         error int,
         cancel int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_game_download_error_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,0,total,0
     FROM market_game_download_error_stat_tmp 
     where total>0 and apkid<10000000000 and apkid>0
     order by total desc;
    
    drop table market_game_download_error_stat_tmp;
    
    
    
    
    create EXTERNAL table if not exists sdk200004 (
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
    location '/apilogs/src/200004/';
  
    drop   table market_game_download_cancel_stat_tmp;
    create table if not exists market_game_download_cancel_stat_tmp (
        day string,
        apkid int,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_cancel_stat_tmp 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            APK_ID,count(*) as total 
        from sdk200004 
        where VERSION_CODE<10000000000
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID;
    
    drop table market_game_download_cancel_stat;
    create table if not exists market_game_download_cancel_stat (
         day string,
         time int,
         apkid int,
         click int,
         success int,
         error int,
         cancel int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_game_download_cancel_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,0,0,total 
     FROM market_game_download_cancel_stat_tmp 
     where total>0 and apkid<10000000000 and apkid>0
     order by total desc;
    
    drop table market_game_download_cancel_stat_tmp;
    
    
    
    drop table market_mobile_download_stat_tmp;
    create table if not exists market_mobile_download_stat_tmp (
         day string,
         time int,
         apkid int,
         click int,
         success int,
         error int,
         cancel int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_mobile_download_stat_tmp 
        select day ,time,apkid ,sum(click),sum(success),sum(error),sum(cancel) from (
        select * from market_game_download_click_stat 
        UNION ALL 
        select * from market_game_download_success_stat 
        UNION ALL 
        select * from market_game_download_error_stat 
        UNION ALL 
        select * from market_game_download_cancel_stat 
        ) tmp where time>0 group by day,time,apkid;
    
 "
 
 
 