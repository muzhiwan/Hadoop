#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists MARKET200001 (
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
        CELL_PHONE_SCREEN_WIDTH int,
        CELL_PHONE_SCREEN_HEIGHT int,
        CELL_PHONE_NETWORK string,
        CELL_PHONE_SYSTEM_VERSION string,
        MUZHIWAN_VERSION string,
        CELL_PHONE_FIRMWARE string,
        CELL_PHONE_DEVICE_ID string,
        UID string,
        EVENT_ID string,
        TITLE string,
        PACKAGE_NAME string,
        VERSION string,
        VERSION_CODE int,
        APK_LINK string,
        APK_ID int,
        APK_BAIDU_LINK string,
        APK_SIZE bigint,
        FLAG_GOOGLE_PLAY int,
        FLAG_LOGIN_ACCOUNT int,
        OPERATION_TAG string,
        CHANNEL string,
        APK_SOURCE string,
        CATEGORY string,
        errorcode int,
        errormsg string,
        responsecode int,
        savepath string,
        realUrl string,
        sdcardavaliablesize int,
        sdcardtotalsize int,
        sdcardmounted int,
        serverip string,
        contact string,
        usetime int,
        progress int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/MARKET200001/';
    
     drop   table download_success_rate;
     create table download_success_rate as
     select case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,CLIENT_AREA as area,count(*) as total
     FROM MARKET200001 
     where progress>0 and usetime>0 and progress/usetime<100 and EVENT_ID='100003'
     group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,CLIENT_AREA ;
   
     drop table download_error_rate;
     create table  download_error_rate as
     select case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,CLIENT_AREA as area,count(*) as total
     FROM MARKET200001 
     where progress>0 and usetime>0 and progress/usetime<100 and EVENT_ID='100002'
     group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,CLIENT_AREA ;
   
     drop table download_cancel_rate;
     create table  download_cancel_rate as
     select case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,CLIENT_AREA as area,count(*) as total
     FROM MARKET200001 
     where progress>0 and usetime>0 and progress/usetime<100 and EVENT_ID='100004'
     group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,CLIENT_AREA ;

    
    
 "
 
 