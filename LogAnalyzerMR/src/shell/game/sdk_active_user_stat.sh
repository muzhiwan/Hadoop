#!/bin/bash

/opt/hive/bin/hive -e "
    create EXTERNAL table if not exists sdk100003 (
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
        VERSION_CODE string,
        VERSION string,
        SESSION_ID string,
        UID string,
        sdksession string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/100003/';
  
    drop   table sdk_active_user_stat;
    create table if not exists sdk_active_user_stat (
        stat_day string,
        user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_active_user_stat 
        select 
            case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as stat_day
            ,count(DISTINCT CELL_PHONE_DEVICE_ID ) as user_count          
        from sdk100001
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end ;


"

