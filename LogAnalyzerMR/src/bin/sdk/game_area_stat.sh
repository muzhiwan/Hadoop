#!/bin/bash

/opt/hive/bin/hive -e "
    create EXTERNAL table if not exists sdk100001 (
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
        FLAG_ROOT string,
        MEMORY_CARD_SIZE string,
        AVAILABLE_MEMORY_SIZE string,
        FLAG_D_CN string,
        FLAG_WDJ string,
        FLAG_360 string,
        FLAG_YYB string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/100001/';
  
    drop table game_area_stat;
    create table if not exists sdk_game_area_stat(
        day string,
        package string,
        versioncode int,
        versionname string,
        area string,
        total int
    )
    Row Format Delimited 
    Fields Terminated By '\t' 
    stored as textfile;
    
    insert overwrite table sdk_game_area_stat 
    select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as stat_day,
    PACKAGE_NAME,VERSION_CODE,VERSION,regexp_extract(CLIENT_AREA, '(\S+)(\s+)(\S+)', 0) ,count(DISTINCT CELL_PHONE_DEVICE_ID) as total
    from sdk100001 group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end
    ,PACKAGE_NAME,VERSION_CODE,VERSION,regexp_extract(CLIENT_AREA, '(\S+)(\s+)(\S+)', 0) ;


"

