#!/bin/bash

sudo -u hdfs hive -e "

    create EXTERNAL table if not exists StandSDK100008 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
        fps int,
        totalmemsize int,
        avavilablememsize int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100008/';
    
    drop table sdk_mobile_fps;
    create  table sdk_mobile_fps as
        select package_name as package,CELL_PHONE_MODEL as model,sum(fps) as totla,count(*) as count,sum(fps)/count(*) as avg
        from StandSDK100008 
        group by PACKAGE_NAME,CELL_PHONE_MODEL;
    
    
    
     
    
    
    
    
"

