#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists dl (
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
    location '/user/hdfs/200001/';
    
    
    drop table rate_stat;
     create  table rate_stat as
        select CLIENT_IP,CLIENT_AREA,APK_ID as vid , package_name as package,version_code as versioncode,APK_LINK,(apk_size/1024)/(usetime/1000) as rate 
        from dl 
        where  EVENT_ID='100003';
    
    INSERT OVERWRITE LOCAL DIRECTORY '/home/hdfs/dl/' SELECT * FROM rate_stat where rate<50;
    
    
 "
 
 
 
