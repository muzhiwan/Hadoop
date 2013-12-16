#!/bin/bash

sudo -u hdfs hive -e "

    create EXTERNAL table if not exists sdk200007_history (
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
        VERSION_NAME string,
        VERSION_CODE bigint
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200007/';
  
      
    drop   table market_model_info_history;
    create table market_model_info_history as  
        select CELL_PHONE_BRAND as brand,CELL_PHONE_MODEL as model,CELL_PHONE_CPU as cpu,CELL_PHONE_DENSITY as density ,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from sdk200007
        where CELL_PHONE_BRAND is not null  and CELL_PHONE_MODEL is not null and CELL_PHONE_DENSITY is not null
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,CELL_PHONE_CPU,CELL_PHONE_DENSITY;
   
    
    drop   table sdk_app_stat_history;
    create  table sdk_app_stat_history as
        select CELL_PHONE_BRAND as brand,CELL_PHONE_MODEL as model,PACKAGE_NAME as package,VERSION_CODE as versioncode,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from sdk200007
        where VERSION_CODE<10000000000
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,PACKAGE_NAME,VERSION_CODE;
        
    
    
    
    
    
    drop   table sdk_app_stat_tmp1;
    create  table sdk_app_stat_tmp1 as
        select CELL_PHONE_BRAND as brand,CELL_PHONE_MODEL as model,PACKAGE_NAME as package,VERSION_CODE as versioncode,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from sdk200007
        where VERSION_CODE<10000000000
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,PACKAGE_NAME,VERSION_CODE;
        
    
     drop   table market_model_info1;
    create table market_model_info1 as  
        select CELL_PHONE_BRAND as brand,CELL_PHONE_MODEL as model,CELL_PHONE_CPU as cpu,CELL_PHONE_DENSITY as density ,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from sdk200007 
        where CELL_PHONE_BRAND is not null  and CELL_PHONE_MODEL is not null and CELL_PHONE_DENSITY is not null
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,CELL_PHONE_CPU,CELL_PHONE_DENSITY;
    
    
    
    
    
 "
 
 

 
 
 
