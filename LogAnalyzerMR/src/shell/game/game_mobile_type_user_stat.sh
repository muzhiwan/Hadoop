#!/bin/bash

hive -e "
    drop table mobile_type_active_user_stat;
    create table if not exists mobile_type_active_user_stat(
        stat_day string,
        PACKAGE_NAME string,
        VERSION_CODE string,
        CELL_PHONE_BRAND string,
        CELL_PHONE_MODEL string,
        user_count bigint
    )
    Row Format Delimited 
    Fields Terminated By '\t' 
    stored as textfile;

    insert overwrite table mobile_type_active_user_stat 
       select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as stat_day,
              PACKAGE_NAME,VERSION_CODE,CELL_PHONE_BRAND,CELL_PHONE_MODEL,count(DISTINCT CELL_PHONE_DEVICE_ID) as user_count 
       from sdk100001 
       group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,PACKAGE_NAME,VERSION_CODE,CELL_PHONE_BRAND,CELL_PHONE_MODEL;
    
     drop table mobile_type_active_user_stat_sort;
    create table if not exists mobile_type_active_user_stat_sort (
        stat_day string,
        PACKAGE_NAME string,
        VERSION_CODE string,
        CELL_PHONE_BRAND string,
        CELL_PHONE_MODEL string,
        user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table mobile_type_active_user_stat_sort 
     SELECT * FROM mobile_type_active_user_stat where user_count>0 order by user_count desc;
    
    
    drop table mobile_type_new_user_stat;
    create table if not exists mobile_type_new_user_stat (
        stat_day string,
        PACKAGE_NAME string,
        VERSION_CODE string,
        CELL_PHONE_BRAND string,
        CELL_PHONE_MODEL string,
        user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table mobile_type_new_user_stat select from_unixtime(floor(first_time/1000),'yyyy-MM-dd') as stat_day,PACKAGE_NAME,VERSION_CODE,CELL_PHONE_BRAND,CELL_PHONE_MODEL,count(DISTINCT CELL_PHONE_DEVICE_ID ) as user_count from game_user_info group by from_unixtime(floor(first_time/1000),'yyyy-MM-dd'),PACKAGE_NAME,VERSION_CODE,CELL_PHONE_BRAND,CELL_PHONE_MODEL;
    
"

