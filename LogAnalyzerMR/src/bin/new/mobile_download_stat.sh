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
    
    
    drop table user_download_record;
     create  table user_download_record as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
          uid,APK_ID as vid , package_name as package,version_code as versioncode
        from MARKET200001 
        where EVENT_CLASS_ID='MARKET200001' and UID!='-1' and EVENT_ID='100001';
    
    
    
    
    drop table market_game_download_click_status_tmp;
    create table if not exists market_game_download_click_status_tmp (
        day string,
        apkid int,
        event string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_click_status_tmp 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            APK_ID,EVENT_ID,count(*) as total 
        from MARKET200001 
        where EVENT_CLASS_ID='MARKET200001'
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID,EVENT_ID;
    
    
   drop table market_mobile_download_event_click;
    create table if not exists market_mobile_download_event_click (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table  market_mobile_download_event_click
     select day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,total,0,0,0,0,0
     FROM market_game_download_click_status_tmp 
     where total>0 and apkid<10000000000 and apkid>0 and event='100001'
     order by day desc,total desc;
   
   drop table market_mobile_download_event_error;
    create table if not exists market_mobile_download_event_error (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table  market_mobile_download_event_error
     select day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,0,0,total,0,0
     FROM market_game_download_click_status_tmp 
     where total>0 and apkid<10000000000 and apkid>0 and event='100002'
     order by day desc,total desc;
   
   drop table market_mobile_download_event_success;
    create table if not exists market_mobile_download_event_success (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table  market_mobile_download_event_success
     select day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,0,total,0,0,0
     FROM market_game_download_click_status_tmp 
     where total>0 and apkid<10000000000 and apkid>0 and event='100003'
     order by day desc,total desc;
   
   drop table market_mobile_download_event_cancel;
    create table if not exists market_mobile_download_event_cancel (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table  market_mobile_download_event_cancel
     select day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,0,0,0,total,0
     FROM market_game_download_click_status_tmp 
     where total>0 and apkid<10000000000 and apkid>0 and event='100004'
     order by day desc,total desc;
   
   drop table market_mobile_download_event_begin;
    create table if not exists market_mobile_download_event_begin (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table  market_mobile_download_event_begin
     select day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,total,0,0,0,0
     FROM market_game_download_click_status_tmp 
     where total>0 and apkid<10000000000 and apkid>0 and event='100005'
     order by day desc,total desc;
   
   drop table market_mobile_download_event_exchange;
    create table if not exists market_mobile_download_event_exchange (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table  market_mobile_download_event_exchange
     select day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,0,0,0,0,0,total
     FROM market_game_download_click_status_tmp 
     where total>0 and apkid<10000000000 and apkid>0 and event='100006'
     order by day desc,total desc;
   
    drop table market_mobile_download_event_tmp;
    create table if not exists market_mobile_download_event_tmp (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_mobile_download_event_tmp 
        select day ,time,apkid ,sum(click),sum(begin),sum(success),sum(error),sum(cancel),sum(exchange) from (
        select * from market_mobile_download_event_click 
        UNION ALL 
        select * from market_mobile_download_event_begin 
        UNION ALL 
        select * from market_mobile_download_event_success 
        UNION ALL 
        select * from market_mobile_download_event_error 
        UNION ALL 
        select * from market_mobile_download_event_cancel 
        UNION ALL 
        select * from market_mobile_download_event_exchange 
        ) tmp where time>0 group by day,time,apkid order by day desc;
    
    
    drop table market_mobile_download_stat;
    create table if not exists market_mobile_download_stat (
         day string,
         time int,
         apkid int,
         click int,
         begin int,
         success int,
         error int,
         cancel int,
         exchange int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_mobile_download_stat 
        select day ,time,apkid ,sum(click),sum(begin),sum(success),sum(error),sum(cancel),sum(exchange) from (
        select * from market_mobile_download_event_tmp 
        UNION ALL 
        select day,time,apkid,click,0 as begin,success,error,cancel,0 as exchange from market_mobile_download_stat_tmp 
        ) tmp where time>0 group by day,time,apkid order by day desc;
    
    drop table market_downlog_mob;
    create table market_downlog_mob as 
          select a.day as day ,a.time as time,sum(a.click) as click,sum(a.begin) as begin ,sum(a.success) as success ,sum(a.error) as error ,sum(a.cancel) as cancel,sum(a.exchange) as exchange from market_mobile_download_stat a group by a.day,a.time;    
    
    
 "
 
  mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS market_mobile_download_stat;
    CREATE TABLE market_mobile_download_stat (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          apkid int(10) NOT NULL,
          click int(10) NOT NULL,
          begin int(10) NOT NULL,
          success int(10) NOT NULL,
          error int(10) NOT NULL,
          cancel int(10) NOT NULL,
          exchange int(10) NOT NULL,
          KEY index_apkid (apkid),
          KEY index_click (click),
          KEY index_begin (begin),
          KEY index_success (success),
          KEY index_error (error),
          KEY index_exchange (exchange),
          KEY index_cancel (cancel)
          
    );

    DROP TABLE IF EXISTS market_downlog_mob;
    CREATE TABLE market_downlog_mob (
         day varchar(255) NOT NULL,
         time int(10) NOT NULL,
         click int(10) NOT NULL,
          begin int(10) NOT NULL,
          success int(10) NOT NULL,
          error int(10) NOT NULL,
          cancel int(10) NOT NULL,
          exchange int(10) NOT NULL
    );
    
    DROP TABLE IF EXISTS user_download_record;
    CREATE TABLE user_download_record (
         day varchar(255) NOT NULL,
         uid varchar(255) NOT NULL,
         vid int(10) NOT NULL,
          package varchar(255) NOT NULL,
          versioncode int(10) NOT NULL,
          KEY index_uid (uid)
    );
    
   
EOF
 
  sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_mobile_download_stat --export-dir /user/hive/warehouse/market_mobile_download_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_downlog_mob --export-dir /user/hive/warehouse/market_downlog_mob --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table user_download_record --export-dir /user/hive/warehouse/user_download_record --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_mobile_download_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_downlog_mob  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  user_download_record  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 
 
