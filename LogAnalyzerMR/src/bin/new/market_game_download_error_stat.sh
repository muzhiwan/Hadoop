#!/bin/bash

sudo -u hdfs hive -e "
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
    
    
    
    
  
    drop table market_game_download_error_stat_tmp1;
    create table if not exists market_game_download_error_stat_tmp1 (
        day string,
        apkid int,
        errorcode int,
        message string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_error_stat_tmp1 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,APK_ID,responsecode,savepath,count(*) as total 
        from sdk200002
        where VERSION_CODE<10000000000 and OPERATION_TAG is not null and responsecode<0 and savepath is not null
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID,responsecode,savepath;
    
    
    
     drop table market_game_download_error_stat_tmp2;
    create table if not exists market_game_download_error_stat_tmp2 (
        day string,
        apkid int,
        errorcode int,
        message string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_game_download_error_stat_tmp2
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,APK_ID,errorcode,errormsg,count(*) as total 
        from MARKET200001
        where EVENT_ID='100002' and OPERATION_TAG is not null and errorcode<0
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,APK_ID,errorcode,errormsg;
    
    drop table market_game_download_error_stat_tmp;
    create table market_game_download_error_stat_tmp as 
    select * from (
        select * from market_game_download_error_stat_tmp1
        UNION ALL 
        select * from market_game_download_error_stat_tmp2 
        
        ) tmp where day is not null;
    
    
    
    
    
    drop    table market_download_error_game_tmp;
    Create Table market_download_error_game_tmp as  select day,unix_timestamp(day,'yyyy-MM-dd')  as time,apkid,errorcode,message,sum(a.total) as total  from market_game_download_error_stat_tmp a where day is not null  group by day,unix_timestamp(day,'yyyy-MM-dd'),apkid,errorcode,message;
   
    drop    table market_download_error_game;
    Create Table market_download_error_game as  select day,time,apkid,errorcode,message,total  from market_download_error_game_tmp a where total>10 order by day desc, apkid desc,total desc;
    drop    table market_download_error_game_tmp;
    
    
    drop    table market_download_error_code;
    Create Table market_download_error_code as  select day,unix_timestamp(day,'yyyy-MM-dd')  as time,errorcode,sum(a.total) as total  from market_game_download_error_stat_tmp a where day is not null group by day,unix_timestamp(day,'yyyy-MM-dd'),errorcode;
   
    drop    table market_download_error_message;
    Create Table market_download_error_message as  select day,unix_timestamp(day,'yyyy-MM-dd')  as time,errorcode,message,sum(a.total) as total  from market_game_download_error_stat_tmp a  where day is not null and length(message)<255 group by day,unix_timestamp(day,'yyyy-MM-dd'),errorcode,message;

 "
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS market_download_error_game;
    CREATE TABLE market_download_error_game (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          apkid int(10) NOT NULL,
          errorcode int(10) NOT NULL,
          message varchar(255) NOT NULL,
          total int(10) NOT NULL,
          KEY index_errorcode (errorcode),
          KEY index_time (time),
          KEY index_apkid (apkid),
          KEY index_total (total)
    )DEFAULT CHARSET=utf8 ;
    DROP TABLE IF EXISTS market_download_error_code;
    CREATE TABLE market_download_error_code (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          errorcode int(10) NOT NULL,
          total int(10) NOT NULL,
          KEY index_errorcode (errorcode),
          KEY index_time (time),
          KEY index_total (total)
    );
    DROP TABLE IF EXISTS market_download_error_message;
    CREATE TABLE market_download_error_message (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          errorcode int(10) NOT NULL,
          message varchar(255) NOT NULL,
          total int(10) NOT NULL,
          KEY index_errorcode (errorcode),
          KEY index_time (time),
          KEY index_total (total)
    )DEFAULT CHARSET=utf8 ;

EOF
    
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_download_error_game --export-dir /user/hive/warehouse/market_download_error_game --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_download_error_code --export-dir /user/hive/warehouse/market_download_error_code --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_download_error_message --export-dir /user/hive/warehouse/market_download_error_message --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_download_error_game  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_download_error_code  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_download_error_message  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 