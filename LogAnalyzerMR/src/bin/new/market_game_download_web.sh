#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists web814 (
        SERVER_TIME bigint,
        CLIENT_IP string,
        CLIENT_AREA string,
        userId string,
        userName string,
        userIp string,
        downTime int,
        gameId int,
        downWay int,
        downResource int,
        brandId int,
        productId int
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/814/';
  
    drop table market_game_download_web;
    
    create table if not exists market_game_download_web (
        day string,
        time int,
        gameId int,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table market_game_download_web 
        select from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd'),gameId,count(*)
        from web814 
        where gameId>0 and SERVER_TIME>0
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,gameId;
        
    
    drop table market_downlog_web;
    Create Table market_downlog_web as  select a.day as day ,a.time as time,sum(a.total) as total from market_game_download_web a group by a.day,a.time;
    
    
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
        select day,apkid,click as total 
        from market_mobile_download_stat;
        
    drop table market_all_download_stat;
    create table if not exists market_all_download_stat (
         day string,
         time int,
         gameId int,
         web_count int,
         client_count int,
         total int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    
    insert overwrite table market_all_download_stat 
        select day ,time,gameId ,sum(web_count),sum(client_count),sum(total) from (
        select day,time ,gameId ,total as web_count , 0 as client_count ,total  from market_game_download_web
        UNION ALL 
        select day,unix_timestamp(day,'yyyy-MM-dd') as time,apkid as gameId,0 as web_count,total as client_count,total  from market_game_download_click_stat_tmp
        ) tmp where time>0 group by day,time,gameId;
    
    drop   table market_downlog_all;
    create table if not exists market_downlog_all (
         day string,
         time int,
         web_count int,
         client_count int,
         total int
     )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
    insert overwrite table market_downlog_all
        select a.day as day ,a.time as time,sum(a.web_count) as web_count ,sum(a.client_count) as client_count ,sum(a.total) as total  from market_all_download_stat a group by a.day,a.time;    
    
    drop table market_game_download_click_stat_tmp;
    
 "
 
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS market_game_download_web;
    CREATE TABLE market_game_download_web (
         day varchar(255) NOT NULL,
         time int(10) NOT NULL,
         gameId int(10) NOT NULL,
         total int(10) NOT NULL,
         KEY index_gameId (gameId)
    );
    
    DROP TABLE IF EXISTS market_all_download_stat;
    CREATE TABLE market_all_download_stat (
         day varchar(255) NOT NULL,
         time int(10) NOT NULL,
         gameId int(10) NOT NULL,
         web_count int(10) NOT NULL,
         client_count int(10) NOT NULL,
         total int(10) NOT NULL,
         KEY index_gameId (gameId),
	     KEY index_web_count (web_count),
	     KEY index_client_count (client_count),
	     KEY index_total (total)
    );
    
    DROP TABLE IF EXISTS market_downlog_web;
    CREATE TABLE market_downlog_web (
         day varchar(255) NOT NULL,
         time int(10) NOT NULL,
         total int(10) NOT NULL
    );
    
    DROP TABLE IF EXISTS market_downlog_all;
    CREATE TABLE market_downlog_all (
         day varchar(255) NOT NULL,
         time int(10) NOT NULL,
         web_count int(10) NOT NULL,
         client_count int(10) NOT NULL,
         total int(10) NOT NULL
    );
    
EOF

 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_game_download_web --export-dir /user/hive/warehouse/market_game_download_web --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_all_download_stat --export-dir /user/hive/warehouse/market_all_download_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_downlog_web --export-dir /user/hive/warehouse/market_downlog_web --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_downlog_all --export-dir /user/hive/warehouse/market_downlog_all --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_game_download_web  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_all_download_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_downlog_web  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_downlog_all  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"



 
 