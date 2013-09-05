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
        where gameId>0
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,gameId;
    
 "
 
 
 mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

	DROP TABLE IF EXISTS market_game_download_web;
	CREATE TABLE market_game_download_web (
		  day varchar(255) NOT NULL,
		  time int(10) NOT NULL,
		  gameId int(10) NOT NULL,
		  total int(10) NOT NULL,
		  KEY index_gameId (gameId)
	);

EOF

 sudo -u hdfs  sqoop export --connect jdbc:mysql://114.112.50.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_game_download_web --export-dir /user/hive/warehouse/market_game_download_web --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h114.112.50.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  market_game_download_web  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 