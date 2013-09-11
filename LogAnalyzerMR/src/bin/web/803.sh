#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists web803 (
        SERVER_TIME bigint,
        CLIENT_IP string,
        CLIENT_AREA string,
        userId string,
        userName string,
        userIp string,
        searchTime int,
        searchKey string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/803/';
  
    drop table web_search_stat_temp;
    create table if not exists web_search_stat_temp (
        day string,
        time int,
        searchKey string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table web_search_stat_temp 
        select from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd'),searchKey,count(*)
        from web803
        where searchKey is not null
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,searchKey;
    
    drop table web_search_stat;
    create table if not exists web_search_stat (
        day string,
        time int,
        searchKey string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table web_search_stat 
        select day,time,searchKey,total
        from web_search_stat_temp
        where searchKey is not null and total>0 and length(searchKey)<100 and length(searchKey)>0
        order by day desc,total desc;
    drop table web_search_stat_temp;
    
 "
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS web_search_stat;
    CREATE TABLE web_search_stat (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          searchKey varchar(255) NOT NULL,
          total int(10) NOT NULL,
          KEY index_searchKey (searchKey)
    )DEFAULT CHARSET=utf8 ;

EOF

 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table web_search_stat --export-dir /user/hive/warehouse/web_search_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  web_search_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 
 
 