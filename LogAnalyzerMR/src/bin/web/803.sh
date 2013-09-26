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
  
    drop table web_search_day_stat;
    create table if not exists web_search_day_stat (
        day string,
        time int,
        searchKey string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\\001'
    stored as textfile;
    
     insert overwrite table web_search_day_stat 
        select from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd'),searchKey,count(*)
        from web803
        where searchKey is not null and length(searchKey)<100 and length(searchKey)>0
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,searchKey;
    
    drop table web_log_search;
    Create Table web_log_search as  select a.day as day ,a.time as time,sum(a.total) as total from web_search_day_stat a where a.time>0 group by a.day,a.time;
    
    
    drop table web_search_week_stat;
    create table if not exists web_search_week_stat (
        year int,
        week int,
        searchKey string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\\001'
    stored as textfile;

    insert overwrite table web_search_week_stat
        select from_unixtime(SERVER_TIME,'yyyy'),weekofyear(from_unixtime(SERVER_TIME,'yyyy-MM-dd')),searchKey,count(*)
        from web803
        where searchKey is not null and length(searchKey)<100 and length(searchKey)>0
        group by from_unixtime(SERVER_TIME,'yyyy'),weekofyear(from_unixtime(SERVER_TIME,'yyyy-MM-dd')),searchKey;

    drop table web_search_month_stat;
    create table if not exists web_search_month_stat (
        year int,
        month int,
        searchKey string,
        total int
    )
    Row Format Delimited
    Fields Terminated By '\\001'
    stored as textfile;

    insert overwrite table web_search_month_stat
        select from_unixtime(SERVER_TIME,'yyyy'),month(from_unixtime(SERVER_TIME,'yyyy-MM-dd')),searchKey,count(*)
        from web803
        where searchKey is not null and length(searchKey)<100 and length(searchKey)>0
        group by from_unixtime(SERVER_TIME,'yyyy'),month(from_unixtime(SERVER_TIME,'yyyy-MM-dd')),searchKey;

 "
 
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS web_search_day_stat;
    CREATE TABLE web_search_day_stat (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          searchKey varchar(255) NOT NULL,
          total int(10) NOT NULL,
          KEY index_searchKey (searchKey),
          KEY index_time (time),
          KEY index_total (total)
    )DEFAULT CHARSET=utf8 ;

    DROP TABLE IF EXISTS web_log_search;
	CREATE TABLE web_log_search (
	   day varchar(255) NOT NULL,
	   time int(10) NOT NULL,
	   total int(10) NOT NULL
	 )DEFAULT CHARSET=utf8;
    
    
    DROP TABLE IF EXISTS web_search_week_stat;
    CREATE TABLE web_search_week_stat (
          year int(10) NOT NULL,
          week int(10) NOT NULL,
          searchKey varchar(255) NOT NULL,
          total int(10) NOT NULL,
          KEY index_searchKey (searchKey),
          KEY index_year (year),
          KEY index_week (week),
          KEY index_total (total)
    )DEFAULT CHARSET=utf8 ;
    
    DROP TABLE IF EXISTS web_search_month_stat;
    CREATE TABLE web_search_month_stat (
          year int(10) NOT NULL,
          month int(10) NOT NULL,
          searchKey varchar(255) NOT NULL,
          total int(10) NOT NULL,
          KEY index_searchKey (searchKey),
          KEY index_year (year),
          KEY index_month (month),
          KEY index_total (total)
    )DEFAULT CHARSET=utf8 ;

EOF

 sudo -u hdfs  sqoop export --connect  "jdbc:mysql://10.1.1.16:3306/stat_sdk?useUnicode=true&characterEncoding=utf-8" --username statsdkuser --password statsdkuser2111579711 --table web_search_day_stat --export-dir /user/hive/warehouse/web_search_day_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect  "jdbc:mysql://10.1.1.16:3306/stat_sdk?useUnicode=true&characterEncoding=utf-8" --username statsdkuser --password statsdkuser2111579711 --table web_log_search --export-dir /user/hive/warehouse/web_log_search --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect  "jdbc:mysql://10.1.1.16:3306/stat_sdk?useUnicode=true&characterEncoding=utf-8" --username statsdkuser --password statsdkuser2111579711 --table web_search_week_stat --export-dir /user/hive/warehouse/web_search_week_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect  "jdbc:mysql://10.1.1.16:3306/stat_sdk?useUnicode=true&characterEncoding=utf-8" --username statsdkuser --password statsdkuser2111579711 --table web_search_month_stat --export-dir /user/hive/warehouse/web_search_month_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  web_search_day_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  web_log_search  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  web_search_week_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  web_search_month_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 
 
 