#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists sdk200001 (
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
        APK_SOURCE string,
        CATEGORY string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200001/';
  
    drop table download_record;
    create table if not exists download_record (
        deviceid string,
        package_name string
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table download_record 
        select CELL_PHONE_DEVICE_ID, PACKAGE_NAME
        from sdk200001 
        where CELL_PHONE_DEVICE_ID is not null and PACKAGE_NAME is not null;
        
    drop table mahout_record;
    create table if not exists mahout_record (
        deviceid string,
        package_names string
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
   
   insert overwrite table mahout_record 
       SELECT deviceid,concat_ws('|', collect_set(package_name)) FROM download_record GROUP BY deviceid;

 INSERT OVERWRITE DIRECTORY '/user/hdfs/mahout/src' SELECT package_names FROM mahout_record;
    
 "
 sudo -u hdfs mahout fpg -i /user/hdfs/mahout/src -o /user/hdfs/mahout/out  -method mapreduce -regex '[\|]'  -s 2 -k 10
 
 txt_file="/home/hdfs/mahout.txt"
 rm -f "${txt_file}"
 for seq_file in $( sudo -u hdfs hadoop fs -lsr /user/hdfs/mahout/out/frequentpatterns/part-r-0* | awk '{print $8}'); do
       sudo -u hdfs mahout seqdumper -i  "${seq_file}" >> "${txt_file}"
done
 
 parse_file="/home/hdfs/ret.txt"
 python /opt/py/sdk_stat/parse_mahout_data.py  "${txt_file}" "${parse_file}" 
 
  sudo -u hdfs hadoop fs -rmr /user/hdfs/mahout/result/*
  sudo -u hdfs hadoop fs -put "${parse_file}" /user/hdfs/mahout/result/
 
 sudo -u hdfs hive -e "
     drop table recommender;
     create EXTERNAL table if not exists recommender (
        package string,
        pkgs string
    )
    row format delimited
    fields terminated by ':'
    stored as textfile
    location '/user/hdfs/mahout/result/';
 
 "
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS recommender;
    CREATE TABLE recommender (
          package varchar(255) NOT NULL,
          pkgs varchar(1000) NOT NULL,
          KEY index_package (package)
    );

EOF

 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table recommender --export-dir /user/hdfs/mahout/result --input-fields-terminated-by ':' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  recommender  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 
 
 