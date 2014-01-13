#!/bin/bash

sudo -u hdfs hive -e "


    create EXTERNAL table if not exists StandSDK100001 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
        session string,
        totalmemsize string,
        avavilablememsize string,
        havedcn int,
        havewdj int,
        have360 int,
        haveyyb int,
        have91 int,
        havebaidu int,
        usetime int,
        playinstalled int,
        googleaccountlogined int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100001/';
    
    
    
    
    create EXTERNAL table if not exists StandSDK100002 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100002/';
    
    
    
    
    create EXTERNAL table if not exists StandSDK100003 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100003/';
    
    
    
    
    create EXTERNAL table if not exists StandSDK100004 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
         type int,
         errorcode int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100004/';
    
    
    
    
    
    create EXTERNAL table if not exists StandSDK100005 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
        savefilename string,
         share int,
         savefileversion int,
         savefileusername string,
         savefileuid string,
         type int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100005/';
    
    
    
    
    
    
    create EXTERNAL table if not exists StandSDK100006 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
         color int,
         shape int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100006/';
    
    
    
    
    
    
    create EXTERNAL table if not exists StandSDK100007 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
         errorcode int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100007/';
    
    
    
    
    

    create EXTERNAL table if not exists StandSDK100008 (
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
        gpu string,
        mac string,
        imei string,
        serialno string,
        EVENT_ID string,
        rooted int,
        TITLE string,
        PACKAGE_NAME string,
        VERSION_CODE int,
        VERSION string,
        fps int,
        totalmemsize int,
        avavilablememsize int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/StandSDK100008/';
    
    
    drop    table standsdk100002_day_stat ;
    create  table standsdk100002_day_stat as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            EVENT_ID,PACKAGE_NAME as package,count(*) as total 
        from StandSDK100002 
        where SERVER_TIME >0 and CLIENT_TIME >0 and PACKAGE_NAME is not null
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,EVENT_ID,PACKAGE_NAME;
    
    
    drop    table standsdk100003_day_stat ;
    create  table standsdk100003_day_stat as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            EVENT_ID,PACKAGE_NAME as package,count(*) as total 
        from StandSDK100003 
        where SERVER_TIME >0 and CLIENT_TIME >0  and PACKAGE_NAME is not null
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,EVENT_ID,PACKAGE_NAME;
    
    
    drop    table standsdk100004_day_stat ;
    create  table standsdk100004_day_stat as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            EVENT_ID,PACKAGE_NAME as package,count(*) as total 
        from StandSDK100004 
        where SERVER_TIME >0 and CLIENT_TIME >0  and PACKAGE_NAME is not null
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,EVENT_ID,PACKAGE_NAME;
    
    
    drop    table standsdk100005_day_stat ;
    create  table standsdk100005_day_stat as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            EVENT_ID,PACKAGE_NAME as package,count(*) as total 
        from StandSDK100005 
        where SERVER_TIME >0 and CLIENT_TIME >0  and PACKAGE_NAME is not null
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,EVENT_ID,PACKAGE_NAME;
    
    
    drop    table standsdk100006_day_stat ;
    create  table standsdk100006_day_stat as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            EVENT_ID,PACKAGE_NAME as package,count(*) as total 
        from StandSDK100006 
        where SERVER_TIME >0 and CLIENT_TIME >0  and PACKAGE_NAME is not null
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,EVENT_ID,PACKAGE_NAME;
    
    drop    table standsdk100007_day_stat ;
    create  table standsdk100007_day_stat as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
            EVENT_ID,PACKAGE_NAME as package,count(*) as total 
        from StandSDK100007 
        where SERVER_TIME >0 and CLIENT_TIME >0  and PACKAGE_NAME is not null
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end,EVENT_ID,PACKAGE_NAME;
    
    drop    table sdk_mobile_fps;
    create  table sdk_mobile_fps as
        select package_name as package,CELL_PHONE_MODEL as model,sum(fps) as fps_total,count(*) as fps_count,sum(fps)/count(*) as fps_avg
        from StandSDK100008 
        where package_name is not null and length(package_name)>10 and fps >0  and CLIENT_TIME >0 
        group by PACKAGE_NAME,CELL_PHONE_MODEL;
    
"


 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS sdk_mobile_fps;
    CREATE TABLE sdk_mobile_fps (
        package varchar(255) NOT NULL,
        model varchar(255) NOT NULL,
        fps_total int(10) NOT NULL,
        fps_count int(10) NOT NULL,
        fps_avg float NOT NULL,
        KEY index_package (package),
        KEY index_avg (fps_avg),
        KEY index_model (model)
    );
    
    DROP TABLE IF EXISTS standsdk100002_day_stat;
    CREATE TABLE standsdk100002_day_stat (
        day varchar(255) NOT NULL,
        EVENT_ID varchar(255) NOT NULL,
        package varchar(255) NOT NULL,
        total int(10) NOT NULL,
        KEY index_day (day)
    );
    
    DROP TABLE IF EXISTS standsdk100003_day_stat;
    CREATE TABLE standsdk100003_day_stat (
        day varchar(255) NOT NULL,
        EVENT_ID varchar(255) NOT NULL,
        package varchar(255) NOT NULL,
        total int(10) NOT NULL,
        KEY index_day (day)
    );
    
    DROP TABLE IF EXISTS standsdk100004_day_stat;
    CREATE TABLE standsdk100004_day_stat (
        day varchar(255) NOT NULL,
        EVENT_ID varchar(255) NOT NULL,
        package varchar(255) NOT NULL,
        total int(10) NOT NULL,
        KEY index_day (day)
    );
    
    DROP TABLE IF EXISTS standsdk100005_day_stat;
    CREATE TABLE standsdk100005_day_stat (
        day varchar(255) NOT NULL,
        EVENT_ID varchar(255) NOT NULL,
        package varchar(255) NOT NULL,
        total int(10) NOT NULL,
        KEY index_day (day)
    );
    
    DROP TABLE IF EXISTS standsdk100006_day_stat;
    CREATE TABLE standsdk100006_day_stat (
        day varchar(255) NOT NULL,
        EVENT_ID varchar(255) NOT NULL,
        package varchar(255) NOT NULL,
        total int(10) NOT NULL,
        KEY index_day (day)
    );
    
    DROP TABLE IF EXISTS standsdk100007_day_stat;
    CREATE TABLE standsdk100007_day_stat (
        day varchar(255) NOT NULL,
        EVENT_ID varchar(255) NOT NULL,
        package varchar(255) NOT NULL,
        total int(10) NOT NULL,
        KEY index_day (day)
    );
    
    
 
EOF
 
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_mobile_fps --export-dir /user/hive/warehouse/sdk_mobile_fps --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table standsdk100002_day_stat --export-dir /user/hive/warehouse/standsdk100002_day_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table standsdk100003_day_stat --export-dir /user/hive/warehouse/standsdk100003_day_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table standsdk100004_day_stat --export-dir /user/hive/warehouse/standsdk100004_day_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table standsdk100005_day_stat --export-dir /user/hive/warehouse/standsdk100005_day_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table standsdk100006_day_stat --export-dir /user/hive/warehouse/standsdk100006_day_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table standsdk100007_day_stat --export-dir /user/hive/warehouse/standsdk100007_day_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"


 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_mobile_fps  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  standsdk100002_day_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  standsdk100003_day_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  standsdk100004_day_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  standsdk100005_day_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  standsdk100006_day_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  standsdk100007_day_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
 
 
 
 
 
