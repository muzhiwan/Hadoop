#!/bin/bash


srcDir="/apilogs/src/100003"
destDir="/user/root/out/MobileSDKUserInfo"

sudo -u hdfs hadoop fs -mkdir "${destDir}"

starttime=`date +"%s"`
outputPath="${destDir}/${starttime}"

sudo -u hdfs hadoop jar /home/hdfs/shell/LogAnalyzerMR.jar com.muzhiwan.hadoop.LogAnalyzerMR MobileSDKUserInfo "${srcDir}" "${outputPath}"

sudo -u hdfs  hive -e "
    drop table sdk_user_info;
    create table if not exists sdk_user_info (
        device_id string,
        first_time bigint,
        brand string,
        model string,
        cpu string,
        density string,
        screen_width bigint,
        screen_height bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    Load Data Inpath '${outputPath}/part-r-*' Overwrite Into Table sdk_user_info;
    
    drop table sdk_new_user_stat_tmp;
    create table if not exists sdk_new_user_stat_tmp (
        day string,
        total bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_new_user_stat_tmp 
    select from_unixtime(floor(first_time/1000),'yyyy-MM-dd') ,count(DISTINCT device_id )   
    from sdk_user_info 
    group by from_unixtime(floor(first_time/1000),'yyyy-MM-dd');

    drop   table sdk_new_user_stat;
    create table if not exists sdk_new_user_stat (
        day string,
        time bigint,
        new_user_count bigint,
        active_user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table sdk_new_user_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,total,0
     FROM sdk_new_user_stat_tmp 
     where total>0 order by total desc;
    
    drop   table sdk_new_user_stat_tmp;
    
    create EXTERNAL table if not exists sdk100003 (
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
        VERSION_CODE bigint,
        VERSION string,
        SESSION_ID string,
        UID string,
        sdksession string
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/100003/';
  
    drop   table sdk_active_user_stat_tmp;
    create table if not exists sdk_active_user_stat_tmp (
        day string,
        total bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_active_user_stat_tmp 
        select 
            case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end
            ,count(DISTINCT CELL_PHONE_DEVICE_ID )          
        from sdk100003
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000  then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end ;
    
    drop   table sdk_active_user_stat;
    create table if not exists sdk_active_user_stat (
        day string,
        time bigint,
        new_user_count bigint,
        active_user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;
    
     insert overwrite table sdk_active_user_stat 
     SELECT day,unix_timestamp(day,'yyyy-MM-dd')  as time,0,total FROM sdk_active_user_stat_tmp where total>0 order by total desc;
    
    drop   table sdk_active_user_stat_tmp;
    
    create  table sdk_user_stat as
        select day ,time,sum(tmp.new_user_count) as new_user_count ,sum(tmp.active_user_count) as active_user_count from (
        select * from sdk_new_user_stat 
        UNION ALL 
        select * from sdk_active_user_stat  
        ) tmp where time>0 group by day,time order by day desc;
    
    drop table sdk_new_user_stat;
    drop table sdk_active_user_stat;
    
"

sudo -u hdfs hadoop fs -rmr "${destDir}"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF
    DROP TABLE IF EXISTS sdk_user_info;
    CREATE TABLE sdk_user_info (
          device_id varchar(255) NOT NULL,
          first_time bigint(10) NOT NULL,
          brand varchar(255) NOT NULL,
          model varchar(255) NOT NULL,
          cpu varchar(255) NOT NULL,
          density varchar(255) NOT NULL,
          screen_width int(10) NOT NULL,
          screen_height int(10) NOT NULL
    );
    
    DROP TABLE IF EXISTS sdk_user_stat;
    CREATE TABLE sdk_user_stat (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          new_user_count int(10) NOT NULL,
          active_user_count int(10) NOT NULL,
          KEY index_time (time)
    );
    
EOF

#sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_user_info --export-dir /user/hive/warehouse/sdk_user_info --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_user_stat --export-dir /user/hive/warehouse/sdk_user_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

#mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_user_info  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_user_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"





