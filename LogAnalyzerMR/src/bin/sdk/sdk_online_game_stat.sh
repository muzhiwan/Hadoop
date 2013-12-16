#!/bin/bash

events=(SDK200003 SDK200001 SDK200002)
month=`date -d now +"%m"`
day=`date -d now  +"%d"`
for event in "${events[@]}"; do
    /home/hdfs/shell/log_collector.sh "${event}" "${month}" "${day}"
done

srcDir="/apilogs/src/SDK200003"
destDir="/user/root/out/SDKGAMEUserInfo"

sudo -u hdfs hadoop fs -mkdir "${destDir}"

starttime=`date +"%s"`
outputPath="${destDir}/${starttime}"

sudo -u hdfs hadoop jar /home/hdfs/shell/LogAnalyzerMR.jar com.muzhiwan.hadoop.LogAnalyzerMR SDKGAMEUserInfo "${srcDir}" "${outputPath}"

sudo -u hdfs  hive -e "
    drop table sdk_online_game_user_info;
    create table if not exists sdk_online_game_user_info (
        device_id string,
        appkey string,
        first_time bigint,
        package string,
        versioncode string,
        brand string,
        model string,
        imei string,
        mac string
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    Load Data Inpath '${outputPath}/part-r-*' Overwrite Into Table sdk_online_game_user_info;
    
    drop table sdk_online_game_user_stat_tmp1;
    create table if not exists sdk_online_game_user_stat_tmp1 (
        day string,
        time bigint,
        appkey string,
        new_user_count bigint,
        active_user_count bigint,
        reg_user_count bigint,
        login_user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_online_game_user_stat_tmp1 
    select from_unixtime(first_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(first_time,'yyyy-MM-dd'),'yyyy-MM-dd'),regexp_replace(appkey,'MZWAPPKEY','52706321c620d') ,count(DISTINCT device_id ),0,0,0
    from sdk_online_game_user_info 
    where length(device_id)=36
    group by from_unixtime(first_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(first_time,'yyyy-MM-dd'),'yyyy-MM-dd'),regexp_replace(appkey,'MZWAPPKEY','52706321c620d');


    drop table SDK_200003;
    create EXTERNAL table if not exists SDK_200003 (
        server_time bigint,
        client_ip string,
        client_area string,
        number string,
        eventid string,
        client_time bigint,
        ip string,
        appkey string,
        brand string,
        model string,
        packagename string,
        versioncode string,
        sdkversion bigint,
        uniqueid string,
        imei string,
        mac string,
        systemversion string
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/SDK200003/';
  
    drop table SDK_200001;
    create EXTERNAL table if not exists SDK_200001 (
        server_time bigint,
        client_ip string,
        client_area string,
        number string,
        eventid string,
        client_time bigint,
        ip string,
        appkey string,
        brand string,
        model string,
        packagename string,
        versioncode string,
        sdkversion bigint,
        uniqueid string,
        imei string,
        mac string,
        systemversion string,
        username string,
        type  int,
        channel  int,
        errorcode  int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/SDK200001/';
  
    drop table sdk_online_game_user_stat_tmp2;
    create table if not exists sdk_online_game_user_stat_tmp2 (
        day string,
        time bigint,
        appkey string,
        new_user_count bigint,
        active_user_count bigint,
        reg_user_count bigint,
        login_user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_online_game_user_stat_tmp2 
    select from_unixtime(server_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd'),regexp_replace(appkey,'MZWAPPKEY','52706321c620d'),0,count(DISTINCT uniqueid ),0,0
    from SDK_200003 
    where length(uniqueid)=36 and eventid='001' and systemversion is not null
    group by from_unixtime(server_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd'),regexp_replace(appkey,'MZWAPPKEY','52706321c620d');

    drop table sdk_online_game_user_stat_tmp3;
    create table if not exists sdk_online_game_user_stat_tmp3 (
        day string,
        time bigint,
        appkey string,
        new_user_count bigint,
        active_user_count bigint,
        reg_user_count bigint,
        login_user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_online_game_user_stat_tmp3 
    select from_unixtime(server_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd'),appkey,0,0,count(DISTINCT username),0
    from SDK_200001 
    where type=1 and eventid='002'
    group by from_unixtime(server_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd'),appkey;

    
    drop table sdk_online_game_user_stat_tmp4;
    create table if not exists sdk_online_game_user_stat_tmp4 (
        day string,
        time bigint,
        appkey string,
        new_user_count bigint,
        active_user_count bigint,
        reg_user_count bigint,
        login_user_count bigint
    )
    Row Format Delimited
    Fields Terminated By '\t'
    stored as textfile;

    insert overwrite table sdk_online_game_user_stat_tmp4 
    select from_unixtime(server_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd'),appkey,0,0,0,count(DISTINCT username)
    from SDK_200001 
    where type=0 and eventid='002'
    group by from_unixtime(server_time,'yyyy-MM-dd'),unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd'),appkey;

    

    drop    table sdk_online_game_user_stat;
    create  table sdk_online_game_user_stat as
        select day ,time,appkey,sum(tmp.new_user_count) as new_user_count ,sum(tmp.active_user_count) as active_user_count,sum(tmp.reg_user_count) as reg_user_count ,sum(tmp.login_user_count) as login_user_count from (
        select * from sdk_online_game_user_stat_tmp1 
        UNION ALL 
        select * from sdk_online_game_user_stat_tmp2  
        UNION ALL 
        select * from sdk_online_game_user_stat_tmp3  
        UNION ALL 
        select * from sdk_online_game_user_stat_tmp4  
        ) tmp where time>0 group by day,time,appkey order by day desc;
    
    drop table sdk_online_game_user_stat_tmp1;
    drop table sdk_online_game_user_stat_tmp2;
    drop table sdk_online_game_user_stat_tmp3;
    drop table sdk_online_game_user_stat_tmp4;
    
    
    
    
    
    
    
    
    drop    table sdk_online_game_new_user1 ;
    create  table sdk_online_game_new_user1  as  
    select from_unixtime(first_time,'yyyy-MM-dd')  as day, unix_timestamp(from_unixtime(first_time,'yyyy-MM-dd'),'yyyy-MM-dd') as time ,regexp_replace(appkey,'MZWAPPKEY','52706321c620d') as appkey ,device_id as username ,'open' as type
    from sdk_online_game_user_info 
    where length(device_id)=36 and from_unixtime(first_time,'yyyy-MM-dd')=date_sub (from_unixtime(unix_timestamp(),'yyyy-MM-dd'),2);

    drop    table sdk_online_game_new_user2 ;
    create  table sdk_online_game_new_user2  as  
    select DISTINCT from_unixtime(server_time,'yyyy-MM-dd') as day,unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd') as time ,appkey,username,'login' as type
    from SDK_200001 
    where type=1 and eventid='002' and from_unixtime(server_time,'yyyy-MM-dd')=date_sub (from_unixtime(unix_timestamp(),'yyyy-MM-dd'),2);
    
    drop    table sdk_online_game_new_user;
    create  table sdk_online_game_new_user as
        select * from (
        select * from sdk_online_game_new_user1 
        UNION ALL 
        select * from sdk_online_game_new_user2  
        ) tmp;
    
    drop    table sdk_online_game_new_user1 ;
    drop    table sdk_online_game_new_user2 ;
    
    
    
    drop    table sdk_online_game_active_user1 ;
    create  table sdk_online_game_active_user1  as  
    select DISTINCT from_unixtime(server_time,'yyyy-MM-dd') as day,unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd') as time  ,regexp_replace(appkey,'MZWAPPKEY','52706321c620d') as appkey ,uniqueid as username ,'open' as type
    from SDK_200003 
    where length(uniqueid)=36 and from_unixtime(server_time,'yyyy-MM-dd')=date_sub (from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1);

    
    drop    table sdk_online_game_active_user2 ;
    create  table sdk_online_game_active_user2  as  
    select DISTINCT from_unixtime(server_time,'yyyy-MM-dd') as day,unix_timestamp(from_unixtime(server_time,'yyyy-MM-dd'),'yyyy-MM-dd') as time ,appkey,username,'login' as type
    from SDK_200001 
    where type=0 and eventid='002' and from_unixtime(server_time,'yyyy-MM-dd')=date_sub (from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1);
    
    drop    table sdk_online_game_active_user;
    create  table sdk_online_game_active_user as
        select * from (
        select * from sdk_online_game_active_user1 
        UNION ALL 
        select * from sdk_online_game_active_user2  
        ) tmp;
    
    drop    table sdk_online_game_active_user1 ;
    drop    table sdk_online_game_active_user2 ;
    
    
    
    
    
    
"

sudo -u hdfs hadoop fs -rmr "${destDir}"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF
    
    DROP TABLE IF EXISTS sdk_online_game_user_stat;
    CREATE TABLE sdk_online_game_user_stat (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          appkey varchar(255) NOT NULL,
          new_user_count int(10) NOT NULL,
          active_user_count int(10) NOT NULL,
          reg_user_count int(10) NOT NULL,
          login_user_count int(10) NOT NULL,
          KEY index_time (time)
    );

    DROP TABLE IF EXISTS sdk_online_game_new_user;
    CREATE TABLE sdk_online_game_new_user (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          appkey varchar(255) NOT NULL,
          username varchar(255) NOT NULL,
          type varchar(32) NOT NULL,
          KEY index_appkey (appkey),
          KEY index_type (type),
          KEY index_time (time)
    )DEFAULT CHARSET=utf8 ;

    DROP TABLE IF EXISTS sdk_online_game_active_user;
    CREATE TABLE sdk_online_game_active_user (
          day varchar(255) NOT NULL,
          time int(10) NOT NULL,
          appkey varchar(255) NOT NULL,
          username varchar(255) NOT NULL,
          type varchar(32) NOT NULL,
          KEY index_appkey (appkey),
          KEY index_type (type),
          KEY index_time (time)
    )DEFAULT CHARSET=utf8 ;
    
EOF

 sudo -u hdfs  sqoop export --connect  "jdbc:mysql://10.1.1.16:3306/stat_sdk?useUnicode=true&characterEncoding=utf-8" --username statsdkuser --password statsdkuser2111579711 --table sdk_online_game_user_stat --export-dir /user/hive/warehouse/sdk_online_game_user_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect  "jdbc:mysql://10.1.1.16:3306/stat_sdk?useUnicode=true&characterEncoding=utf-8" --username statsdkuser --password statsdkuser2111579711 --table sdk_online_game_new_user --export-dir /user/hive/warehouse/sdk_online_game_new_user --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
 sudo -u hdfs  sqoop export --connect  "jdbc:mysql://10.1.1.16:3306/stat_sdk?useUnicode=true&characterEncoding=utf-8" --username statsdkuser --password statsdkuser2111579711 --table sdk_online_game_active_user --export-dir /user/hive/warehouse/sdk_online_game_active_user --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"


mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_online_game_user_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_online_game_new_user  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_online_game_active_user  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"





