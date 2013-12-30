#!/bin/bash

sudo -u hdfs hive -e "
    create EXTERNAL table if not exists client401 (
        SERVER_TIME bigint,
        CLIENT_IP string,
        CLIENT_AREA string,
        CLIENT_TIME string,
        version string,
        mac string,
        flag int,
        os string,
        cpubit string,
        type int
    )
    row format delimited
    fields terminated by '\t'
    stored as textfile
    location '/apilogs/src/401/';
    
    drop table pc_client_install;
    create table pc_client_install as 
        select from_unixtime(SERVER_TIME,'yyyy-MM-dd') as day,unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') as time,version,count(*) as install,0 as uninstall
        from client401 
        where type=0
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,version;
        
    drop table pc_client_uninstall;
    create table pc_client_uninstall as 
        select from_unixtime(SERVER_TIME,'yyyy-MM-dd') as day,unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') as time,version,0 as install,count(*) as uninstall
        from client401 
        where type=1
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,version;
        
    drop table pc_client_stat;
    create table pc_client_stat as 
         select day ,time,version ,sum(tmp.install) as install,sum(tmp.uninstall) as uninstall from (
        select * from pc_client_uninstall 
        UNION ALL 
        select * from pc_client_install 
        ) tmp group by day,time,version order by day desc;
    
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    create EXTERNAL table if not exists client501 (
        SERVER_TIME bigint,
        CLIENT_IP string,
        CLIENT_AREA string,
        CLIENT_TIME string,
        version string,
        mac string,
        flag int,
        os string,
        cpubit string,
        type int
    )
    row format delimited
    fields terminated by '\t'
    stored as textfile
    location '/apilogs/src/501/';
    
    drop table pc_helper_install;
    create table pc_helper_install as 
        select from_unixtime(SERVER_TIME,'yyyy-MM-dd') as day,unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') as time,version,count(*) as install,0 as uninstall
        from client501 
        where type=0
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,version;
        
    drop table pc_helper_uninstall;
    create table pc_helper_uninstall as 
        select from_unixtime(SERVER_TIME,'yyyy-MM-dd') as day,unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') as time,version,0 as install,count(*) as uninstall
        from client501 
        where type=1
        group by from_unixtime(SERVER_TIME,'yyyy-MM-dd'),unix_timestamp(from_unixtime(SERVER_TIME,'yyyy-MM-dd'),'yyyy-MM-dd') ,version;
        
    drop table pc_helper_stat;
    create table pc_helper_stat as 
         select day ,time,version ,sum(tmp.install) as install,sum(tmp.uninstall) as uninstall from (
        select * from pc_helper_uninstall 
        UNION ALL 
        select * from pc_helper_install 
        ) tmp group by day,time,version order by day desc;
    
   
    
 "
 
  mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    
    DROP TABLE IF EXISTS pc_client_stat;
    CREATE TABLE pc_client_stat (
         day varchar(255) NOT NULL,
         time int(10) NOT NULL,
          version varchar(255) NOT NULL,
          install int(10) NOT NULL,
          uninstall int(10) NOT NULL,
          KEY index_version (version)
    );
    
    
    DROP TABLE IF EXISTS pc_helper_stat;
    CREATE TABLE pc_helper_stat (
         day varchar(255) NOT NULL,
         time int(10) NOT NULL,
          version varchar(255) NOT NULL,
          install int(10) NOT NULL,
          uninstall int(10) NOT NULL,
          KEY index_version (version)
    );
    
   
EOF
 
sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table pc_client_stat --export-dir /user/hive/warehouse/pc_client_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table pc_helper_stat --export-dir /user/hive/warehouse/pc_helper_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  pc_client_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"
mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  pc_helper_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 
 
