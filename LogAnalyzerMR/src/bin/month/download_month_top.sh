


#!/bin/bash

sudo -u hdfs hive -e "
    
    drop table download_month_top_old ;
    create  table download_month_top_old as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end as month,
            APK_ID as apkid ,count(*) as total 
        from sdk200003 
        where VERSION_CODE<10000000000 and APK_ID<10000000000 and APK_ID>0 
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end,APK_ID;
    
    drop table download_month_top_1 ;
    create  table download_month_top_1 as 
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end as month,
            APK_ID as apkid ,count(*) as total 
        from MARKET200001 
        where EVENT_CLASS_ID='MARKET200001' and APK_ID<10000000000 and APK_ID>0 and EVENT_ID='100003'
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end,APK_ID;
    
    
    drop table download_month_top_web ;
    create  table download_month_top_web as 
        select from_unixtime(SERVER_TIME,'yyyy-MM') as month,gameId as apkid ,count(*) as total
        from web814 
        where gameId>0
        group by from_unixtime(SERVER_TIME,'yyyy-MM') ,gameId;
        
    drop table download_month_top ;
    create  table download_month_top as 
        select month,apkid ,sum(tmp.total) as total from (
        select * from download_month_top_old 
        UNION ALL 
        select * from download_month_top_1 
        UNION ALL 
        select * from download_month_top_web 
        ) tmp  where month is not null and apkid<10000000000 and apkid>0  group by month,apkid;
    
    
 "
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS download_month_top;
    CREATE TABLE download_month_top (
        month varchar(255) NOT NULL,
        apkid int(10) NOT NULL,
        total int(10) NOT NULL,
        KEY index_month (month)
    );
    
   
EOF
 
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table download_month_top --export-dir /user/hive/warehouse/download_month_top --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  download_month_top  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

  mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF
    
DROP TABLE IF EXISTS download_month_top_copy;
CREATE TABLE download_month_top_copy (
   MONTH VARCHAR(255) NOT NULL,
   apkid INT(10) NOT NULL,
   title VARCHAR(100) NOT NULL,
   vtitle VARCHAR(100) NOT NULL,
   total INT(10) NOT NULL,
   tid INT(10) NOT NULL,
   parentid INT(10) NOT NULL,
   KEY index_month (MONTH)
 ) ENGINE=MYISAM DEFAULT CHARSET=utf8;
 
INSERT INTO download_month_top_copy
            (MONTH,
             apkid,
             title,
             vtitle,
             total,
             tid,
             parentid)
        SELECT
          w.month,
          w.apkid,
          t.title,
          v.vtitle,
          w.total,
          t.tid,
          t.parentid
        FROM download_month_top w
          INNER JOIN mzw_game_v v
            ON w.apkid = v.vid
          INNER JOIN mzw_game_type t
            ON v.tid = t.tid
        ORDER BY w.total DESC
        LIMIT 1000;


EOF
 
    
    