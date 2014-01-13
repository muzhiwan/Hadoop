


#!/bin/bash

sudo -u hdfs hive -e "
    
    
    drop    table mobile_type_month_top_old ;
    create  table mobile_type_month_top_old as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end as month,
            CELL_PHONE_BRAND as brand , CELL_PHONE_MODEL as model , CELL_PHONE_SYSTEM_VERSION as system_version , count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from sdk200001 
        where VERSION_CODE<10000000000
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end,CELL_PHONE_BRAND,CELL_PHONE_MODEL,CELL_PHONE_SYSTEM_VERSION;
    
    drop    table mobile_type_month_top_1 ;
    create  table mobile_type_month_top_1 as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end as month,
            CELL_PHONE_BRAND as brand , CELL_PHONE_MODEL as model , CELL_PHONE_SYSTEM_VERSION as system_version , count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from MARKET200001 
        where EVENT_CLASS_ID='MARKET200001' and APK_ID<10000000000 and APK_ID>0 and EVENT_ID='100001'
        group by case when CLIENT_TIME > SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM') end,CELL_PHONE_BRAND,CELL_PHONE_MODEL,CELL_PHONE_SYSTEM_VERSION;
    
    drop    table mobile_type_month_top ;
    create  table mobile_type_month_top as
        select month,brand,model,system_version ,sum(tmp.total) as total from (
        select * from mobile_type_month_top_old 
        UNION ALL 
        select * from mobile_type_month_top_1 
        ) tmp where month is not null group by month,brand,model,system_version order by month,total desc;
    
    
        
 "
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS mobile_type_month_top;
    CREATE TABLE mobile_type_month_top (
        month varchar(255) NOT NULL,
        brand varchar(255) NOT NULL,
        model varchar(255) NOT NULL,
        system_version varchar(255) NOT NULL,
        total int(10) NOT NULL,
        KEY index_month (month)
    );
    
 
EOF
 
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table mobile_type_month_top --export-dir /user/hive/warehouse/mobile_type_month_top --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  mobile_type_month_top  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
    
    