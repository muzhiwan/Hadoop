


#!/bin/bash

sudo -u hdfs hive -e "

    drop table user_download_record;
    create  table user_download_record as
        select case when CLIENT_TIME>SERVER_TIME*1000 or SERVER_TIME>(CLIENT_TIME/1000)+864000 then from_unixtime(SERVER_TIME,'yyyy-MM-dd') else from_unixtime(floor(CLIENT_TIME/1000),'yyyy-MM-dd') end as day,
          uid,APK_ID as vid , package_name as package,version_code as versioncode
        from MARKET200001 
        where EVENT_CLASS_ID='MARKET200001' and UID!='-1' and EVENT_ID='100003';
    
  
        
 "
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS user_download_record;
    CREATE TABLE user_download_record (
         day varchar(255) NOT NULL,
         uid varchar(255) NOT NULL,
         vid int(10) NOT NULL,
          package varchar(255) NOT NULL,
          versioncode int(10) NOT NULL,
          KEY index_uid (uid)
    );
    
    
   
EOF
 
 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table user_download_record --export-dir /user/hive/warehouse/user_download_record --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  user_download_record  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
    
    