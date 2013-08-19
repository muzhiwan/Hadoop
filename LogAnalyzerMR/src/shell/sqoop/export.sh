#!/bin/bash

mysql -h10.1.1.2 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk < mysql.sql

/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table game_user_info --export-dir /user/hive/warehouse/game_user_info --input-fields-terminated-by '\t'
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table game_active_user_stat --export-dir /user/hive/warehouse/game_active_user_stat --input-fields-terminated-by '\t'
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table game_new_user_stat --export-dir /user/hive/warehouse/game_new_user_stat --input-fields-terminated-by '\t'
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table mobile_type_active_user_stat --export-dir /user/hive/warehouse/mobile_type_active_user_stat --input-fields-terminated-by '\t'
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table mobile_type_new_user_stat --export-dir /user/hive/warehouse/mobile_type_new_user_stat --input-fields-terminated-by '\t'
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table game_download_stat --export-dir /user/hive/warehouse/game_download_stat --input-fields-terminated-by '\t'

