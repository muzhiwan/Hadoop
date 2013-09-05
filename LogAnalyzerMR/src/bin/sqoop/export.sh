#!/bin/bash

mysql -h10.1.1.2 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk < sdk_stat.sql

/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_game_user_info --export-dir /user/hive/warehouse/sdk_game_user_info --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_game_new_user_stat --export-dir /user/hive/warehouse/sdk_game_new_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_game_active_user_stat --export-dir /user/hive/warehouse/sdk_game_active_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table market_game_download_stat --export-dir /user/hive/warehouse/market_game_download_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_mobile_type_active_user_stat --export-dir /user/hive/warehouse/sdk_mobile_type_active_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_mobile_type_new_user_stat --export-dir /user/hive/warehouse/sdk_mobile_type_new_user_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
/opt/sqoop/bin/sqoop export --connect jdbc:mysql://10.1.1.2:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_app_stat --export-dir /user/hive/warehouse/sdk_app_stat --input-fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

