#!/bin/bash

sudo -u hdfs hive -e "

    create EXTERNAL table if not exists sdk200007 (
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
        VERSION_NAME string,
        VERSION_CODE bigint
    )
    row format delimited
    fields terminated by '\\001'
    stored as textfile
    location '/apilogs/src/200007/';
  
    
    create EXTERNAL table if not exists MARKET200002 (
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
        EVENT_ID string,
        TITLE string,
        PACKAGE_NAME string,
        VERSION string,
        VERSION_CODE bigint,
        id int
    )
    row format delimited
    fields terminated by '\001'
    stored as textfile
    location '/apilogs/src/MARKET200002/';
    
    
      
    
    drop   table market_model_info2;
    create table market_model_info2 as  
        select CELL_PHONE_BRAND as brand,CELL_PHONE_MODEL as model,CELL_PHONE_CPU as cpu,CELL_PHONE_DENSITY as density ,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from MARKET200002 
        where CELL_PHONE_BRAND is not null  and EVENT_ID='100003'  and CELL_PHONE_DENSITY is not null
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,CELL_PHONE_CPU,CELL_PHONE_DENSITY;
    
    
    drop   table market_model_info;
    create table market_model_info as  
        select brand,model,cpu,density, sum(tmp.total) as total from (
            select * from market_model_info_history
            UNION ALL 
            select * from market_model_info2 
        
        ) tmp where model is not null and cpu is not null  and density is not null group by brand,model,cpu,density;
        
        drop   table market_model_info2;
    
    
     drop   table sdk_app_stat_tmp2;
     create  table sdk_app_stat_tmp2  as
        select CELL_PHONE_BRAND as brand,CELL_PHONE_MODEL as model,PACKAGE_NAME as package,VERSION_CODE as versioncode,count(DISTINCT CELL_PHONE_DEVICE_ID) as total 
        from MARKET200002
        where EVENT_ID='100003'
        group by CELL_PHONE_BRAND,CELL_PHONE_MODEL,PACKAGE_NAME,VERSION_CODE;
    
    drop   table sdk_app_stat_tmp;
    create  table sdk_app_stat_tmp as 
        select brand,model,package,versioncode, sum(tmp.total) as total from (
            select * from sdk_app_stat_history
            UNION ALL 
            select * from sdk_app_stat_tmp2 
        
        ) tmp  group by brand,model,package,versioncode;
    
    drop   table sdk_app_stat;
    create table sdk_app_stat as
     select brand,model,package,versioncode, total   FROM sdk_app_stat_tmp 
     where total>5 order by total desc;
    
    drop   table sdk_app_stat_tmp;
    drop   table sdk_app_stat_tmp2;
    
    
 "
 
 
 mysql -h10.1.1.2 -uapplanet_user -papplanet_user2111579711B\(\)\^ -D mzw <<EOF
    DROP TABLE IF EXISTS market_model_info;
    CREATE TABLE market_model_info (
          brand varchar(255) NOT NULL,
          model varchar(255) NOT NULL,
          cpu varchar(255) NOT NULL,
          density varchar(255) NOT NULL,
          total int(10) DEFAULT 0,
          KEY index_brand (brand),
          KEY index_model (model),
          KEY index_cpu (cpu),
          KEY index_density (density)
    )DEFAULT CHARSET=utf8 ;
    
EOF

 sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.2:3306/mzw --username applanet_user --password applanet_user2111579711B\(\)\^ --table market_model_info --export-dir /user/hive/warehouse/market_model_info --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"
mysql -h10.1.1.2 -uapplanet_user -papplanet_user2111579711B\(\)\^ -D mzw -e "ALTER TABLE  market_model_info  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

 
 
 
 mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF

    DROP TABLE IF EXISTS sdk_app_stat;
    CREATE TABLE IF NOT EXISTS sdk_app_stat (
          brand varchar(255) DEFAULT NULL,
          model varchar(255) DEFAULT NULL,
          package varchar(255) DEFAULT NULL,
          versioncode int(10) DEFAULT NULL,
          total int(10) DEFAULT NULL,
          KEY index_total (total),
          KEY index_package (package),
          KEY index_package_versioncode (package,versioncode),
          KEY index_model (model)
    ) ;

EOF

sudo -u hdfs  sqoop export --connect jdbc:mysql://10.1.1.16:3306/stat_sdk --username statsdkuser --password statsdkuser2111579711 --table sdk_app_stat --export-dir /user/hive/warehouse/sdk_app_stat --input-fields-terminated-by '\001' --input-null-string "\\\\N" --input-null-non-string "\\\\N"

mysql -h10.1.1.16 -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk -e "ALTER TABLE  sdk_app_stat  ADD id INT( 10 ) NOT NULL AUTO_INCREMENT PRIMARY KEY   FIRST ;"

mysql -h10.1.1.16  -ustatsdkuser -pstatsdkuser2111579711 -D stat_sdk <<EOF
    
    DROP TABLE IF EXISTS sdk_app_stat_copy;
    CREATE TABLE sdk_app_stat_copy (
       id INT(10) NOT NULL AUTO_INCREMENT,
       brand VARCHAR(255) NOT NULL,
       model VARCHAR(255) NOT NULL,
       package VARCHAR(255) NOT NULL,
       versioncode INT(10) NOT NULL,
       total INT(10) NOT NULL,
       vid INT(10) NOT NULL,
       vtitle CHAR(50) NOT NULL,
       version CHAR(20) NOT NULL,
       tid INT(10) DEFAULT NULL,
       prop INT(10) DEFAULT NULL,
       form INT(10) DEFAULT NULL,
       lang INT(10) NOT NULL DEFAULT '0',
       PRIMARY KEY (id),
       KEY index_total (total),
       KEY index_package (package),
       KEY index_package_versioncode (package,versioncode),
       KEY index_brand (brand),
       KEY index_model (model),
       KEY index_vid (vid),
       KEY index_prop (prop),
       KEY index_tid (tid),
       KEY index_form (form),
       KEY index_lang (lang)
     ) ENGINE=MYISAM  DEFAULT CHARSET=utf8;
    
    
    INSERT INTO sdk_app_stat_copy
        (
        brand, 
        model, 
        package, 
        versioncode, 
        total, 
        vid, 
        vtitle,
        VERSION,
        tid,
        prop,
        form,
        lang
        )
    SELECT  
        s.brand, 
        s.model, 
        s.package, 
        s.versioncode, 
        s.total,
        v.vid,
        v.vtitle,
        v.version,
        v.tid,
        v.prop,
        v.form,
        v.lang
    FROM (mzw_game_v v, sdk_app_stat s)
    WHERE  
        s.package=v.package AND 
        v.tid!=27 AND
        s.versioncode = v.versioncode ;


    DROP TABLE IF EXISTS sdk_app_stat_package;
    CREATE TABLE sdk_app_stat_package (
        id INT(10) NOT NULL AUTO_INCREMENT,
        model VARCHAR(255) NOT NULL,
        vid INT(10) NOT NULL,
        package VARCHAR(255) NOT NULL,
        total INT(10) NOT NULL,
        tid INT(10) DEFAULT NULL,
        prop INT(10) DEFAULT NULL,
        form INT(10) DEFAULT NULL,
        lang INT(10) NOT NULL DEFAULT '0',
        parentid TINYINT(4) NOT NULL DEFAULT '0',
        PRIMARY KEY (id),
        KEY index_model (model),
        KEY index_vid (vid),
        KEY index_package (package),
        KEY index_total (total),
        KEY index_prop (prop),
        KEY index_tid (tid),
        KEY index_form (form),
        KEY index_lang (lang),
        KEY index_parentid (parentid)
     ) ENGINE=MYISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
    
    
    INSERT INTO sdk_app_stat_package
            (
            model,
            vid,
            package,
            total,
            tid,
            prop,
            form,
            lang,
            parentid
             )
    SELECT
        c.model,
        c.vid,
        c.package,
        SUM(c.total) AS total,
        c.tid,
        c.prop,
        c.form,
        c.lang,
        t.parentid
      
    FROM sdk_app_stat_copy c LEFT JOIN mzw_game_type t ON c.tid=t.tid
    GROUP BY model,package
    ORDER BY model,total desc
        
    
    
EOF

 
 
 
 
