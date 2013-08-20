drop   table game_user_info;
create table game_user_info (
	id int(10) NOT NULL AUTO_INCREMENT COMMENT,
	CELL_PHONE_DEVICE_ID varchar(255), 
	PACKAGE_NAME varchar(255), 
	VERSION_CODE varchar(255), 
	first_time bigint, 
	CELL_PHONE_BRAND varchar(255), 
	CELL_PHONE_MODEL varchar(255), 
	CELL_PHONE_CPU varchar(255), 
	CELL_PHONE_DENSITY varchar(255), 
	CELL_PHONE_SCREEN_WIDTH varchar(255), 
	CELL_PHONE_SCREEN_HEIGHT varchar(255)
);
drop   table game_active_user_stat;
create table game_active_user_stat(
	id int(10) NOT NULL AUTO_INCREMENT COMMENT,
	stat_day varchar(255), 
	PACKAGE_NAME varchar(255), 
	VERSION_CODE varchar(255), 
	user_count int
);
drop   table game_new_user_stat;
create table game_new_user_stat(
	id int(10) NOT NULL AUTO_INCREMENT COMMENT,
	stat_day varchar(255), 
	PACKAGE_NAME varchar(255), 
	VERSION_CODE varchar(255), 
	user_count int
);

drop   table mobile_type_active_user_stat;
create table mobile_type_active_user_stat(
	stat_day varchar(255), 
	PACKAGE_NAME varchar(255), 
	VERSION_CODE varchar(255), 
	CELL_PHONE_BRAND varchar(255), 
	CELL_PHONE_MODEL varchar(255), 
	user_count int
);

drop   table mobile_type_new_user_stat;
create table mobile_type_new_user_stat(
	stat_day varchar(255), 
	PACKAGE_NAME varchar(255), 
	VERSION_CODE varchar(255), 
	CELL_PHONE_BRAND varchar(255), 
	CELL_PHONE_MODEL varchar(255), 
	user_count int
);

drop   table game_download_stat;
create table game_download_stat(
	stat_day varchar(255), 
	APK_ID varchar(255), 
	PACKAGE_NAME varchar(255), 
	VERSION_CODE varchar(255), 
	download_count int
);

drop   table sdk_app_stat;
create table sdk_app_stat(
	CELL_PHONE_BRAND varchar(255), 
	CELL_PHONE_MODEL varchar(255), 
	PACKAGE_NAME varchar(255), 
	VERSION_NAME varchar(255), 
	count int
);







