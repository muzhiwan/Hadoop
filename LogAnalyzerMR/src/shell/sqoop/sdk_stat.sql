DROP TABLE IF EXISTS `market_game_download_stat`;

CREATE TABLE `market_game_download_stat` (
  `day` varchar(255) NOT NULL,
  `time` int(10) NOT NULL,
  `apkid` int(10) NOT NULL,
  `package` varchar(255) NOT NULL,
  `versioncode` int(10) NOT NULL,
  `versionname` varchar(255) NOT NULL,
  `total` int(10) NOT NULL,
  KEY `index_apkid` (`apkid`),
  KEY `index_package` (`package`),
  KEY `index_total` (`total`),
  KEY `index_package_versioncode` (`package`,`versioncode`)
);

/*Table structure for table `sdk_app_stat` */

DROP TABLE IF EXISTS `sdk_app_stat`;

CREATE TABLE IF NOT EXISTS `sdk_app_stat` (
  `brand` varchar(255) DEFAULT NULL,
  `model` varchar(255) DEFAULT NULL,
  `package` varchar(255) DEFAULT NULL,
  `versioncode` int(10) DEFAULT NULL,
  `versionname` varchar(255) DEFAULT NULL,
  `total` int(10) DEFAULT NULL,
  KEY `index_total` (`total`),
  KEY `index_package` (`package`),
  KEY `index_package_versioncode` (`package`,`versioncode`)
) ;

/*Table structure for table `sdk_game_active_user_stat` */

DROP TABLE IF EXISTS `sdk_game_active_user_stat`;

CREATE TABLE `sdk_game_active_user_stat` (
  `day` varchar(255) NOT NULL,
  `time` int(10) NOT NULL,
  `package` varchar(255) NOT NULL,
  `versioncode` int(10) NOT NULL,
  `versionname` varchar(255) NOT NULL,
  `total` int(10) NOT NULL,
  KEY `index_time` (`time`),
  KEY `index_total` (`total`),
  KEY `index_package` (`package`),
  KEY `index_package_versioncode` (`package`,`versioncode`)
);

/*Table structure for table `sdk_game_new_user_stat` */

DROP TABLE IF EXISTS `sdk_game_new_user_stat`;

CREATE TABLE `sdk_game_new_user_stat` (
  `day` varchar(255) NOT NULL,
  `time` int(10) NOT NULL,
  `package` varchar(255) NOT NULL,
  `versioncode` int(10) NOT NULL,
  `versionname` varchar(255) NOT NULL,
  `total` int(10) NOT NULL,
  KEY `index_time` (`time`),
  KEY `index_total` (`total`),
  KEY `index_package` (`package`),
  KEY `index_package_versioncode` (`package`,`versioncode`)
) ;

/*Table structure for table `sdk_game_user_info` */

DROP TABLE IF EXISTS `sdk_game_user_info`;

CREATE TABLE `sdk_game_user_info` (
  `device_id` varchar(255) NOT NULL,
  `package` varchar(255) NOT NULL,
  `versioncode` int(10) NOT NULL,
  `first_time` bigint(10) NOT NULL,
  `versionname` varchar(255) NOT NULL,
  `brand` varchar(255) NOT NULL,
  `model` varchar(255) NOT NULL,
  `cpu` varchar(255) NOT NULL,
  `density` varchar(255) NOT NULL,
  `screen_width` int(10) NOT NULL
  `screen_height` int(10) NOT NULL
);

/*Table structure for table `sdk_mobile_type_active_user_stat` */

DROP TABLE IF EXISTS `sdk_mobile_type_active_user_stat`;

CREATE TABLE `sdk_mobile_type_active_user_stat` (
  `day` varchar(255) NOT NULL,
  `time` int(10) NOT NULL,
  `package` varchar(255) NOT NULL,
  `versioncode` int(10) NOT NULL,
  `versionname` varchar(255) NOT NULL,
  `brand` varchar(255) NOT NULL,
  `model` varchar(255) NOT NULL,
  `total` int(10) NOT NULL,
  KEY `index_time` (`time`),
  KEY `index_total` (`total`),
  KEY `index_model` (`model`)
);

/*Table structure for table `sdk_mobile_type_new_user_stat` */

DROP TABLE IF EXISTS `sdk_mobile_type_new_user_stat`;

CREATE TABLE `sdk_mobile_type_new_user_stat` (
  `day` varchar(255) NOT NULL,
  `time` int(10) NOT NULL,
  `package` varchar(255) NOT NULL,
  `versioncode` int(10) NOT NULL,
  `versionname` varchar(255) NOT NULL,
  `brand` varchar(255) NOT NULL,
  `model` varchar(255) NOT NULL,
  `total` int(10) NOT NULL,
  KEY `index_time` (`time`),
  KEY `index_total` (`total`),
  KEY `index_model` (`model`)
);


