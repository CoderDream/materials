/*
Navicat MySQL Data Transfer

Source Server         : node03
Source Server Version : 50547
Source Host           : node03:3306
Source Database       : bigdata

Target Server Type    : MYSQL
Target Server Version : 50547
File Encoding         : 65001

Date: 2018-06-05 17:26:33
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for location_info
-- ----------------------------
DROP TABLE IF EXISTS `location_info`;
CREATE TABLE `location_info` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `location` varchar(50) DEFAULT NULL,
  `counts` varchar(50) DEFAULT NULL,
  `access_date` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=116 DEFAULT CHARSET=utf8;
