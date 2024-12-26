-- MySQL dump 10.13  Distrib 5.6.51, for Linux (x86_64)
--
-- Host: localhost    Database: dags
-- ------------------------------------------------------
-- Server version	5.6.51

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `dags`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `dags` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin */;

USE `dags`;

--
-- Table structure for table `auth_group`
--

DROP TABLE IF EXISTS `auth_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(150) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `auth_group`
--

LOCK TABLES `auth_group` WRITE;
/*!40000 ALTER TABLE `auth_group` DISABLE KEYS */;
/*!40000 ALTER TABLE `auth_group` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `auth_group_permissions`
--

DROP TABLE IF EXISTS `auth_group_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_group_permissions_group_id_permission_id_0cd325b0_uniq` (`group_id`,`permission_id`),
  KEY `auth_group_permissio_permission_id_84c5c92e_fk_auth_perm` (`permission_id`),
  CONSTRAINT `auth_group_permissio_permission_id_84c5c92e_fk_auth_perm` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`),
  CONSTRAINT `auth_group_permissions_group_id_b120cbf9_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `auth_group_permissions`
--

LOCK TABLES `auth_group_permissions` WRITE;
/*!40000 ALTER TABLE `auth_group_permissions` DISABLE KEYS */;
/*!40000 ALTER TABLE `auth_group_permissions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `auth_permission`
--

DROP TABLE IF EXISTS `auth_permission`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_permission` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_bin NOT NULL,
  `content_type_id` int(11) NOT NULL,
  `codename` varchar(100) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_permission_content_type_id_codename_01ab375a_uniq` (`content_type_id`,`codename`),
  CONSTRAINT `auth_permission_content_type_id_2f476e4b_fk_django_co` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=73 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `auth_permission`
--

LOCK TABLES `auth_permission` WRITE;
/*!40000 ALTER TABLE `auth_permission` DISABLE KEYS */;
INSERT INTO `auth_permission` VALUES (1,'Can add log entry',1,'add_logentry'),(2,'Can change log entry',1,'change_logentry'),(3,'Can delete log entry',1,'delete_logentry'),(4,'Can view log entry',1,'view_logentry'),(5,'Can add permission',2,'add_permission'),(6,'Can change permission',2,'change_permission'),(7,'Can delete permission',2,'delete_permission'),(8,'Can view permission',2,'view_permission'),(9,'Can add group',3,'add_group'),(10,'Can change group',3,'change_group'),(11,'Can delete group',3,'delete_group'),(12,'Can view group',3,'view_group'),(13,'Can add user',4,'add_user'),(14,'Can change user',4,'change_user'),(15,'Can delete user',4,'delete_user'),(16,'Can view user',4,'view_user'),(17,'Can add content type',5,'add_contenttype'),(18,'Can change content type',5,'change_contenttype'),(19,'Can delete content type',5,'delete_contenttype'),(20,'Can view content type',5,'view_contenttype'),(21,'Can add session',6,'add_session'),(22,'Can change session',6,'change_session'),(23,'Can delete session',6,'delete_session'),(24,'Can view session',6,'view_session'),(25,'Can add orderd schedule',7,'add_orderdschedule'),(26,'Can change orderd schedule',7,'change_orderdschedule'),(27,'Can delete orderd schedule',7,'delete_orderdschedule'),(28,'Can view orderd schedule',7,'view_orderdschedule'),(29,'Can add pipeline',8,'add_pipeline'),(30,'Can change pipeline',8,'change_pipeline'),(31,'Can delete pipeline',8,'delete_pipeline'),(32,'Can view pipeline',8,'view_pipeline'),(33,'Can add processor',9,'add_processor'),(34,'Can change processor',9,'change_processor'),(35,'Can delete processor',9,'delete_processor'),(36,'Can view processor',9,'view_processor'),(37,'Can add project',10,'add_project'),(38,'Can change project',10,'change_project'),(39,'Can delete project',10,'delete_project'),(40,'Can view project',10,'view_project'),(41,'Can add ready task',11,'add_readytask'),(42,'Can change ready task',11,'change_readytask'),(43,'Can delete ready task',11,'delete_readytask'),(44,'Can view ready task',11,'view_readytask'),(45,'Can add run history',12,'add_runhistory'),(46,'Can change run history',12,'change_runhistory'),(47,'Can delete run history',12,'delete_runhistory'),(48,'Can view run history',12,'view_runhistory'),(49,'Can add schedule',13,'add_schedule'),(50,'Can change schedule',13,'change_schedule'),(51,'Can delete schedule',13,'delete_schedule'),(52,'Can view schedule',13,'view_schedule'),(53,'Can add task',14,'add_task'),(54,'Can change task',14,'change_task'),(55,'Can delete task',14,'delete_task'),(56,'Can view task',14,'view_task'),(57,'Can add upload history',15,'add_uploadhistory'),(58,'Can change upload history',15,'change_uploadhistory'),(59,'Can delete upload history',15,'delete_uploadhistory'),(60,'Can view upload history',15,'view_uploadhistory'),(61,'Can add perm history',16,'add_permhistory'),(62,'Can change perm history',16,'change_permhistory'),(63,'Can delete perm history',16,'delete_permhistory'),(64,'Can view perm history',16,'view_permhistory'),(65,'Can add visit record',17,'add_visitrecord'),(66,'Can change visit record',17,'change_visitrecord'),(67,'Can delete visit record',17,'delete_visitrecord'),(68,'Can view visit record',17,'view_visitrecord'),(69,'Can add rerun history',18,'add_rerunhistory'),(70,'Can change rerun history',18,'change_rerunhistory'),(71,'Can delete rerun history',18,'delete_rerunhistory'),(72,'Can view rerun history',18,'view_rerunhistory');
/*!40000 ALTER TABLE `auth_permission` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `auth_user`
--

DROP TABLE IF EXISTS `auth_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `password` varchar(128) COLLATE utf8_bin NOT NULL,
  `last_login` datetime(6) DEFAULT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `username` varchar(150) COLLATE utf8_bin NOT NULL,
  `first_name` varchar(150) COLLATE utf8_bin NOT NULL,
  `last_name` varchar(150) COLLATE utf8_bin NOT NULL,
  `email` varchar(254) COLLATE utf8_bin NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `date_joined` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `auth_user`
--

LOCK TABLES `auth_user` WRITE;
/*!40000 ALTER TABLE `auth_user` DISABLE KEYS */;
INSERT INTO `auth_user` VALUES (1,'pbkdf2_sha256$320000$I2TbWAHTEH9QCWQJn035mN$LN7C+zuAJB6Z4i4mJ4UbyJf3cqt+cva7W6SOPiDsYMA=','2022-02-12 11:34:20.553015',0,'super','','','',0,1,'2022-02-12 11:06:51.227981');
/*!40000 ALTER TABLE `auth_user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `auth_user_groups`
--

DROP TABLE IF EXISTS `auth_user_groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_groups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_user_groups_user_id_group_id_94350c0c_uniq` (`user_id`,`group_id`),
  KEY `auth_user_groups_group_id_97559544_fk_auth_group_id` (`group_id`),
  CONSTRAINT `auth_user_groups_group_id_97559544_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`),
  CONSTRAINT `auth_user_groups_user_id_6a12ed8b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `auth_user_groups`
--

LOCK TABLES `auth_user_groups` WRITE;
/*!40000 ALTER TABLE `auth_user_groups` DISABLE KEYS */;
/*!40000 ALTER TABLE `auth_user_groups` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `auth_user_user_permissions`
--

DROP TABLE IF EXISTS `auth_user_user_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_user_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_user_user_permissions_user_id_permission_id_14a6b632_uniq` (`user_id`,`permission_id`),
  KEY `auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm` (`permission_id`),
  CONSTRAINT `auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`),
  CONSTRAINT `auth_user_user_permissions_user_id_a95ead1b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `auth_user_user_permissions`
--

LOCK TABLES `auth_user_user_permissions` WRITE;
/*!40000 ALTER TABLE `auth_user_user_permissions` DISABLE KEYS */;
/*!40000 ALTER TABLE `auth_user_user_permissions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `django_admin_log`
--

DROP TABLE IF EXISTS `django_admin_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_admin_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `action_time` datetime(6) NOT NULL,
  `object_id` longtext COLLATE utf8_bin,
  `object_repr` varchar(200) COLLATE utf8_bin NOT NULL,
  `action_flag` smallint(5) unsigned NOT NULL,
  `change_message` longtext COLLATE utf8_bin NOT NULL,
  `content_type_id` int(11) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `django_admin_log_content_type_id_c4bce8eb_fk_django_co` (`content_type_id`),
  KEY `django_admin_log_user_id_c564eba6_fk_auth_user_id` (`user_id`),
  CONSTRAINT `django_admin_log_content_type_id_c4bce8eb_fk_django_co` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`),
  CONSTRAINT `django_admin_log_user_id_c564eba6_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `django_admin_log`
--

LOCK TABLES `django_admin_log` WRITE;
/*!40000 ALTER TABLE `django_admin_log` DISABLE KEYS */;
/*!40000 ALTER TABLE `django_admin_log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `django_content_type`
--

DROP TABLE IF EXISTS `django_content_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_content_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_label` varchar(100) COLLATE utf8_bin NOT NULL,
  `model` varchar(100) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `django_content_type_app_label_model_76bd3d3b_uniq` (`app_label`,`model`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `django_content_type`
--

LOCK TABLES `django_content_type` WRITE;
/*!40000 ALTER TABLE `django_content_type` DISABLE KEYS */;
INSERT INTO `django_content_type` VALUES (1,'admin','logentry'),(3,'auth','group'),(2,'auth','permission'),(4,'auth','user'),(5,'contenttypes','contenttype'),(7,'horae','orderdschedule'),(16,'horae','permhistory'),(8,'horae','pipeline'),(9,'horae','processor'),(10,'horae','project'),(11,'horae','readytask'),(18,'horae','rerunhistory'),(12,'horae','runhistory'),(13,'horae','schedule'),(14,'horae','task'),(15,'horae','uploadhistory'),(17,'horae','visitrecord'),(6,'sessions','session');
/*!40000 ALTER TABLE `django_content_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `django_migrations`
--

DROP TABLE IF EXISTS `django_migrations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_migrations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app` varchar(255) COLLATE utf8_bin NOT NULL,
  `name` varchar(255) COLLATE utf8_bin NOT NULL,
  `applied` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `django_migrations`
--

LOCK TABLES `django_migrations` WRITE;
/*!40000 ALTER TABLE `django_migrations` DISABLE KEYS */;
INSERT INTO `django_migrations` VALUES (1,'contenttypes','0001_initial','2022-02-12 09:55:29.760659'),(2,'auth','0001_initial','2022-02-12 09:55:30.034396'),(3,'admin','0001_initial','2022-02-12 09:55:30.098764'),(4,'admin','0002_logentry_remove_auto_add','2022-02-12 09:55:30.111518'),(5,'admin','0003_logentry_add_action_flag_choices','2022-02-12 09:55:30.137176'),(6,'contenttypes','0002_remove_content_type_name','2022-02-12 09:55:30.262228'),(7,'auth','0002_alter_permission_name_max_length','2022-02-12 09:55:30.299810'),(8,'auth','0003_alter_user_email_max_length','2022-02-12 09:55:30.349364'),(9,'auth','0004_alter_user_username_opts','2022-02-12 09:55:30.368640'),(10,'auth','0005_alter_user_last_login_null','2022-02-12 09:55:30.414506'),(11,'auth','0006_require_contenttypes_0002','2022-02-12 09:55:30.420583'),(12,'auth','0007_alter_validators_add_error_messages','2022-02-12 09:55:30.444473'),(13,'auth','0008_alter_user_username_max_length','2022-02-12 09:55:30.494517'),(14,'auth','0009_alter_user_last_name_max_length','2022-02-12 09:55:30.560091'),(15,'auth','0010_alter_group_name_max_length','2022-02-12 09:55:30.607097'),(16,'auth','0011_update_proxy_permissions','2022-02-12 09:55:30.627969'),(17,'auth','0012_alter_user_first_name_max_length','2022-02-12 09:55:30.675934'),(18,'horae','0001_initial','2022-02-12 09:55:30.965477'),(19,'horae','0002_permhistory','2022-02-12 09:55:31.037482'),(20,'horae','0003_visitrecord','2022-02-12 09:55:31.107807'),(21,'horae','0004_project_parent_id','2022-02-12 09:55:31.138005'),(22,'horae','0005_project_type','2022-02-12 09:55:31.176561'),(23,'horae','0006_processor_project_id','2022-02-12 09:55:31.219662'),(24,'horae','0007_auto_20180828_0534','2022-02-12 09:55:31.238239'),(25,'horae','0008_auto_20180828_0535','2022-02-12 09:55:31.375948'),(26,'horae','0009_auto_20180829_0912','2022-02-12 09:55:31.435597'),(27,'horae','0010_task_version_id','2022-02-12 09:55:31.460889'),(28,'horae','0011_auto_20180907_0601','2022-02-12 09:55:31.488545'),(29,'horae','0012_rerunhistory','2022-02-12 09:55:31.555390'),(30,'horae','0013_auto_20180917_0736','2022-02-12 09:55:31.606755'),(31,'sessions','0001_initial','2022-02-12 09:55:31.631710');
/*!40000 ALTER TABLE `django_migrations` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `django_session`
--

DROP TABLE IF EXISTS `django_session`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_session` (
  `session_key` varchar(40) COLLATE utf8_bin NOT NULL,
  `session_data` longtext COLLATE utf8_bin NOT NULL,
  `expire_date` datetime(6) NOT NULL,
  PRIMARY KEY (`session_key`),
  KEY `django_session_expire_date_a5c62663` (`expire_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `django_session`
--

LOCK TABLES `django_session` WRITE;
/*!40000 ALTER TABLE `django_session` DISABLE KEYS */;
INSERT INTO `django_session` VALUES ('rtz6s2mft9eg2tnio9wzsf74novlwv5e','.eJxVjMsOgjAUBf-la9P0AbR16Z5vaHoftagpCYWV8d-FhIVuZ-act4hpW0vcGi9xInEVWlx-GSR8cj0EPVK9zxLnui4TyCORp21ynIlft7P9OyiplX2dyFjvNGZDOGhLVpP3g0UXui5wzjtzGHKvmFWPAERGA6FDpYMF5cXnC-ksODA:1nIqfU:poGX_OleCNJSBXqSBdEh21G0wVtt6qFKqPRQKSqEeBg','2022-02-26 11:34:20.559165');
/*!40000 ALTER TABLE `django_session` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_orderdschedule`
--

DROP TABLE IF EXISTS `horae_orderdschedule`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_orderdschedule` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` int(11) NOT NULL,
  `run_time` varchar(20) COLLATE utf8_bin NOT NULL,
  `status` int(11) NOT NULL,
  `pl_id` int(11) NOT NULL,
  `end_time` datetime(6) DEFAULT NULL,
  `start_time` datetime(6) NOT NULL,
  `init_time` datetime(6) DEFAULT NULL,
  `ordered_id` varchar(255) COLLATE utf8_bin NOT NULL,
  `run_tag` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `horae_orderdschedule_task_id_run_time_f1002164_uniq` (`task_id`,`run_time`),
  KEY `horae_orderdschedule_task_id_9cc63c0f` (`task_id`),
  KEY `horae_orderdschedule_run_time_a79f520c` (`run_time`),
  KEY `horae_orderdschedule_pl_id_5fac503f` (`pl_id`),
  KEY `horae_orderdschedule_ordered_id_1c1218e0` (`ordered_id`),
  KEY `horae_orderdschedule_run_tag_3b6ea9e3` (`run_tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_orderdschedule`
--

LOCK TABLES `horae_orderdschedule` WRITE;
/*!40000 ALTER TABLE `horae_orderdschedule` DISABLE KEYS */;
/*!40000 ALTER TABLE `horae_orderdschedule` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_permhistory`
--

DROP TABLE IF EXISTS `horae_permhistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_permhistory` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `resource_type` varchar(16) COLLATE utf8_bin NOT NULL,
  `resource_id` bigint(20) NOT NULL,
  `permission` varchar(200) COLLATE utf8_bin NOT NULL,
  `status` smallint(5) unsigned NOT NULL,
  `deadline` datetime(6) DEFAULT NULL,
  `update_time` datetime(6) NOT NULL,
  `create_time` datetime(6) NOT NULL,
  `reason` varchar(512) COLLATE utf8_bin NOT NULL,
  `odps_perm_type` int(11) DEFAULT NULL,
  `applicant_id` int(11) NOT NULL,
  `grantor_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `horae_permhistory_applicant_id_f1a62a4b_fk_auth_user_id` (`applicant_id`),
  KEY `horae_permhistory_grantor_id_f433b952_fk_auth_user_id` (`grantor_id`),
  CONSTRAINT `horae_permhistory_applicant_id_f1a62a4b_fk_auth_user_id` FOREIGN KEY (`applicant_id`) REFERENCES `auth_user` (`id`),
  CONSTRAINT `horae_permhistory_grantor_id_f433b952_fk_auth_user_id` FOREIGN KEY (`grantor_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_permhistory`
--

LOCK TABLES `horae_permhistory` WRITE;
/*!40000 ALTER TABLE `horae_permhistory` DISABLE KEYS */;
/*!40000 ALTER TABLE `horae_permhistory` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_pipeline`
--

DROP TABLE IF EXISTS `horae_pipeline`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_pipeline` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) COLLATE utf8_bin NOT NULL,
  `owner_id` int(11) NOT NULL,
  `ct_time` varchar(250) COLLATE utf8_bin NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `enable` int(11) NOT NULL,
  `type` int(11) NOT NULL,
  `email_to` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `sms_to` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `tag` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `life_cycle` varchar(50) COLLATE utf8_bin NOT NULL,
  `monitor_way` int(11) NOT NULL,
  `private` int(11) NOT NULL,
  `project_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `horae_pipeline_owner_id_9f78d258` (`owner_id`),
  KEY `horae_pipeline_enable_5c0a7b4e` (`enable`),
  KEY `horae_pipeline_type_c329b07f` (`type`),
  KEY `horae_pipeline_project_id_b5307dd5` (`project_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_pipeline`
--

LOCK TABLES `horae_pipeline` WRITE;
/*!40000 ALTER TABLE `horae_pipeline` DISABLE KEYS */;
INSERT INTO `horae_pipeline` VALUES (1,'申万测试流程1',1,'0 * * * *','2022-02-12 20:13:24.000000',0,0,'','申万测试流程','','','20220219',1,1,1),(2,'申万测试流程',1,'0 * * * *','2022-02-12 20:14:19.000000',0,0,'','申万测试流程','','','20220219',1,1,2),(3,'流程测试1',1,'','2022-02-13 02:25:47.000000',0,0,'','','','','20220220',1,1,2),(4,'中经社测试流程',1,'','2022-02-13 02:26:09.000000',0,0,'','','','','20220220',1,1,4);
/*!40000 ALTER TABLE `horae_pipeline` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_processor`
--

DROP TABLE IF EXISTS `horae_processor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_processor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(250) COLLATE utf8_bin NOT NULL,
  `type` int(11) NOT NULL,
  `template` longtext COLLATE utf8_bin,
  `update_time` datetime(6) NOT NULL,
  `description` varchar(1024) COLLATE utf8_bin NOT NULL,
  `config` longtext COLLATE utf8_bin,
  `owner_id` int(11) NOT NULL,
  `private` int(11) DEFAULT NULL,
  `ap` int(11) DEFAULT NULL,
  `tag` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `tpl_files` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `project_id` int(11) DEFAULT NULL,
  `input_config` longtext COLLATE utf8_bin,
  `output_config` longtext COLLATE utf8_bin,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `horae_processor_type_bcef6d9b` (`type`),
  KEY `horae_processor_owner_id_8600ee3a` (`owner_id`),
  KEY `horae_processor_private_4eb03d80` (`private`),
  KEY `horae_processor_ap_2e37f9e2` (`ap`),
  KEY `horae_processor_project_id_27d0b0de` (`project_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_processor`
--

LOCK TABLES `horae_processor` WRITE;
/*!40000 ALTER TABLE `horae_processor` DISABLE KEYS */;
INSERT INTO `horae_processor` VALUES (1,'样本筛选',1,'','2022-02-12 20:09:36.000000','','=H0',1,1,0,'','',-1,'input_0=hdfsH0\n','output_0=hdfsH0\n'),(2,'proc_for_SQL计算_290',1,'sql','2022-02-13 02:02:01.000000','','',1,1,0,NULL,NULL,0,NULL,NULL),(3,'proc_for_SQL计算_251',1,'sql','2022-02-13 02:11:02.000000','','',1,1,0,NULL,NULL,0,NULL,NULL),(4,'test',1,'','2022-02-13 02:12:28.000000','','=H0',1,1,0,'','',3,'input_0=hiveH0\n','output_0=hiveH0\n'),(5,'proc_for_SQL计算252_292',1,'sql','2022-02-13 02:14:37.000000','','',1,1,0,NULL,NULL,0,NULL,NULL);
/*!40000 ALTER TABLE `horae_processor` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_project`
--

DROP TABLE IF EXISTS `horae_project`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_project` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(20) COLLATE utf8_bin NOT NULL,
  `owner_id` int(11) NOT NULL,
  `is_default` int(11) NOT NULL,
  `description` varchar(10240) COLLATE utf8_bin DEFAULT NULL,
  `parent_id` int(11) NOT NULL,
  `type` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `horae_project_name_type_39e7c016_uniq` (`name`,`type`),
  KEY `horae_project_owner_id_f8bb0004` (`owner_id`),
  KEY `horae_project_parent_id_66cf7597` (`parent_id`),
  KEY `horae_project_type_27d19d98` (`type`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_project`
--

LOCK TABLES `horae_project` WRITE;
/*!40000 ALTER TABLE `horae_project` DISABLE KEYS */;
INSERT INTO `horae_project` VALUES (1,'默认项目',1,1,'',0,0),(2,'申万测试流程',1,0,'',0,0),(3,'急速行情',1,0,'test3',-1,1),(4,'中经社',1,0,'',0,0);
/*!40000 ALTER TABLE `horae_project` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_readytask`
--

DROP TABLE IF EXISTS `horae_readytask`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_readytask` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pl_id` int(11) NOT NULL,
  `schedule_id` int(11) NOT NULL,
  `status` int(11) NOT NULL,
  `update_time` datetime(6) DEFAULT NULL,
  `type` int(11) NOT NULL,
  `init_time` datetime(6) DEFAULT NULL,
  `retry_count` int(11) DEFAULT NULL,
  `retried_count` int(11) DEFAULT NULL,
  `run_time` varchar(20) COLLATE utf8_bin NOT NULL,
  `server_tag` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `task_id` int(11) NOT NULL,
  `pid` int(11) NOT NULL,
  `owner_id` int(11) NOT NULL,
  `run_server` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `task_handler` varchar(4096) COLLATE utf8_bin DEFAULT NULL,
  `is_trigger` int(11) NOT NULL,
  `next_task_ids` varchar(200) COLLATE utf8_bin DEFAULT NULL,
  `prev_task_ids` varchar(200) COLLATE utf8_bin DEFAULT NULL,
  `work_dir` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `horae_readytask_task_id_run_time_schedul_b61da62e_uniq` (`task_id`,`run_time`,`schedule_id`,`is_trigger`),
  KEY `horae_readytask_pl_id_39d05218` (`pl_id`),
  KEY `horae_readytask_schedule_id_ee6ea05f` (`schedule_id`),
  KEY `horae_readytask_run_time_aed168d5` (`run_time`),
  KEY `horae_readytask_task_id_db46c660` (`task_id`),
  KEY `horae_readytask_pid_03d97c6d` (`pid`),
  KEY `horae_readytask_owner_id_10798e6e` (`owner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_readytask`
--

LOCK TABLES `horae_readytask` WRITE;
/*!40000 ALTER TABLE `horae_readytask` DISABLE KEYS */;
/*!40000 ALTER TABLE `horae_readytask` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_rerunhistory`
--

DROP TABLE IF EXISTS `horae_rerunhistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_rerunhistory` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` int(11) NOT NULL,
  `run_time` varchar(20) COLLATE utf8_bin NOT NULL,
  `pl_id` int(11) NOT NULL,
  `start_time` datetime(6) DEFAULT NULL,
  `end_time` datetime(6) DEFAULT NULL,
  `status` int(11) NOT NULL,
  `schedule_id` int(11) NOT NULL,
  `tag` int(11) DEFAULT NULL,
  `type` int(11) NOT NULL,
  `task_handler` varchar(4096) COLLATE utf8_bin DEFAULT NULL,
  `run_server` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `server_tag` varchar(50) COLLATE utf8_bin NOT NULL,
  `pl_name` varchar(1024) COLLATE utf8_bin NOT NULL,
  `task_name` varchar(1024) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`),
  KEY `horae_rerunhistory_task_id_4cfadc9b` (`task_id`),
  KEY `horae_rerunhistory_run_time_6ae9f1c3` (`run_time`),
  KEY `horae_rerunhistory_pl_id_aaf378e1` (`pl_id`),
  KEY `horae_rerunhistory_status_738cc697` (`status`),
  KEY `horae_rerunhistory_schedule_id_e5e01eb4` (`schedule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_rerunhistory`
--

LOCK TABLES `horae_rerunhistory` WRITE;
/*!40000 ALTER TABLE `horae_rerunhistory` DISABLE KEYS */;
/*!40000 ALTER TABLE `horae_rerunhistory` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_runhistory`
--

DROP TABLE IF EXISTS `horae_runhistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_runhistory` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` int(11) NOT NULL,
  `run_time` varchar(20) COLLATE utf8_bin NOT NULL,
  `pl_id` int(11) NOT NULL,
  `start_time` datetime(6) DEFAULT NULL,
  `end_time` datetime(6) DEFAULT NULL,
  `status` int(11) NOT NULL,
  `schedule_id` int(11) NOT NULL,
  `tag` int(11) DEFAULT NULL,
  `type` int(11) NOT NULL,
  `task_handler` varchar(4096) COLLATE utf8_bin DEFAULT NULL,
  `run_server` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `server_tag` varchar(50) COLLATE utf8_bin NOT NULL,
  `pl_name` varchar(1024) COLLATE utf8_bin NOT NULL,
  `task_name` varchar(1024) COLLATE utf8_bin NOT NULL,
  `cpu` int(11) DEFAULT NULL,
  `mem` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `horae_runhistory_task_id_run_time_1e737ba6_uniq` (`task_id`,`run_time`),
  KEY `horae_runhistory_task_id_e14fb07d` (`task_id`),
  KEY `horae_runhistory_run_time_dcc5af16` (`run_time`),
  KEY `horae_runhistory_pl_id_1a7e48d3` (`pl_id`),
  KEY `horae_runhistory_status_6102154c` (`status`),
  KEY `horae_runhistory_schedule_id_b641ab8c` (`schedule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_runhistory`
--

LOCK TABLES `horae_runhistory` WRITE;
/*!40000 ALTER TABLE `horae_runhistory` DISABLE KEYS */;
/*!40000 ALTER TABLE `horae_runhistory` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_schedule`
--

DROP TABLE IF EXISTS `horae_schedule`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_schedule` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` int(11) NOT NULL,
  `run_time` varchar(20) COLLATE utf8_bin NOT NULL,
  `status` int(11) NOT NULL,
  `pl_id` int(11) NOT NULL,
  `end_time` datetime(6) DEFAULT NULL,
  `start_time` datetime(6) NOT NULL,
  `init_time` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `horae_schedule_task_id_run_time_22eabfac_uniq` (`task_id`,`run_time`),
  KEY `horae_schedule_task_id_50a69b94` (`task_id`),
  KEY `horae_schedule_run_time_21dce471` (`run_time`),
  KEY `horae_schedule_pl_id_ab5942a6` (`pl_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_schedule`
--

LOCK TABLES `horae_schedule` WRITE;
/*!40000 ALTER TABLE `horae_schedule` DISABLE KEYS */;
/*!40000 ALTER TABLE `horae_schedule` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_task`
--

DROP TABLE IF EXISTS `horae_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pl_id` int(11) NOT NULL,
  `pid` int(11) NOT NULL,
  `next_task_ids` varchar(10240) COLLATE utf8_bin DEFAULT NULL,
  `prev_task_ids` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `over_time` int(11) NOT NULL,
  `name` varchar(250) COLLATE utf8_bin NOT NULL,
  `config` longtext COLLATE utf8_bin,
  `retry_count` int(11) DEFAULT NULL,
  `last_run_time` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `priority` int(11) NOT NULL,
  `except_ret` int(11) NOT NULL,
  `description` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `server_tag` varchar(50) COLLATE utf8_bin NOT NULL,
  `version_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `horae_task_pl_id_4fab8bcb` (`pl_id`),
  KEY `horae_task_pid_9f41019c` (`pid`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_task`
--

LOCK TABLES `horae_task` WRITE;
/*!40000 ALTER TABLE `horae_task` DISABLE KEYS */;
INSERT INTO `horae_task` VALUES (1,2,1,'2,8','',0,'通过算子筛选数据','',0,'202202130027',6,0,'通过算子筛选数据','ALL',5),(2,2,1,'6','1',0,'通过算子权重计算','',0,'202202130027',6,0,'通过算子进行权重和参数计算','ALL',5),(5,2,1,'','6,7',0,'流程计算','input_0=hdfs\r\noutput_0=hdfs',0,'202202130202',6,0,'','ALL',5),(6,2,3,'5','2',0,'SQL计算','',0,'202202130211',6,0,'test_sql','test',0),(7,2,5,'5','8',0,'灰度-SQL计算','',0,'202202130214',6,0,'test_sql','test',0),(8,2,1,'7','1',0,'灰度-计算权重','input_0=hdfs\r\noutput_0=hdfs',0,'202202130217',6,0,'','ALL',5);
/*!40000 ALTER TABLE `horae_task` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_uploadhistory`
--

DROP TABLE IF EXISTS `horae_uploadhistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_uploadhistory` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `processor_id` int(11) NOT NULL,
  `status` int(11) NOT NULL,
  `update_time` datetime(6) DEFAULT NULL,
  `upload_time` datetime(6) DEFAULT NULL,
  `upload_user_id` int(11) NOT NULL,
  `version` varchar(250) COLLATE utf8_bin NOT NULL,
  `description` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `git_url` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
  `name` varchar(250) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `horae_uploadhistory_processor_id_name_37f59ca3_uniq` (`processor_id`,`name`),
  KEY `horae_uploadhistory_processor_id_33bae6a1` (`processor_id`),
  KEY `horae_uploadhistory_upload_user_id_748ba9c4` (`upload_user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_uploadhistory`
--

LOCK TABLES `horae_uploadhistory` WRITE;
/*!40000 ALTER TABLE `horae_uploadhistory` DISABLE KEYS */;
INSERT INTO `horae_uploadhistory` VALUES (5,1,0,'2022-02-13 00:26:33.000000','2022-02-13 00:26:33.000000',1,'1.0.0','测试插件',NULL,'1.0.0'),(6,1,0,'2022-02-13 02:17:01.000000','2022-02-13 02:17:01.000000',1,'1.0.1','',NULL,'1.0.1'),(7,1,0,'2022-02-13 02:17:10.000000','2022-02-13 02:17:10.000000',1,'1.0.2','',NULL,'1.0.2');
/*!40000 ALTER TABLE `horae_uploadhistory` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `horae_visitrecord`
--

DROP TABLE IF EXISTS `horae_visitrecord`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `horae_visitrecord` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app` varchar(128) COLLATE utf8_bin NOT NULL,
  `uri` varchar(128) COLLATE utf8_bin NOT NULL,
  `param` varchar(128) COLLATE utf8_bin NOT NULL,
  `description` varchar(1024) COLLATE utf8_bin NOT NULL,
  `ip` varchar(64) COLLATE utf8_bin NOT NULL,
  `visit_time` datetime(6) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `horae_visitrecord_user_id_f4e5338f_fk_auth_user_id` (`user_id`),
  CONSTRAINT `horae_visitrecord_user_id_f4e5338f_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=154 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `horae_visitrecord`
--

LOCK TABLES `horae_visitrecord` WRITE;
/*!40000 ALTER TABLE `horae_visitrecord` DISABLE KEYS */;
INSERT INTO `horae_visitrecord` VALUES (1,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:17:10.481308',1),(2,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:19:06.834970',1),(3,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:29:51.502286',1),(4,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:31:29.969888',1),(5,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:39:56.904826',1),(6,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:40:03.717010',1),(7,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 11:40:59.602203',1),(8,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 11:43:17.069929',1),(9,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:47:17.530816',1),(10,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:54:35.411604',1),(11,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 11:54:40.181328',1),(12,'processor','/processor/create/','','','192.168.189.1','2022-02-12 11:58:11.312005',1),(13,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:01:51.229661',1),(14,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 12:02:24.463827',1),(15,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:02:27.311241',1),(16,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 12:02:28.666063',1),(17,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:02:35.633885',1),(18,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 12:02:38.236449',1),(19,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:02:51.499164',1),(20,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:04:20.401693',1),(21,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:05:58.037712',1),(22,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:06:01.475533',1),(23,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:07:50.983018',1),(24,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:08:08.923426',1),(25,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:08:51.315789',1),(26,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:09:36.878212',1),(27,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:09:46.242720',1),(28,'processor','/processor/update/1/','','','192.168.189.1','2022-02-12 12:09:53.269972',1),(29,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:10:01.095561',1),(30,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:10:46.927105',1),(31,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:10:51.293775',1),(32,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:10:58.351569',1),(33,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:13:00.836255',1),(34,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:13:24.558221',1),(35,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:13:50.491906',1),(36,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 12:14:05.032921',1),(37,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:14:44.500169',1),(38,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:14:47.126389',1),(39,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:28:44.400454',1),(40,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:28:57.969135',1),(41,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:29:01.340839',1),(42,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 12:52:38.559703',1),(43,'processor','/processor/create/','','','192.168.189.1','2022-02-12 12:52:42.446105',1),(44,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:52:55.461170',1),(45,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:53:20.055042',1),(46,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:53:31.156450',1),(47,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 12:53:32.731810',1),(48,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 14:04:41.496500',1),(49,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 14:17:00.358875',1),(50,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 14:42:06.386715',1),(51,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 14:50:43.314309',1),(52,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 15:45:18.109998',1),(53,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 15:45:19.830976',1),(54,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 15:45:22.365646',1),(55,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 16:16:22.464819',1),(56,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 16:17:45.283443',1),(57,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 16:17:46.459478',1),(58,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 16:19:04.690840',1),(59,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 16:26:33.129211',1),(60,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 16:26:49.033382',1),(61,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 16:26:50.143552',1),(62,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 16:27:15.875208',1),(63,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 16:27:25.910258',1),(64,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 16:27:27.044299',1),(65,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 16:27:44.564670',1),(66,'pipeline','/pipeline/update_task/1/','','','192.168.189.1','2022-02-12 16:28:10.033820',1),(67,'pipeline','/pipeline/update_task/1/','','','192.168.189.1','2022-02-12 17:06:38.936717',1),(68,'pipeline','/pipeline/update_task/1/','','','192.168.189.1','2022-02-12 17:06:42.251762',1),(69,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 17:06:44.352134',1),(70,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 17:06:47.094784',1),(71,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 17:17:22.962076',1),(72,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 17:31:58.663547',1),(73,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:31:59.933002',1),(74,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:49:53.774325',1),(75,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:51:11.279823',1),(76,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:54:01.562273',1),(77,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:54:58.597489',1),(78,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 17:55:29.406401',1),(79,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:55:31.482459',1),(80,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:55:39.628593',1),(81,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:56:12.744549',1),(82,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:57:20.424054',1),(83,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 17:58:23.024247',1),(84,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:02:01.466160',1),(85,'pipeline','/pipeline/delete_task/2/','','','192.168.189.1','2022-02-12 18:02:09.251894',1),(86,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:02:25.887812',1),(87,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:02:26.838732',1),(88,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:02:38.891495',1),(89,'pipeline','/pipeline/update_task/5/','','','192.168.189.1','2022-02-12 18:03:52.807751',1),(90,'pipeline','/pipeline/update_task/5/','','','192.168.189.1','2022-02-12 18:03:55.219969',1),(91,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:08:00.283950',1),(92,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:08:02.083015',1),(93,'pipeline','/pipeline/update_task/4/','','','192.168.189.1','2022-02-12 18:08:23.670735',1),(94,'pipeline','/pipeline/update_task/4/','','','192.168.189.1','2022-02-12 18:08:29.028545',1),(95,'pipeline','/pipeline/update_task/4/','','','192.168.189.1','2022-02-12 18:08:35.475842',1),(96,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 18:10:12.254654',1),(97,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 18:10:14.489886',1),(98,'pipeline','/pipeline/update_task/4/','','','192.168.189.1','2022-02-12 18:10:16.120515',1),(99,'pipeline','/pipeline/update_task/4/','','','192.168.189.1','2022-02-12 18:10:17.219959',1),(100,'pipeline','/pipeline/update_task/5/','','','192.168.189.1','2022-02-12 18:10:25.428012',1),(101,'pipeline','/pipeline/update_task/5/','','','192.168.189.1','2022-02-12 18:10:27.357760',1),(102,'pipeline','/pipeline/update_task/1/','','','192.168.189.1','2022-02-12 18:10:28.577207',1),(103,'pipeline','/pipeline/update_task/1/','','','192.168.189.1','2022-02-12 18:10:30.783361',1),(104,'pipeline','/pipeline/update_task/4/','','','192.168.189.1','2022-02-12 18:10:32.912381',1),(105,'pipeline','/pipeline/delete_task/2/','','','192.168.189.1','2022-02-12 18:10:41.391538',1),(106,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:10:48.982162',1),(107,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:10:49.864970',1),(108,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:11:02.129519',1),(109,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 18:11:25.305209',1),(110,'pipeline','/pipeline/update_task/1/','','','192.168.189.1','2022-02-12 18:11:28.347966',1),(111,'processor','/processor/create/','','','192.168.189.1','2022-02-12 18:11:58.750533',1),(112,'processor','/processor/create/','','','192.168.189.1','2022-02-12 18:12:15.967352',1),(113,'processor','/processor/create/','','','192.168.189.1','2022-02-12 18:12:23.919871',1),(114,'processor','/processor/create/','','','192.168.189.1','2022-02-12 18:12:28.778735',1),(115,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:12:45.337696',1),(116,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:14:37.238742',1),(117,'pipeline','/pipeline/history/2/','','','192.168.189.1','2022-02-12 18:15:44.110330',1),(118,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 18:15:44.343758',1),(119,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 18:15:44.353113',1),(120,'pipeline','/pipeline/history_list/4_1/','','','192.168.189.1','2022-02-12 18:16:06.979989',1),(121,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:16:16.288603',1),(122,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:16:26.926094',1),(123,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:16:45.868648',1),(124,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:16:46.794502',1),(125,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:17:01.704252',1),(126,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:17:02.927660',1),(127,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:17:10.701310',1),(128,'processor','/processor/view_history/1/','','','192.168.189.1','2022-02-12 18:17:22.230982',1),(129,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:17:24.519919',1),(130,'pipeline','/pipeline/create_task/2/','','','192.168.189.1','2022-02-12 18:17:38.970669',1),(131,'pipeline','/pipeline/update_task/7/','','','192.168.189.1','2022-02-12 18:18:13.095948',1),(132,'pipeline','/pipeline/update_task/7/','','','192.168.189.1','2022-02-12 18:18:18.449683',1),(133,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:19:39.543477',1),(134,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:19:40.769204',1),(135,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:19:46.327882',1),(136,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 18:19:49.954194',1),(137,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:19:55.337534',1),(138,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:19:58.295233',1),(139,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:20:04.739759',1),(140,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 18:20:13.127671',1),(141,'pipeline','/pipeline/update_task/2/','','','192.168.189.1','2022-02-12 18:20:17.199733',1),(142,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:20:38.790572',1),(143,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:22:45.631428',1),(144,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:23:28.801363',1),(145,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:24:39.061991',1),(146,'pipeline','/pipeline/update_task/6/','','','192.168.189.1','2022-02-12 18:24:42.270028',1),(147,'pipeline','/pipeline/update_task/7/','','','192.168.189.1','2022-02-12 18:24:44.466140',1),(148,'pipeline','/pipeline/update_task/7/','','','192.168.189.1','2022-02-12 18:24:54.035094',1),(149,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 18:25:34.310732',1),(150,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 18:25:47.540354',1),(151,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 18:26:03.170676',1),(152,'pipeline','/pipeline/create/','','','192.168.189.1','2022-02-12 18:26:09.324866',1),(153,'pipeline','/pipeline/2/','','','192.168.189.1','2022-02-12 18:26:44.522083',1);
/*!40000 ALTER TABLE `horae_visitrecord` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-02-13  2:30:52
