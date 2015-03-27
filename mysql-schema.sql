/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table neversleepdb_blobs
# ------------------------------------------------------------

CREATE TABLE `neversleepdb_blobs` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `blob_id` bigint(20) NOT NULL,
  `part` int(10) NOT NULL DEFAULT '0',
  `entity_id` varchar(127) NOT NULL DEFAULT '',
  `data` longblob NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `blob_id` (`blob_id`,`part`,`entity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table neversleepdb_root_log
# ------------------------------------------------------------

CREATE TABLE `neversleepdb_root_log` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `blob_id` bigint(20) NOT NULL,
  `part` int(10) NOT NULL DEFAULT '0',
  `data` longblob NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `blob_id` (`blob_id`,`part`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table neversleepdb_root_log_master
# ------------------------------------------------------------

CREATE TABLE `neversleepdb_root_log_master` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `blob_id` bigint(20) NOT NULL,
  `part` int(10) NOT NULL DEFAULT '0',
  `data` longblob NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `blob_id` (`blob_id`,`part`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table neversleepdb_vars
# ------------------------------------------------------------

CREATE TABLE `neversleepdb_vars` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(127) NOT NULL DEFAULT '',
  `value` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `neversleepdb_vars` WRITE;
/*!40000 ALTER TABLE `neversleepdb_vars` DISABLE KEYS */;

INSERT INTO `neversleepdb_vars` (`id`, `name`, `value`)
VALUES
	(1,'last_saved_blob_id',1),
	(2,'last_saved_root_log',0);

/*!40000 ALTER TABLE `neversleepdb_vars` ENABLE KEYS */;
UNLOCK TABLES;



/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
