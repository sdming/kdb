CREATE TABLE `ttable` (
  `pk` bigint(20) NOT NULL AUTO_INCREMENT,
  `cbool` tinyint(1) DEFAULT NULL,
  `cint` int(11) DEFAULT NULL,
  `cfloat` float DEFAULT NULL,
  `cnumeric` decimal(10,2) DEFAULT NULL,
  `cstring` varchar(100) DEFAULT NULL,
  `cdate` date DEFAULT NULL,
  `cdatetime` datetime DEFAULT NULL,
  `cguid` varchar(36) DEFAULT NULL,
  `cbytes` varbinary(1000) DEFAULT NULL,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `ttable_c` (
  `c_bool` tinyint(1) DEFAULT NULL,
  `c_int` int(11) DEFAULT NULL,
  `c_float` float DEFAULT NULL,
  `c_numeric` decimal(10,2) DEFAULT NULL,
  `c_string` varchar(100) DEFAULT NULL,
  `c_datetime` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DELIMITER //
CREATE PROCEDURE `sp_query`(cbool BOOLEAN, cint int, cfloat float, cnumeric NUMERIC(10,4), cdate date, cdatetime datetime)
BEGIN	 
   SELECT cbool, cint, cfloat, cnumeric, cdate, cdatetime;
END//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `sp_exec`(cint int)
BEGIN	 
   UPDATE ttable SET cdatetime = NOW() WHERE cint = cint;
END//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `sp_exec_inout`(IN cint int, INOUT cfloat float, OUT cdatetime datetime)
BEGIN	 
  SET cdatetime = NOW();
	SET cfloat = cfloat * cint;
END//
DELIMITER ;
