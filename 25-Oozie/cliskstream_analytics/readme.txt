Create a DIR (/tmp/cs) and change permissions to RW for all (recursive)
Upload the omniture data to DFS (regusers.tsv, urlmap.tsv, omniture.tsv)

Create Users Table
	create table users (swid STRING, birth_dt STRING, gender_cd CHAR(1))
	ROW FORMAT DELIMITED 
	FIELDS TERMINATED by '\t' 
	stored as textfile tblproperties ("skip.header.line.count"="1");

Create Products Table
	create table products (url STRING, category STRING)
	ROW FORMAT DELIMITED 
	FIELDS TERMINATED by '\t'
	stored as textfile tblproperties ("skip.header.line.count"="1");

Create click stream table
	create table omniturelogs (col_1 STRING,col_2 STRING,col_3 STRING,col_4 STRING,col_5 STRING,col_6 STRING,col_7 STRING,col_8 STRING,col_9 STRING,col_10 STRING,col_11 STRING,col_12 STRING,col_13 STRING,col_14 STRING,col_15 STRING,col_16 STRING,col_17 STRING,col_18 STRING,col_19 STRING,col_20 STRING,col_21 STRING,col_22 STRING,col_23 STRING,col_24 STRING,col_25 STRING,col_26 STRING,col_27 STRING,col_28 STRING,col_29 STRING,col_30 STRING,col_31 STRING,col_32 STRING,col_33 STRING,col_34 STRING,col_35 STRING,col_36 STRING,col_37 STRING,col_38 STRING,col_39 STRING,col_40 STRING,col_41 STRING,col_42 STRING,col_43 STRING,col_44 STRING,col_45 STRING,col_46 STRING,col_47 STRING,col_48 STRING,col_49 STRING,col_50 STRING,col_51 STRING,col_52 STRING,col_53 STRING) 
	ROW FORMAT DELIMITED 
	FIELDS TERMINATED by '\t' 
	stored as textfile tblproperties ("skip.header.line.count"="1");

Load the Data
 LOAD DATA INPATH '/tmp/cs/urlmap.tsv' OVERWRITE INTO TABLE products;
 LOAD DATA INPATH '/tmp/cs/regusers.tsv' OVERWRITE INTO TABLE users;
 LOAD DATA INPATH '/tmp/cs/omniture.tsv' OVERWRITE INTO TABLE omniturelogs;


Create a view, since we dont need all columns from omniture
	
	CREATE VIEW omniture AS SELECT col_2 ts, col_8 ip, col_13 url, col_14 swid, col_50 city, col_51 country, col_53 state FROM omniturelogs

Create a Join to get analytics out
	create table csa as select to_date(o.ts) logdate, o.url, o.ip, o.city, upper(o.state) state, o.country, p.category, CAST(datediff( from_unixtime( unix_timestamp() ), from_unixtime( unix_timestamp(u.birth_dt, 'dd-MMM-yy'))) / 365 AS INT) age, u.gender_cd from 
	omniture o inner join products p 
	on o.url = p.url left outer join users u on o.swid = concat('{', u.swid , '}')


-----create a table in mysql which would hold the analyzed stuff
-------------create a table in mysql
CREATE TABLE `csanalytics` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`vist_date` VARCHAR(20) NULL,
	`url` VARCHAR(100) NULL,
	`ip` VARCHAR(100) NULL,
	`city`  VARCHAR(100) NULL,
	`state` VARCHAR(100) NULL,
	`country` VARCHAR(100) NULL,
	`category` VARCHAR(100) NULL,
	`age` VARCHAR(100) NULL,
	`gender` VARCHAR(100) NULL,
	PRIMARY KEY (`id`)
)



==================Oozie Job Configuration
Lets assume the file named a.tsv which has CS data would arrive to HDFS daily once
We need to load this data to Hive
Do Analytics using Hive
Store the analyzed data in MySQL using Sqoop, the sqoop command is 
	sqoop export --connect jdbc:mysql://ixatlabmaster:3306/hdptests --username lab --password killer --table csanalytics  --columns "vist_date,url,ip,city,state,country,category,age,gender"  --export-dir /tmp/cs/stage -m 1

All the above need to be done via Oozie
Sqoop requires the DBDriver, we need that to be placed in /user/oozie/share/lib/sqoop
