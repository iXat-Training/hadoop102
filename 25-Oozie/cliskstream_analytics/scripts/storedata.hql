CREATE EXTERNAL TABLE IF NOT EXISTS csaresults (
logdate STRING, url STRING, ip STRING, scity STRING, sstate STRING, country STRING, category STRING, age STRING, gender STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/tmp/cs/stage';

INSERT OVERWRITE table csaresults select * from csa where age is not null and gender_cd is not null;
