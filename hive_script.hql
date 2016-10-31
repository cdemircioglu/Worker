#Hive scripts
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;

DROP TABLE raw_src_xdr;
CREATE EXTERNAL TABLE IF NOT EXISTS raw_src_xdr 
( 
  END_TIME STRING,
  MSISDN STRING,
  HOST STRING,
  URL_CLASS STRING,
  FIRST_URI STRING,
  IP STRING,
  APP_OR_URL STRING,
  IAB_TIER1 STRING,
  IAB_TIER2 STRING,
  UPLINK_BYTES STRING,
  DOWNLOAD_BYTES STRING,
  DURATION STRING,
  MINUTES STRING
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'wasb://src@hwlondonstorage.blob.core.windows.net/data/raw/xdr/';


DROP TABLE refined_src_xdr;
CREATE EXTERNAL TABLE IF NOT EXISTS refined_src_xdr
(
END_TIME STRING,
MSISDN STRING,
HOST STRING,
URL_CLASS STRING,
FIRST_URI STRING,
IP STRING,
APP_OR_URL STRING,
IAB_TIER2 STRING,
UPLINK_BYTES STRING,
DOWNLOAD_BYTES STRING,
DURATION STRING,
MINUTES STRING
)
PARTITIONED BY (IAB_TIER1 string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'wasb://src@hwlondonstorage.blob.core.windows.net/data/refined/xdr/';


	
INSERT OVERWRITE TABLE refined_src_xdr PARTITION (IAB_TIER1)  
SELECT 
END_TIME,
MSISDN,
HOST,
URL_CLASS,
FIRST_URI,
IP,
APP_OR_URL,
IAB_TIER2,
UPLINK_BYTES,
DOWNLOAD_BYTES,
DURATION,
MINUTES,
IAB_TIER1
FROM raw_src_xdr;

