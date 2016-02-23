clickstreamData = load 'clicks.csv' USING PigStorage(',') as (userid:int, timestamp:chararray,url:chararray);
clickstreamData_g = group clickstreamData by url;
clickstreamData_gc = foreach clickstreamData_g generate group as URL, COUNT(clickstreamData) as NUMVIEWS;
dump clickstreamData_gc;
