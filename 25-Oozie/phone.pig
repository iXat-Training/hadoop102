rmf  /user/cloudera/phones/phoneoutput;
phones = LOAD '/user/cloudera/phones/phone.txt' USING PigStorage(',') as (brand:chararray,city:chararray,state:chararray,region:chararray,country:chararray,sales:int);
cubedata = CUBE phones by CUBE(brand,city,state,region);
results = FOREACH cubedata GENERATE FLATTEN(group) as (brand,city,state,region) , SUM(cube.sales) as sum_sales;
result_formatted = FOREACH results GENERATE (brand is not NULL? brand:'*') as BRAND,(city is not NULL? city:'*') as CITY,(state is not NULL? state:'*') as STATE, (region is not NULL? region:'*') as REGION, sum_sales as TOTALSALES;
store result_formatted into '/user/cloudera/phones/phoneoutput';