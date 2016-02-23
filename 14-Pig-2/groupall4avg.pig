visits = LOAD 'clicks2.csv' USING PigStorage(',') as (user:int,website:chararray,timestamp:chararray); --we could have used long for timestamp

user_visits = group visits by user;

user_counts = foreach user_visits generate group as user, COUNT(visits) as numvisits;

all_counts = group user_counts all;

avg_count = foreach all_counts generate AVG(user_counts.numvisits);

dump avg_count;