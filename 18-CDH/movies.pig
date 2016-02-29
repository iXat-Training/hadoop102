--load all your jars to /user/cloudera/jars
--load the movies.txt to /user/cloudera/movies
REGISTER hdfs:///user/cloudera/jars/json-simple-1.1.1.jar; --we could do this because we are on a PD cluster and the script is also being executed from the same Pseudo Dist Node
REGISTER hdfs://localhost:8020/user/cloudera/jars/elephant-bird-hadoop-compat-4.13.jar;
REGISTER hdfs://quickstart.cloudera:8020/user/cloudera/jars/elephant-bird-pig-4.13.jar;

movie = LOAD '/user/cloudera/movies/movies.txt' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (thejson:map []);
details = FOREACH movie GENERATE thejson#'title' as TITLE, thejson#'year' as Year, CONCAT(CONCAT(thejson#'director'#'first_name',' '), thejson#'director'#'last_name') as DIR_NAME;
dir_movies_g = group details by DIR_NAME;
dir_movies = FOREACH dir_movies_g generate group as DIR_NAME, COUNT(details) as NUM_MOVIES_DIRECTED;
dir_movies_o = ORDER dir_movies by NUM_MOVIES_DIRECTED desc;
STORE dir_movies_o  INTO '/user/cloudera/movies/director_movies' using PigStorage('\t');

