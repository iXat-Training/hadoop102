employees = LOAD 'datatypes.csv' using PigStorage('\t') 
    AS (empid:int, email:chararray, name:tuple(first_name:chararray, last_name:chararray), 
	projects:bag{project: tuple(project_name:chararray, state:chararray)}, skills:map[chararray]) ;

emplist = FOREACH employees GENERATE empid, email, name.first_name, projects, skills#'IDE' ;
--dump emplist;

project_list = FOREACH emplist generate FLATTEN(projects);
dump project_list;