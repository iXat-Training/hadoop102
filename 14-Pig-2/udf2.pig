REGISTER udftest-1.jar;

DEFINE WDFS  com.ixat.pigudf.udftest.WeekDayFromString;

emp_sal_changes = load 'empsal.tsv' USING PigStorage('\t') as
	(
		empid:int,
		ratechangedate:chararray,
		rate:double,
		payfrequency:int,
		modifieddate:chararray
	);

emp_sal_changes_week_groups = group emp_sal_changes by WDFS(ratechangedate);
emp_sal_changes_week_groups_count = foreach emp_sal_changes_week_groups generate group as WeekDay, COUNT(emp_sal_changes) as NUMCHANGES;
emp_sal_changes_week_groups_count_o = order emp_sal_changes_week_groups_count by NUMCHANGES desc;
dump emp_sal_changes_week_groups_count_o;
