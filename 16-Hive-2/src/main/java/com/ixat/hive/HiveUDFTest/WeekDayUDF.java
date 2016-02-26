package com.ixat.hive.HiveUDFTest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class WeekDayUDF extends org.apache.hadoop.hive.ql.exec.UDF{

	public Text evaluate(Text input){
		
		try {
			String strDate = input.toString();
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yy");
			Date date = sdf.parse(strDate);
			SimpleDateFormat sdf2 = new SimpleDateFormat("EEE");
			Text text = new Text(sdf2.format(date));
			return text;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			return null;
		}
		
	}
}
