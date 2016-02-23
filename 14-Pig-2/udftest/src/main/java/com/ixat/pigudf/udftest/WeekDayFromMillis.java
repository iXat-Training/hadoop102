package com.ixat.pigudf.udftest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class WeekDayFromMillis extends EvalFunc<String>{
	
	SimpleDateFormat sf = new SimpleDateFormat("EEE");
	
	
	@Override
	public String exec(Tuple arg0) throws IOException {
		if(arg0.size()<1) return null;
		
		
		//strDate would be in the format of 1357718725941
		String strDate = arg0.get(0).toString();
		
		Date theDate = new Date(Long.parseLong(strDate));
		return sf.format(theDate);
			
		
	}

}
