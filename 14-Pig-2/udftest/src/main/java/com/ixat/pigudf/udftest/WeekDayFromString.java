package com.ixat.pigudf.udftest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class WeekDayFromString extends EvalFunc<String>{
	SimpleDateFormat sf = new SimpleDateFormat("dd-MM-yy");
	SimpleDateFormat sf2 = new SimpleDateFormat("EEE");
	
	@Override
	public String exec(Tuple arg0) throws IOException {
		if(arg0.size()<1) return null;
		
		
		//strDate would be in the format of 24-Nov-15
		String strDate = arg0.get(0).toString();
		
		try {
			Date date = sf.parse(strDate);
			
			return sf2.format(date);
			
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		
		
	}

}
