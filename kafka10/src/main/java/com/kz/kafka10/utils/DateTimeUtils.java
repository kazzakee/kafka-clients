package com.kz.kafka10.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateTimeUtils {

	public static Date toWholeHour(Date d, int beforeOrAfter) {
	    Calendar c = new GregorianCalendar();
	    c.setTime(d);
		c.add(Calendar.HOUR, beforeOrAfter);
	    c.set(Calendar.MINUTE, 0);
	    c.set(Calendar.SECOND, 0);
	    c.set(Calendar.MILLISECOND, 0);
	    return c.getTime();
    }
	
	public static Date toWholeMinute(Date d, int beforeOrAfter) {
	    Calendar c = new GregorianCalendar();
	    c.setTime(d);
        c.add(Calendar.MINUTE, beforeOrAfter);
	    c.set(Calendar.SECOND, 0);
	    c.set(Calendar.MILLISECOND, 0);
	    return c.getTime();
    }
	
	public static String printableDate(Date date) {
		return printableDate(date, null);
	}
	
	public static String printableDate(Date date, String pattern){
		if(pattern==null) {
			pattern = "MM/dd/yyyy HH:mm:ss.SSSZ";
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
		return dateFormat.format(date);
	}
	
	public static void main(String[] args) {
		Date date = new Date();
		System.out.println("prev whole hour, millies: "+printableDate(toWholeHour(date,-1))+", "+toWholeHour(date,-1).getTime());
		System.out.println("curr whole hour, millies: "+printableDate(toWholeHour(date,0))+", "+toWholeHour(date,0).getTime());
		System.out.println("next whole hour, millies: "+printableDate(toWholeHour(date,1))+", "+toWholeHour(date,1).getTime());

		System.out.println("prev whole minute, millies: "+printableDate(toWholeMinute(date,-15))+", "+toWholeMinute(date,-15).getTime());
		System.out.println("curr whole minute, millies: "+printableDate(toWholeMinute(date,0))+", "+toWholeMinute(date,0).getTime());
		System.out.println("next whole minute, millies: "+printableDate(toWholeMinute(date,15))+", "+toWholeMinute(date,15).getTime());
	}
}
