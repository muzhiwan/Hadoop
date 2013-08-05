package com.muzhiwan.hadoop;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utility {

	public static String packWord(String word) throws UnsupportedEncodingException {
		String retStr = null;
		try {
			retStr = URLDecoder.decode(word, "UTF-8").replace('\r', ' ').replace('\n', ' ').replace('\t', ' ');
		} catch (IllegalArgumentException e) {
			retStr = word;
			System.err.println(word);
		}
		
		return retStr;
	}

	public static String timeToDate(String time) {
		Long lTime = 0L;
		try {
			lTime = Long.parseLong(time);
		} catch (NumberFormatException e) {
			System.err.println(time);
		}
		
		Date date = new Date(lTime);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		return sdf.format(date);
	}

}
