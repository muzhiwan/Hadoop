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

	public static String timeToDate(long time) {
		Date date = new Date(time);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		return sdf.format(date);
	}
	
	public static String timeToDate(String time) {
		Long lTime = 0L;
		try {
			lTime = Long.parseLong(time);
		} catch (NumberFormatException e) {
			System.err.println(time);
		}
		
		return timeToDate(lTime);
	}

	public static String getString(String[] data, int index) {
		String defaultRet = "";
		
		if (data == null) { return defaultRet; }
		else if (data.length <= index) { return defaultRet; }
		else if (data[index] == null) { return defaultRet; }
		
		return data[index];
	}

	public static long getValidTime(String clientTime, String serverTime) {
		long lClientTime = 0L;
		long lServerTime = 0L;
		long validTime = 0L;
		long minValidTime = 1332088922000L;
		
		try {
			lClientTime = Long.parseLong(clientTime);
			lServerTime = Long.parseLong(serverTime) * 1000;
		} catch (NumberFormatException e) {
			System.err.println(String.format("Client Time: %s; Server Time: %s.", clientTime, serverTime));
		}
		
		if (lServerTime > 0 && lServerTime < lClientTime) {
			validTime = lServerTime;
			// System.err.println(String.format("Use server time. Client Time: %s; Server Time: %s.", clientTime, serverTime));
		} else if (lClientTime < minValidTime) {
			validTime = minValidTime;
			System.err.println(String.format("Use standard time. Client Time: %s; Server Time: %s.", clientTime, serverTime));
		} else {
			validTime = lClientTime;
		}
		
		return validTime;
	}
}
