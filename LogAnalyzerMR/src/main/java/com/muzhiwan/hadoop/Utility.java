package com.muzhiwan.hadoop;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utility {

	public static String packWord(String word){
		String retStr = null;
		try {
			retStr = URLDecoder.decode(word, "UTF-8").replace('\r', ' ').replace('\n', ' ').replace('\t', ' ');
		} catch (Exception e) {
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
	
	public static long getValidTime(long clientTime, long serverTime) {
		if (serverTime > clientTime+10*24*60*60*1000 || serverTime < clientTime) {
			return serverTime;
		}else {
			return clientTime;
		}
	}

	public static long getValidTime(String clientTime, String serverTime) {
		long lClientTime = 0L;
		long lServerTime = 0L;
		long validTime = 0L;
		
		try {
			lClientTime = Long.parseLong(clientTime);
			lServerTime = Long.parseLong(serverTime) * 1000;
		} catch (NumberFormatException e) {
			System.err.println(String.format("Client Time: %s; Server Time: %s.", clientTime, serverTime));
		}
		
		if (lServerTime > lClientTime+10*24*60*60*1000 && lServerTime < lClientTime) {
			validTime = lServerTime;
		}else {
			validTime = lClientTime;
		}
		
		return validTime;
	}
}
