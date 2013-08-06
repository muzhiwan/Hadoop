package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileSDKAnalysisDataMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	private static final String DELIMITER = "\001";
	private static final String EVENT_CLASS_ID = "100001";
	private static final String DEFAULT_TIME = "0";
	
	private static final int INPUT_SERVER_TIME = 0;
/*	private static final int INPUT_CLIENT_IP = 1;
	private static final int INPUT_CLIENT_AREA = 2;*/
	
	private static final int INPUT_EVENT_CLASS_ID = 3;
	private static final int INPUT_CLIENT_TIME = 4;
/*	private static final int INPUT_EVENT_TAG = 5;
	private static final int INPUT_CELL_PHONE_BRAND = 6;
	private static final int INPUT_CELL_PHONE_MODEL = 7;
	private static final int INPUT_CELL_PHONE_CPU = 8;
	private static final int INPUT_CELL_PHONE_DENSITY = 9;
	private static final int INPUT_CELL_PHONE_SCREEN_WIDTH = 10;
	private static final int INPUT_CELL_PHONE_SCREEN_HEIGHT = 11;*/
	private static final int INPUT_CELL_PHONE_NETWORK = 12;
/*	private static final int INPUT_CELL_PHONE_SYSTEM_VERSION = 13;
	private static final int INPUT_MUZHIWAN_VERSION = 14;
	private static final int INPUT_CELL_PHONE_FIRMWARE = 15;*/
	private static final int INPUT_CELL_PHONE_DEVICE_ID = 16;
	
	private static final int INPUT_TITLE = 17;
	private static final int INPUT_PACKAGE_NAME = 18;
	// private static final int INPUT_VERSION_CODE = 19;
	private static final int INPUT_VERSION = 20;
/*	private static final int INPUT_SESSION_ID = 21;
	private static final int INPUT_UID = 22;
	private static final int INPUT_FLAG_ROOT = 23;
	private static final int INPUT_MEMORY_CARD_SIZE = 24;
	private static final int INPUT_AVAILABLE_MEMORY_SIZE = 25;
	private static final int INPUT_FLAG_D_CN = 26;
	private static final int INPUT_FLAG_WDJ = 27;
	private static final int INPUT_FLAG_360 = 28;
	private static final int INPUT_FLAG_YYB = 29;*/
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String data[] = value.toString().split(DELIMITER);
		if (Utility.getString(data, INPUT_EVENT_CLASS_ID).equals(EVENT_CLASS_ID)) {
			String serverTime = Utility.getString(data, INPUT_SERVER_TIME);
			if (serverTime.length() < 1) { serverTime = DEFAULT_TIME; }
			
			String clientTime = Utility.getString(data, INPUT_CLIENT_TIME);
			if (clientTime.length() < 1)  { clientTime = DEFAULT_TIME; }
			
			long time = Utility.getValidTime(clientTime, serverTime);
			String mapperKey = String.format("%s\t%s\t%s\t%s\t%s",
					Utility.timeToDate(time),
					Utility.packWord(Utility.getString(data, INPUT_TITLE)),
					Utility.packWord(Utility.getString(data, INPUT_PACKAGE_NAME)),
					Utility.packWord(Utility.getString(data, INPUT_VERSION)),
					Utility.packWord(Utility.getString(data, INPUT_CELL_PHONE_DEVICE_ID)));
			
			String mapperValue = String.format("%d\t%s\t%d", time,
					Utility.packWord(Utility.getString(data, INPUT_CELL_PHONE_NETWORK)), 1);
			
			context.write(new Text(mapperKey), new Text(mapperValue));
		} else {
			System.err.println("Input Error: " +data.length+" , "+ value.toString());
		}
	}

}
