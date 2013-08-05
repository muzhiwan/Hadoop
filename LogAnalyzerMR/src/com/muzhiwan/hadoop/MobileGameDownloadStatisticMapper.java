package com.muzhiwan.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileGameDownloadStatisticMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final String DELIMITER = "\001";
	private static final String EVENT_CLASS_ID = "200001";
	
/*	private static final int INPUT_SERVER_TIME = 0;
	private static final int INPUT_CLIENT_IP = 1;
	private static final int INPUT_CLIENT_AREA = 2;*/
	
	private static final int INPUT_EVENT_CLASS_ID = 3;
	private static final int INPUT_CLIENT_TIME = 4;
/*	private static final int INPUT_EVENT_TAG = 5;
	private static final int INPUT_CELL_PHONE_BRAND = 6;
	private static final int INPUT_CELL_PHONE_MODEL = 7;
	private static final int INPUT_CELL_PHONE_CPU = 8;
	private static final int INPUT_CELL_PHONE_DENSITY = 9;
	private static final int INPUT_CELL_PHONE_SCREEN_WIDTH = 10;
	private static final int INPUT_CELL_PHONE_SCREEN_HEIGHT = 11;
	private static final int INPUT_CELL_PHONE_NETWORK = 12;
	private static final int INPUT_CELL_PHONE_SYSTEM_VERSION = 13;
	private static final int INPUT_MUZHIWAN_VERSION = 14;
	private static final int INPUT_CELL_PHONE_FIRMWARE = 15;
	private static final int INPUT_CELL_PHONE_DEVICE_ID = 16;*/
	
	private static final int INPUT_TITLE = 17;
	private static final int INPUT_PACKAGE_NAME = 18;
	private static final int INPUT_VERSION = 19;
/*	private static final int INPUT_VERSION_CODE = 20;
	private static final int INPUT_APK_LINK = 21;
	private static final int INPUT_APK_ID = 22;
	private static final int INPUT_APK_BAIDU_LINK = 23;
	private static final int INPUT_APK_SIZE = 24;
	private static final int INPUT_FLAG_GOOGLE_PLAY = 25;
	private static final int INPUT_FLAG_LOGIN_ACCOUNT = 26;
	private static final int INPUT_OPERATION_TAG = 27;
	private static final int INPUT_CHANNEL = 28;
	private static final int INPUT_APK_SOURCE = 29;
	private static final int INPUT_CATEGORY = 30;*/
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String data[] = value.toString().split(DELIMITER);
		if (Utility.getString(data, INPUT_EVENT_CLASS_ID).equals(EVENT_CLASS_ID)) {
			String mapperKey = String.format("%s\t%s\t%s\t%s",
					Utility.timeToDate(Utility.getString(data, INPUT_CLIENT_TIME)),
					Utility.packWord(Utility.getString(data, INPUT_TITLE)),
					Utility.packWord(Utility.getString(data, INPUT_PACKAGE_NAME)),
					Utility.packWord(Utility.getString(data, INPUT_VERSION)));
			context.write(new Text(mapperKey), new IntWritable(1));
		} else {
			System.err.println("Input Error: " + value.toString());
		}
	}

}
