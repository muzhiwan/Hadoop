package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileGameUserInfoMR {

	public static class GameUserInfoMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapkey = new Text();
		private Text mapvalue = new Text();

		public void map(LongWritable key, Text value, Context context) {
			String data[] = value.toString().split(Utils100001.DELIMITER);
			try{
				if (data.length==30&&data[Utils100001.INPUT_EVENT_CLASS_ID].equals(Utils100001.EVENT_CLASS_ID)) {
					long serverTime = Long.parseLong(data[Utils100001.INPUT_SERVER_TIME])* 1000;
					long clientTime = Long.parseLong(data[Utils100001.INPUT_CLIENT_TIME]);
					long time = Utility.getValidTime(clientTime, serverTime);
					String mapperKey = String.format("%s\t%s\t%s",
							data[Utils100001.INPUT_CELL_PHONE_DEVICE_ID],
							data[Utils100001.INPUT_PACKAGE_NAME],
							data[Utils100001.INPUT_VERSION_CODE]
						);
					String mapperValue	=	String.format("%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
							time,
							data[Utils100001.INPUT_VERSION],
							data[Utils100001.INPUT_CELL_PHONE_BRAND],
							data[Utils100001.INPUT_CELL_PHONE_MODEL],
							data[Utils100001.INPUT_CELL_PHONE_CPU],
							data[Utils100001.INPUT_CELL_PHONE_DENSITY],
							data[Utils100001.INPUT_CELL_PHONE_SCREEN_WIDTH],
							data[Utils100001.INPUT_CELL_PHONE_SCREEN_HEIGHT]);
					mapkey.set(mapperKey);
					mapvalue.set(mapperValue);
					context.write(mapkey, mapvalue);
				} else {
					System.err.println("Input Error: " +data.length+" , "+ value.toString());
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	public static class GameUserInfoReducer extends Reducer<Text, Text, Text, Text>  {

		private static final String DELIMITER = "\t";
		private String vstr;
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long timestamp=0;
			for(Text v:values){
				long access_time=Long.parseLong(v.toString().split(DELIMITER)[0]);
				if(timestamp==0||access_time<timestamp){
					timestamp=access_time;
					vstr=v.toString();
				}
			}
			context.write(key, new Text(vstr));
		}
	}

}
