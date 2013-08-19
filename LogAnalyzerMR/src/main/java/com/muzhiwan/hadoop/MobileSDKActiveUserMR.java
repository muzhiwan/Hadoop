package com.muzhiwan.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileSDKActiveUserMR {

	public static class ActiveUserMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapkey = new Text();
		private Text mapvalue = new Text();

		public void map(LongWritable key, Text value, Context context) {
			String data[] = value.toString().split(Utils100001.DELIMITER);
			try{
				if (data.length==30&&data[Utils100001.INPUT_EVENT_CLASS_ID].equals(Utils100001.EVENT_CLASS_ID)) {
					long serverTime = Long.parseLong(data[Utils100001.INPUT_SERVER_TIME])* 1000;
					long clientTime = Long.parseLong(data[Utils100001.INPUT_CLIENT_TIME]);
					long time = Utility.getValidTime(clientTime, serverTime);
					
					String mapperKey	=	String.format("%s\t%s",
							Utility.timeToDate(time,"yyyy-MM-dd"),
							data[Utils100001.INPUT_CELL_PHONE_DEVICE_ID]);
					mapkey.set(mapperKey);
					mapvalue.set("");
					context.write(mapkey, mapvalue);
				} else {
					System.err.println("Input Error: " +data.length+" , "+ value.toString());
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	public static class ActiveUserReducer extends Reducer<Text, Text, Text, LongWritable>  {
		
		private static final String DELIMITER = "\t";
		private Map<String,Long> usermap	=	new HashMap<String, Long>();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String data[]	= key.toString().split(DELIMITER);
			Long count	=	usermap.get(data[0]);
			if(count==null){
				usermap.put(data[0], 1l);
			}else{
				usermap.put(data[0], count+1);
			}
		}
		
		protected void cleanup(Context context)
		throws IOException, InterruptedException {
			for(String key:usermap.keySet()){
				context.write(new Text(key),new LongWritable(usermap.get(key)));
			}
		}
	}

}
