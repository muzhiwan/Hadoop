package com.muzhiwan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SDKGameUserInfoMR {

	public static class SDKGameMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapkey = new Text();
		private Text mapvalue = new Text();

		public void map(LongWritable key, Text value, Context context) {
			String data[] = value.toString().split("\001");
			if(data.length<17){
				System.out.println("Error line : "+value.toString());
				return;
			}
			try{
					long time = Long.parseLong(data[0]);
					String mapperKey = String.format("%s\t%s",
							data[13],//deviceid
//							data[10]//package name
							data[7]//appkey
						);
					String mapperValue	=	String.format("%d\t%s\t%s\t%s\t%s\t%s\t%s",
							time,
							data[10],//package name
							data[11],//versioncode
							data[8],//brand
							data[9],//model
							data[14],//imei
							data[15]//mac
					);
					mapkey.set(mapperKey);
					mapvalue.set(mapperValue);
					context.write(mapkey, mapvalue);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	public static class SDKGameReducer extends Reducer<Text, Text, Text, Text>  {

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
