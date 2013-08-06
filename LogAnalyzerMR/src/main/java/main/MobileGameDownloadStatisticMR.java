package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.muzhiwan.hadoop.Utility;

public class MobileGameDownloadStatisticMR {

	public static class MobileGameDownloadStatisticMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private static final String DELIMITER = "\001";
		private static final String EVENT_CLASS_ID = "200001";

		private static final int INPUT_EVENT_CLASS_ID = 3;
		private static final int INPUT_CLIENT_TIME = 4;

		private static final int INPUT_TITLE = 17;
		private static final int INPUT_PACKAGE_NAME = 18;
		private static final int INPUT_VERSION = 19;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String data[] = value.toString().split(DELIMITER);
			if (Utility.getString(data, INPUT_EVENT_CLASS_ID).equals(
					EVENT_CLASS_ID)) {
				String mapperKey = String.format("%s\t%s\t%s\t%s",
						Utility.timeToDate(Utility.getString(data,
								INPUT_CLIENT_TIME)), Utility.packWord(Utility
								.getString(data, INPUT_TITLE)), Utility
								.packWord(Utility.getString(data,
										INPUT_PACKAGE_NAME)), Utility
								.packWord(Utility
										.getString(data, INPUT_VERSION)));
				context.write(new Text(mapperKey), new IntWritable(1));
			} else {
				System.err.println("Input Error: " + data.length + " , "
						+ value.toString());
			}
		}

	}

	public static class MobileGameDownloadStatisticReducer extends	Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;

			for (IntWritable value : values) {
				count += value.get();
			}

			context.write(key, new IntWritable(count));
		}
	}

}
