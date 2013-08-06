package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class MobileSDKActiveUsersMR {


	public static class MobileSDKActiveUsersMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapkey = new Text();
		private Text mapvalue = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			context.write(mapkey, mapvalue);
		}
	}

	public static class MobileSDKActiveUsersReducer extends
	  Reducer<Text, Text, Text, Text>  {
		private Text rkey = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ToHBase <in>");
			System.exit(2);
		}

		conf.set("hbase.zookeeper.quorum",
				"etu-master.etu-testdomain,etu-3.etu-testdomain" +
				",etu-4.etu-testdomain,etu-5.etu-testdomain,etu-6.etu-testdomain");
		conf.set("hbase.zookeeper.property.clientPort", "2181");


		conf.set("mapred.job.priority", "VERY_HIGH");

		Job job = new Job(conf, "Import txtFile to hbase!");
		job.setJarByClass(MobileSDKActiveUsersMR.class);
		job.setMapperClass(MobileSDKActiveUsersMapper.class);
		job.setReducerClass(MobileSDKActiveUsersReducer.class);
		job.setNumReduceTasks(32);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		job.waitForCompletion(true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
