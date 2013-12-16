package com.muzhiwan.hadoop;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.muzhiwan.hadoop.MobileGameUserInfoMR.GameUserInfoMapper;
import com.muzhiwan.hadoop.MobileGameUserInfoMR.GameUserInfoReducer;
import com.muzhiwan.hadoop.MobileSDKUserInfoMR.MobileSDKUserInfoMapper;
import com.muzhiwan.hadoop.MobileSDKUserInfoMR.MobileSDKUserInfoReducer;
import com.muzhiwan.hadoop.SDKGameUserInfoMR.SDKGameMapper;
import com.muzhiwan.hadoop.SDKGameUserInfoMR.SDKGameReducer;

public class JobRunner {
	private static final String TYPE_MOBILE_SDK_USERINFO = "MobileSDKUserInfo";
	private static final String TYPE_MOBILE_GAME_USERINFO = "MobileGAMEUserInfo";
	private static final String TYPE_SDK_GAME_USERINFO = "SDKGAMEUserInfo";

	private static final String JOB_MOBILE_SDK_USERINFO = "Mobile SDK Analysis User Info Collection";
	private static final String JOB_SDK_GAME_USERINFO = "Mobile SDK Analysis User Info Collection";
	private static final String JOB_MOBILE_GAME_USERINFO = "Mobile GAME Analysis User Info Collection";

	private static Map<String, Method> jobEntryMap = new HashMap<String, Method>();
	static {
		try {
			jobEntryMap.put(TYPE_MOBILE_SDK_USERINFO, JobRunner.class
					.getMethod("getMobileSDKUserInfo", String.class,
							String.class));
			jobEntryMap.put(TYPE_MOBILE_GAME_USERINFO, JobRunner.class
					.getMethod("getMobileGameUserInfo", String.class,
							String.class));
			jobEntryMap.put(TYPE_SDK_GAME_USERINFO, JobRunner.class
					.getMethod("getSDKGameUserInfo", String.class,
							String.class));
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
	}

	public static int executeAnalysis(String type, String inputPath,
			String outputPath) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {

		if (jobEntryMap.containsKey(type)) {
			return (Integer) jobEntryMap.get(type).invoke(null, inputPath,
					outputPath);
		}
		return 0;
	}

	
	public static int getMobileSDKUserInfo(String inputPath,
			String outputPath) throws IOException, InterruptedException,
			ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LogAnalyzerMR.class);
		job.setJobName(JOB_MOBILE_SDK_USERINFO);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(MobileSDKUserInfoMapper.class);
		job.setReducerClass(MobileSDKUserInfoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}
	
	public static int getSDKGameUserInfo(String inputPath,
			String outputPath) throws IOException, InterruptedException,
			ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LogAnalyzerMR.class);
		job.setJobName(JOB_SDK_GAME_USERINFO);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(SDKGameMapper.class);
		job.setReducerClass(SDKGameReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}
	
	public static int getMobileGameUserInfo(String inputPath,
			String outputPath) throws IOException, InterruptedException,
			ClassNotFoundException {
		long timeStart = System.currentTimeMillis();
		
		Job job = new Job();
		job.setJarByClass(LogAnalyzerMR.class);
		job.setJobName(JOB_MOBILE_GAME_USERINFO);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(GameUserInfoMapper.class);
		job.setReducerClass(GameUserInfoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		long timeEnd = System.currentTimeMillis();
		System.err.println("The total cost: " + (timeEnd - timeStart));
		
		return exitCode;
	}
}
