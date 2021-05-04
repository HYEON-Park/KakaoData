package com.park.mar8kdmain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KDMain {
	public static void main(String[] args) {
		try {
			
			Configuration c = new Configuration();
			Job j = Job.getInstance();
			
			j.setMapperClass(KDMapper.class);
			j.setCombinerClass(KDReducer.class);
			j.setReducerClass(KDReducer.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(j, new Path("/kakao.txt"));
			FileOutputFormat.setOutputPath(j, new Path("/kResult"));
			
			j.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
