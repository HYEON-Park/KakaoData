package com.park.mar8kdmain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KDReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
 
	private static final IntWritable SUM = new IntWritable();
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {

	int sum = 0;
	for (IntWritable l : arg1) {
		sum += l.get();
	}
	SUM.set(sum);
	arg2.write(arg0, SUM);
	}
}
