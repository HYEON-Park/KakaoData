package com.park.mar8kdmain;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//2020년 10월 19일 오전 8:50,		소현님이 들어왔습니다.
//2020년 10월 19일 오후 4:15, 	한정임 : 에어컨 때문에 추우시면 말씀해주세요~

public class KDMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final Text WORD = new Text();
	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			String line = value.toString();
			
			String[] line2 = line.split(" : "); 

			StringTokenizer st = new StringTokenizer(line2[1], " ");
			while (st.hasMoreTokens()) {
				WORD.set(st.nextToken());
				context.write(WORD, ONE);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
