package com.ixat.mr.TemparatureAnalytics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {
		String[] data = value.toString().split(",");
		String state = data[1];
		IntWritable temp = new IntWritable(Integer.parseInt(data[2]));
		context.write(new Text(state), temp);
	}
}
