package com.ixat.mr.chaining.JobChaining.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LowerCaseMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	Text record = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		record.set(value.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " "));
		context.write(key, record);
		context.getCounter(
				com.ixat.mr.chaining.JobChaining.counters.MYCOUNTERS.COUNT_LOWERCASE)
				.increment(1);

	}

}
