package com.ixat.mr.TemparatureAnalytics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(final Text key, final Iterable<IntWritable> values,
			final Context context) throws IOException, InterruptedException {
		int sumOfTemperatures = 0;
		int numValues = 0;

		for (IntWritable temperature : values) {
			sumOfTemperatures += temperature.get();
			numValues++;
		}
		int average = sumOfTemperatures / numValues;
		context.write(key, new IntWritable(average));
	}
}
