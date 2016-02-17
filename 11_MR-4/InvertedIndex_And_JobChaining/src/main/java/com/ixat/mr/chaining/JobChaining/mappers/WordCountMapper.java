package com.ixat.mr.chaining.JobChaining.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	IntWritable one = new IntWritable(1);
	Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
           context.write(new Text(tokenizer.nextToken()), one );
           context.getCounter(com.ixat.mr.chaining.JobChaining.counters.MYCOUNTERS.COUNT_WORD_SPLITS).increment(1);
			
        }
     }

}