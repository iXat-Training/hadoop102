package com.ixat.mr.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hello world!
 *
 */
public class WordCount 
{
    public static void main( String[] args ) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
    {
    		int numReducers = 2;
    	   Configuration conf = new Configuration();
           String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
           if (otherArgs.length < 2) {
               System.err.println("Usage: WordCount <in> <out> [numReducers]");
               System.exit(2);
           }

           if(otherArgs.length>2){
        	   numReducers = Integer.parseInt(otherArgs[2]);
           }
           Job job = Job.getInstance(conf);
           job.setJobName("WordCount");

           job.setJarByClass(WordCount.class);

           job.setMapperClass(TokenizerMapper.class);
           job.setReducerClass(IntSumReducer.class);

           job.setOutputKeyClass(Text.class);
           job.setOutputValueClass(IntWritable.class);

           job.setNumReduceTasks(numReducers);
           FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
           FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

           System.exit(job.waitForCompletion(true) ? 0 : 1);
       }
    
    
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          
        	String cleanLine = value.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
        	 
        	StringTokenizer itr = new StringTokenizer(cleanLine);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().trim());
                context.write(word, one);
            }
            
        }
    }
    
    
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
}
