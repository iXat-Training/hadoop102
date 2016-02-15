package com.ixat.mr.TemparatureAnalytics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TDriver{
	   public static void main(String args[]) throws Exception{
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "AvgTemperatureJob");
	    String outputDir = "/OUTPUT_" + System.currentTimeMillis();
	    String inputFile = "/test/temp.txt";
	    
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    job.setInputFormatClass(TextInputFormat.class);
	      

	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setJarByClass(TDriver.class);
	    job.setMapperClass(TMapper.class);
	    job.setReducerClass(TReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    
	    job.setNumReduceTasks(2);
	    job.waitForCompletion(true);
	    System.out.println("Job Completed, results are stored in the directory - " + outputDir);

	  }
	}
