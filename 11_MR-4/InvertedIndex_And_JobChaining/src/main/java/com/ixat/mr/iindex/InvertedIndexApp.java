package com.ixat.mr.iindex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Example to demontsrate construction of an Inverted Index
//sample data can be found on internet
//we are using the book named Bible Myths and their Parallels in other Religions by T. W. Doane
//pick the file from http://www.gutenberg.org/files/31885/31885-0.txt
//there are 40 chapters in the above book, lets split the book into the respective chapters and do an inverted index on it
//split the book into chapters using csplit -f chapter 31885-0.txt '/^CHAPTER.*\./' {39}
//upload all the chapters to HDFS and run the below driver to do analytics.


//there are 40 chapters, and this job could be heavy in resource consumption, hence run this is throttle mode (on a set of chapters at a time)
//if you want to kill the job - 
//yarn application -kill <theApplicationID>

public class InvertedIndexApp extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		InvertedIndexApp theDriver = new InvertedIndexApp();
		int res = ToolRunner.run(theDriver, args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		int numReducers = 2;
		if (args.length <2 ) {
			System.out
					.println("Usage: InvertedIndexApp <inputDir> <output> [numReducers]");
			System.exit(-1);
		}

		if(args.length>2)
			numReducers = Integer.parseInt(args[2]);
		
		Job job = Job.getInstance(getConf());
		job.setJobName("IIndexOfaBook");
		job.setJarByClass(InvertedIndexApp.class);

		/* Field separator for reducer output */
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", "\t");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IndexMapper.class);
		job.setCombinerClass(IndexReducer.class);
		job.setReducerClass(IndexReducer.class);
		job.setPartitionerClass(ChapterPartitioner.class);
		
		
		job.setNumReduceTasks(numReducers);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		/* Pick files recursively for this type of Job */
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		/* Delete output filepath if already exists */
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class IndexMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text filename = new Text();

		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String filenameStr = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			filename = new Text(filenameStr);

			String line = value.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
			
		
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().trim());
				context.write(word, filename);
			}
		}		
	}
	
	public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values,
				final Context context) throws IOException, InterruptedException {

			StringBuilder stringBuilder = new StringBuilder();

			for (Text value : values) {
				stringBuilder.append(value.toString());

				if (values.iterator().hasNext()) {
					stringBuilder.append(",");
				}
			}

			context.write(key, new Text(stringBuilder.toString()));
		}

	}
	
	public static class ChapterPartitioner extends Partitioner<Text,Text> 
	implements Configurable{
			
			private Configuration config;
		
			public int getPartition(Text key, Text value, int numReducers) {
				if(numReducers<=1) return numReducers;
				//chapter01
				int chapterNum = Integer.parseInt(value.toString().substring(7));

				return chapterNum % numReducers;
				
						
				
			}

			public void setConf(Configuration conf) {
				// TODO Auto-generated method stub
				config = conf;
			}

			public Configuration getConf() {
				// TODO Auto-generated method stub
				return config;
			}

			
			
		}
	
}
