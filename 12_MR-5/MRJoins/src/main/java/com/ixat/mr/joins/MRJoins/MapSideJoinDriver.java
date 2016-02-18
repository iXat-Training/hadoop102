package com.ixat.mr.joins.MRJoins;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//Demonstration of Map Side Join

public class MapSideJoinDriver  extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new MapSideJoinDriver(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {		
		if (args.length != 3) {
			System.out.println("usage MapSideJoinDriver <productsData> <salesData> <ResultsDir>\n");
			return -1;
		}
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("MapSideJoin");
		job.setJarByClass(MapSideJoinDriver.class);
		
		String productsDataFile = args[0];
		String salesDataFile = args[1];
		String outputDir = args[2];
		
		Path outPath = new Path(outputDir);
		
		//get the ProductsData file to all our mappers...
		//Old way
		//DistributedCache.addCacheFile(new URI(productsDataFile), conf);
		
		//new way, if added with a #, open the file directly in the mapper using FileInputStream
		//job.addCacheFile(new URI(productsDataFile + "#theproducts"));
		
		job.addCacheFile(new URI(productsDataFile));
		
		FileInputFormat.setInputPaths(job, new Path(salesDataFile));
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(SalesMapper.class);
		job.setReducerClass(SalesReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		
		/* Delete output filepath if already exists */
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
		
	}
	
	public static class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		private static HashMap<String, String> ProductsMap = new HashMap<String, String>();
		
		private Text product = new Text();
		private DoubleWritable saleValue = new DoubleWritable();
		
		protected void setup(Context context) throws IOException,
		InterruptedException {

				Configuration conf = context.getConfiguration();
				//old way
				//Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(conf);

				
				//new way
				//File f = new File("./theproducts"); //this name should match with the name that we have used in the driver
				
				
				//or run a loop on the context.getCacheFiles() and pick all the files that user has provided
				 for ( URI uri : context.getCacheFiles()) {
					 
					InputStream ins = FileSystem.get(uri, context.getConfiguration()).open(new Path(uri));
					 
					BufferedReader in = new BufferedReader( new InputStreamReader(ins));
					String line = null;
					while((line = in.readLine())!=null){
						String[] productData = line.split("\\t");
						ProductsMap.put(productData[0].trim(), productData[1].trim());
					}
					in.close();
				 }
				
		}



		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data = value.toString().split("\\t");
			String productID = data[4].trim();
			String lineTotal = data[8].trim();
			
			product.set(ProductsMap.get(productID));
			saleValue.set(Double.parseDouble(lineTotal));
			context.write(product, saleValue);
			
		}
		
	}
		
	public static class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
		
		@Override
		public void reduce(final Text key, final Iterable<DoubleWritable> values,
				final Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));
			
		}
		   
	            
	}
	
	
}
