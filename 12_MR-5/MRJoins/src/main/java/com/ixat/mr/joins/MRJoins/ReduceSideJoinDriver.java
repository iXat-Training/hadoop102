package com.ixat.mr.joins.MRJoins;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ixat.mr.joins.MRJoins.MapSideJoinDriver.SalesMapper;
import com.ixat.mr.joins.MRJoins.MapSideJoinDriver.SalesReducer;

public class ReduceSideJoinDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new ReduceSideJoinDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.println("usage ReduceSideJoinDriver <productsData> <salesData> <ResultsDir>\n");
			return -1;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("ReduceSideJoin");
		job.setJarByClass(ReduceSideJoinDriver.class);

		String productsDataFile = args[0];
		String salesDataFile = args[1];
		String outputDir = args[2];

		Path outPath = new Path(outputDir);
		Path products = new Path(productsDataFile);
		Path sales = new Path(salesDataFile);

		MultipleInputs.addInputPath(job, products, TextInputFormat.class,
				ProductsDataMapper.class);

		MultipleInputs.addInputPath(job, sales, TextInputFormat.class,
				SalesDataMapper.class);
		
		FileOutputFormat.setOutputPath(job, outPath);

		job.setReducerClass(ProductSalesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextDoubleWritable.class);

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

	public static class ProductsDataMapper extends
			Mapper<LongWritable, Text, Text, TextDoubleWritable> {

		private Text productID = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] productData = value.toString().split("\\t");
			productID.set(productData[0].trim());
			TextDoubleWritable productName = new TextDoubleWritable(new Text(
					productData[1].trim()));
			context.write(productID, productName);

		}
	}

	public static class SalesDataMapper extends
			Mapper<LongWritable, Text, Text, TextDoubleWritable> {

		private Text productID = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] salesData = value.toString().split("\\t");
			productID.set(salesData[4].trim());
			double salesValue = Double.parseDouble(salesData[8].trim());

			TextDoubleWritable sales = new TextDoubleWritable(
					new DoubleWritable(salesValue));
			context.write(productID, sales);

		}
	}

	public static class ProductSalesReducer extends
			Reducer<Text, TextDoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<TextDoubleWritable> values,
				Context context) throws IOException, InterruptedException {
				String productId = key.toString();
				double sum = 0.0;
				for (TextDoubleWritable value : values) {
					Writable rawValue = value.get();
					if (rawValue instanceof Text) {
						productId += " -->" + rawValue.toString();
					} else if (rawValue instanceof DoubleWritable) {
						sum += ((DoubleWritable) rawValue).get();
					}
				}
				context.write(new Text(productId), new DoubleWritable(sum));
		}
	}

	public static class TextDoubleWritable extends GenericWritable {

		private static Class<? extends Writable>[] CLASSES = null;

		static {
			CLASSES = (Class<? extends Writable>[]) new Class[] {
					Text.class,
					DoubleWritable.class
			// add as many different class as you want
			};
		}

		// this empty initialize is required by Hadoop
		public TextDoubleWritable() {
		}

		public TextDoubleWritable(Writable instance) {
			set(instance);
		}

		@Override
		protected Class<? extends Writable>[] getTypes() {
			return CLASSES;
		}

		@Override
		public String toString() {
			return "MyGenericWritable [getTypes()="
					+ Arrays.toString(getTypes()) + "]";
		}
	}

}
