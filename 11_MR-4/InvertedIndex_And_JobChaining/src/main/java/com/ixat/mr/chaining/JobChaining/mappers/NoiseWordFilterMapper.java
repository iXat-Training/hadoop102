package com.ixat.mr.chaining.JobChaining.mappers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NoiseWordFilterMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static String[] noisewords = new String[] { "the", "and", "a",
			"to", "of", "in", "i", "is", "that", "it", "on", "you", "this",
			"for", "but", "with", "are", "have", "be", "at", "or", "as", "was",
			"so", "if", "out", "not","they","only","its","by","will","been","then","could","would","had","has","would","should","our","us","no","other","which","from","has","than","can","all","any","their","there","we","an","these","were",
			"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",
			"0","1","2","3","4","5","6","7","8","9",
			"one","two","tree","four","five","six","seven","eight","nine","ten"};

	List<String> noisewordsList = null;

	public void setup(Context ctx) {
		noisewordsList = Arrays.asList(noisewords);

	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		StringTokenizer st = new StringTokenizer(value.toString());
		StringBuffer buffer = new StringBuffer();
		while (st.hasMoreTokens()) {
			String word = st.nextToken().trim();
			if (!noisewordsList.contains(word)) {
				if (buffer.length() > 0)
					buffer.append(" ");
				buffer.append(word);
			} else {
				context.getCounter(
						com.ixat.mr.chaining.JobChaining.counters.MYCOUNTERS.COUNT_NOISEWORDS)
						.increment(1);

			}
		}
		context.write(key, new Text(buffer.toString()));

	}

}