package com.ixat.mr.joins.MRJoins;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class SalesMapperTests {

	private MapSideJoinDriver.SalesMapper mapper;
	private MapDriver driver;
	
	@Before public void setUp(){
		mapper = new MapSideJoinDriver.SalesMapper();
		driver = new MapDriver(mapper);
		driver.addCacheFile("./data/products.tsv");
	}

	@Test public void testMap() throws IOException{
		//driver.addCacheFile("./data/products.tsv#theproducts");
		driver.withInput(new LongWritable(1), new Text("43659	1	4911-403C-98	1	776	1	2024.9940	.0000	2024.994000	B207C96D-D9E6-402B-8470-2CC176C42283	2005-07-01 00:00:00.000"));
		driver.withOutput(new Text("Mountain-100 Black, 42"), new DoubleWritable(2024.994000));
		driver.runTest();
	}
}
