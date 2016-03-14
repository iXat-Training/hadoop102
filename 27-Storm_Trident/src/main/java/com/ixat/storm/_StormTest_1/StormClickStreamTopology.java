package com.ixat.storm._StormTest_1;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StormClickStreamTopology {

	public static void main(String[] args) throws Exception {
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("website-click-spout", new WebSiteClickSpout(),4);
		builder.setBolt("website-click-counter-bolt", new ClickCounterBolt(),2).shuffleGrouping("website-click-spout");
		
		doLocal(config,builder);
		//doCluster(config,builder);
		
	}
	
	private static void doLocal(Config config,TopologyBuilder builder)throws Exception{
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("ClickStorm", config, builder.createTopology());
		Thread.sleep(10000);		
		cluster.shutdown();
		
	}

	private static void doCluster(Config config,TopologyBuilder builder)throws Exception{
		
		StormSubmitter.submitTopology("WebSiteClick-STORM", config, builder.createTopology());
	}
	
	
	public static class WebSiteClickSpout extends BaseRichSpout{

		private SpoutOutputCollector collector;
		private int taskID;
		private static final Map<Integer, String> map =
				new HashMap<Integer, String>();
				static {
				
					map.put(1, "www.amazon.in");
					map.put(2, "www.flipkart.com");
					map.put(3, "www.snapdeal.com");
					map.put(4, "www.twitter.com");
					map.put(5, "www.youtube.com");
					map.put(6, "www.yahoo.com");
					map.put(7, "www.facebook.com");
					map.put(8, "www.shopclues.com");
					map.put(9, "www.google.com");
					map.put(10, "www.ebay.in");
				}
				
				
		public void nextTuple() {
			// TODO Auto-generated method stub
			Random r = new Random();
			String clickedSite = map.get(r.nextInt(10)+1);
			collector.emit(new Values(clickedSite + "/" + taskID));
			
		}

		
		public void open(Map arg0, TopologyContext arg1,
				SpoutOutputCollector arg2) {
			// TODO Auto-generated method stub
			collector = arg2;
			taskID = arg1.getThisTaskIndex();
		}

		
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("clickedSite"));
		}

	}
	
	
	public static class ClickCounterBolt extends BaseRichBolt{

		Map<String, Integer> counters;
		
		public void execute(Tuple arg0) {
			// TODO Auto-generated method stub
			//String webSite = arg0.getString(0); (or)
			String webSite = arg0.getStringByField("clickedSite");
			
			
			if(!counters.containsKey(webSite)){
				counters.put(webSite, 1);
			}else{
				Integer c = counters.get(webSite) +1;
				counters.put(webSite, c);
			}
			System.out.println(counters);
		}

		
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
			counters = new HashMap<String, Integer>();
			
		}

		
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}
		
		
	}
	
	
}
