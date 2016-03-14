package com.ixat.storm._StormTest_1;


import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class TridentClickStreamTopology {

	public static void main(String args[]){
		
		 TridentTopology topology = new TridentTopology();
		 WebSiteClickSpout spout = new WebSiteClickSpout();
		 Count c = new Count();
		 c.zero();
		 
   	 topology.newStream("clickStreamSource",spout)  
   	 .groupBy(new Fields("clickedSite"))
        .aggregate(new Fields("clickedSite"), c, new Fields("count"))
        .each(new Fields("clickedSite","count"), new Debug());  
   	
   	 Config config = new Config();
        
   	 LocalCluster localCluster = new LocalCluster();

   	 localCluster.submitTopology("WordCount",config,topology.build());
	}

	
	public static class  WebSiteClickSpout extends BaseRichSpout{

		private SpoutOutputCollector collector;
		private int taskID;
		private static final Map<Integer, String> map =
				new HashMap<Integer, String>();
				static {
					map.put(0, "www.twitter.com");
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
			/*try{
				Thread.sleep(100);
			}catch(Exception ex){}*/
			Random r = new Random();
			String clickedSite = map.get(r.nextInt(10)+1);
			//String clickedSite = map.get(0);
			System.out.println("EMMITTED:" + clickedSite);
			collector.emit(new Values(clickedSite));// + "/" + taskID));
		
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
	
}
