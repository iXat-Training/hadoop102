package com.ixat.kafka.twitterstram;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {
	public static void main(String args[]) throws Exception {
		Properties props = new Properties();
		props.put("metadata.broker.list","localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i < 100; i++) {
			String msg = "Producer Test#" + i;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"NEWS", "KEY:" + i, msg);
			producer.send(data);
		}
		System.out.println("Messages sent....");
	}
}
