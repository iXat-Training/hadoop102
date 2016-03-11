package com.ixat.kafka.twitterstram;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class KafkaTwitterSource {
	public static void main(String args[]) throws Exception {
		//Handle exceptions properly
		Properties props = new Properties();
		props.put("metadata.broker.list","localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(Constants.consumerKey);
		cb.setOAuthConsumerSecret(Constants.consumerSecret);
		cb.setOAuthAccessToken(Constants.token);
		cb.setOAuthAccessTokenSecret(Constants.secret);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
				.getInstance();

		StatusListenerAdapter listener = new StatusListenerAdapter(producer, "NEWS");
		
		FilterQuery fq = new FilterQuery();

		String keywords[] = { "trump", "donald_trump", "donald-trump",
				"donald trump" };

		fq.track(keywords);

		twitterStream.addListener(listener);
		twitterStream.filter(fq);

	}

	public static class StatusListenerAdapter implements StatusListener{
		private Producer<String, String> producer;
		private String topicName;
		public StatusListenerAdapter(Producer<String, String> producer, String topic){
			this.producer = producer;
			topicName = topic;
		}
		public void onException(Exception arg0) {
			// TODO Auto-generated method stub

		}

		public void onDeletionNotice(StatusDeletionNotice arg0) {
			// TODO Auto-generated method stub

		}

		public void onScrubGeo(long arg0, long arg1) {
			// TODO Auto-generated method stub

		}

		public void onStatus(Status status) {

			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					topicName, "TWEETID_"+status.getId(), status.getText());
			producer.send(data);
		}

		public void onTrackLimitationNotice(int arg0) {
			// TODO Auto-generated method stub

		}

		public void onStallWarning(StallWarning arg0) {
			// TODO Auto-generated method stub

		}
	}
}
