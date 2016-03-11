package com.ixat.kafka.twitterstram;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class GetTwitterData {

	public static void main(String args[]){
		 ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.setDebugEnabled(true);
         cb.setOAuthConsumerKey(Constants.consumerKey);
         cb.setOAuthConsumerSecret(Constants.consumerSecret);
         cb.setOAuthAccessToken(Constants.token);
         cb.setOAuthAccessTokenSecret(Constants.secret);

         TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

         StatusListener listener = new StatusListener() {
 
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
                 

            	 System.out.println(status.getId() + "  --> " + status.getText());
             }

        
             public void onTrackLimitationNotice(int arg0) {
                 // TODO Auto-generated method stub

             }

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}

         };
         FilterQuery fq = new FilterQuery();
     
         String keywords[] = {"trump","donald_trump","donald-trump","donald trump"};

         fq.track(keywords);
         
         twitterStream.addListener(listener);
         twitterStream.filter(fq);  
        
     }
		
	
}
