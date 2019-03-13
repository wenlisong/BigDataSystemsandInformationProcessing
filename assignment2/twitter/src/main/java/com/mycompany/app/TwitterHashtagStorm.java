package com.mycompany.app;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterHashtagStorm {
	
	public static void main(String[] args) throws Exception {
		String consumerKey = "QzjpEe7Mk4c8W9mmrppPfEazI";
		String consumerSecret = "QBcRBN8HJIKT5nm5ngWD9rl4s3FPV2Wpoava0tX1MjYTEaLAC9";
		String accessToken = "906367537159573504-qH0zLIw0Av7ig6NLL75zwW6Qk42KIkE";
		String accessTokenSecret = "WlOYHBBz9ce99fmyi9j78SAH8MzZ9g0UYarWm18wW5I5v";

		String[] keyWords = { "trump", "Trump", "TRUMP" };
		
		Config conf = new Config();
		// config.setDebug(true);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitter-spout",new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords), 12);
		
		builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(),9).shuffleGrouping("twitter-spout");

		builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt()).fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));
		
		// LocalCluster cluster = new LocalCluster();
		StormSubmitter.submitTopology("TwitterHashtagStorm", conf,builder.createTopology());
		// Thread.sleep(30000);
		// cluster.shutdown();
	}
}