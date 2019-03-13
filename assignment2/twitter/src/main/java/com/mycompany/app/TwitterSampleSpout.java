package com.mycompany.app;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

@SuppressWarnings("serial")
public class TwitterSampleSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue = null;
	private TwitterStream twitterStream;
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;
	private String[] keyWords;

	public TwitterSampleSpout() {
	}
	
	public TwitterSampleSpout(String consumerKey, String consumerSecret,String accessToken, String accessTokenSecret, String[] keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

		this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener(){

			public void onException(Exception ex) {

			}

			public void onStatus(Status status) {
				queue.offer(status);
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

			}

			public void onScrubGeo(long userId, long upToStatusId) {

			}

			public void onStallWarning(StallWarning warning) {

			}
			
		};
		this.twitterStream.addListener(listener);

		if (keyWords.length == 0) {
			this.twitterStream.sample();
		}else {
			FilterQuery query = new FilterQuery().track(keyWords);
			this.twitterStream.filter(query);
		}
	}

	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			this.collector.emit(new Values(ret));
		}
	}

	public void close() {
		this.twitterStream.shutdown();
	}

	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}