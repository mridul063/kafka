package com.github.mridul.kafka.twitter.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	private final static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

	private String consumerKey = "dvnqeeCMQZZkgj2eddMoDD4T1";
	private String consumerSecret = "Mxvykv3QGncUThkxgyogNtDIR9ras2KGsA6NjyXdXtc9NbZDKM";
	private String token = "3890264593-hORhPlagjaKqPuKM4GN2vETS1GfQUSmtNEHMwxp";
	private String secret = "9RFTzk6MndDL6PbzQHs1GO0hhYCw8NQ5rZefLFnd0EWB9";
	private String BOOTSTRAP_SERVER = "localhost:9092";

	public TwitterProducer() {

	}

	public static void main(String[] args) {
		TwitterProducer twitterProducer = new TwitterProducer();
		twitterProducer.run();
	}

	public void run() {
		// create twitter client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client = createTwitterClient(msgQueue);
		client.connect();

		// kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		// loop to send tweets
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (null != msg) {
				logger.info(msg);
				// producer record
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweets", null, msg);
				
				// send data
				producer.send(record, new Callback() {

					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (null != exception) {
							logger.error("Error while producing : {}", exception.getMessage());
						}
					}
				});
			}
		}
		logger.info("Application ends");
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// safe producer properties
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep 5 otherwise 1
		
		//high throughput producer (At the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB
		
		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("dollar");
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

}
