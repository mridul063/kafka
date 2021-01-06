package org.kafka.consumer.elastic.search;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mridul.kafka.utils.Constants;

public class ElasticSearchConsumer {
	
	public static RestHighLevelClient createClient() {
		
		//Replace with your own credentials
		String hostname = "kafka-2779789376.ap-southeast-2.bonsaisearch.net";
		String username = "llha5cxvtl";
		String password = "bzbupegrhk";
		
		//Don't do if u run local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});		
		
		RestHighLevelClient restClient = new RestHighLevelClient(builder);
		return restClient;
	}
	
	public static KafkaConsumer<String, String> createConsumer(String topic) {
		String group_id = "kafka-demo-elastic-search";

		// Consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // can be latest/earliest/none ,
																						// none throws error if no
																						// offsets are saved

		// Create a consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// Subscribe to the topic
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}
	
	public static void main(String[] args) throws IOException {

		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

		RestHighLevelClient client = createClient();

		//String jsonString = "{\"foo\":\"bar\"}";

		KafkaConsumer<String, String> kafkaConsumer = createConsumer("twitter_tweets");
		
		// Poll new data
		while (true) {
			// consumer.poll(100); // deprecated
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0
			for (ConsumerRecord<String, String> record : records) {
				// insert data into elastic search
				IndexRequest indexRequest = new IndexRequest("twitter").source(record.value(), XContentType.JSON);
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				String id = indexResponse.getId();
				logger.info(id);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}

			}
		}

		// close the client gracefully
		//client.close();
	}

}
