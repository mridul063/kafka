package com.github.mridul.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mridul.kafka.utils.Constants;

public class ConsumerGroups {

	private final static Logger logger = LoggerFactory.getLogger(ConsumerGroups.class);

	public static void main(String[] args) {

		String group_id = "first_application";
		String topic = "topic_1";

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

		// Poll new data
		while (true) {
			// consumer.poll(100); // deprecated
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Key : {} \n Value : {} \n Partition : {} \n Offset: {}", record.key(), record.value(),
						record.partition(), record.offset());
			}
		}

	}

}
