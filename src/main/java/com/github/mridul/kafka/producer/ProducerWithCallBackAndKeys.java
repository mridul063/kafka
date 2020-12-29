package com.github.mridul.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mridul.kafka.utils.Constants;

public class ProducerWithCallBackAndKeys {

	final static Logger logger = LoggerFactory.getLogger(ProducerWithCallBackAndKeys.class);

	public static void main(String[] args) {
		System.out.println("hello world!!");

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			String topic = "topic_1";
			String message = "Message : " + i;
			String key = "Id_" + i;
			// producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);

			// send data
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// On success log the details
					if (null == exception) {
						logger.info("Topic: {} \n Partition: {} \n Offset: {} \n Timestamp : {}", metadata.topic(),
								metadata.partition(), metadata.offset(), metadata.timestamp());
					} else {
						logger.error("Error while producing : {}", exception.getMessage());
					}

				}
			});
		}
		// producer.flush();
		// close
		producer.close();

	}

}
