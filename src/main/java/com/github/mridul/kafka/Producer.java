package com.github.mridul.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	
	public static void main(String[] args) {
		System.out.println("hello world!!");
		
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("test_topic", "Hello World New!!");
		
		// send data
		producer.send(record);
		
		//producer.flush();
		// close
		producer.close();
	}

}
