package com.github.mridul.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mridul.kafka.utils.Constants;

public class ConsumerWithThread {

	private final static Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);

	public static void main(String[] args) {
		new ConsumerWithThread().run();
	}
	
	public void run() {
		String groupId = "second_application";
		String topic = "topic_1";
		
		// Latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		logger.info("Creating the consumer thread");
		Runnable consumerRunnable = new ConsumerRunnable(topic, groupId, latch);
		
		// Start consumer thread
		Thread consumerThread = new Thread(consumerRunnable);
		consumerThread.start();
		
		//add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->  {
			logger.info("Caught shutdown hook");
			((ConsumerRunnable)consumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.info("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
		
	}

	public class ConsumerRunnable implements Runnable {
		
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(String topic, String groupId, CountDownLatch latch) {
			this.latch = latch;
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // can be latest/earliest/none
																							// ,
			consumer = new KafkaConsumer<String, String>(properties);
			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {
			try {
				// Poll new data
				while (true) {
					// consumer.poll(100); // deprecated
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0
					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key : {} \n Value : {} \n Partition : {} \n Offset: {}", record.key(),
								record.value(), record.partition(), record.offset());
					}
				}

			} catch (WakeupException exception) {
				logger.info("Received shutdown exception");
			} finally {
				consumer.close();
				// tell our main code we are done with the consumer
				latch.countDown();
			}
		}

		public void shutdown() {
			// this method is to interupt the poll
			// it will throw the exception WakeUpException
			consumer.wakeup();

		}
	}

}
