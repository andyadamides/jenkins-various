package com.andy.id.kafka;


/*
 * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 */

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	public static void main(String[] args){
		long events = Long.parseLong(args[0]);
		
		//log4j needs setting up properly
		org.apache.log4j.BasicConfigurator.configure();
		Properties props = new Properties();
		
		//find a one or more Brokers to determine the Leader for each topic, at least 2
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//optional partitioning scheme
		//props.put("partitioner.class", "example.producer.SimplePartitioner");
		//require an acknowledgement from the Broker that the message was received
		//avoid data loss
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);
		
		
		/*
		 *Note that the Producer is a Java Generic and you need to tell it the 
		 *type of two parameters. The first is the type of the Partition key, 
		 *the second the type of the message. In this example they are both Strings, 
		 *which also matches to what we defined in the Properties above.
		 */
		Producer<String, String> producer = new Producer<String, String>(config);
		
		
		//Build messages
		for (long nEvents = 0; nEvents < events; nEvents++) { 
			Random rnd = new Random();		 
			long runtime = new Date().getTime();
			String ip = "192.168.2." + rnd.nextInt(255);
			String msg = runtime + ",www.example.com," + ip;
			
			//Send to broker
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
			producer.send(data);
		}
		
		producer.close();
		
	}
	
	
}
