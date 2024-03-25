package com.app.tcs;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
		
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "New message 1");
		
		producer.send(producerRecord, new Callback() {  
		    @Override
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {  
		        if (e== null) {  
		            System.out.println("Successfully received the details as: \n" +  
		                    "Topic: " + recordMetadata.topic() + "\n" +  
		                    "Partition: " + recordMetadata.partition() + "\n" +  
		                    "Offset " + recordMetadata.offset() + "\n" +  
		                    "Timestamp " + recordMetadata.timestamp());  
		                      }  
		  
		         else {  
		        	 System.out.println("Can't produce,getting error");  
		  
		        }  
		    }

			 
		});
		producer.flush();
		producer.close();
		
		
		
		
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        // create consumer configs
        Properties properties1 = new Properties();
        properties1.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties1.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties1.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties1.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties1.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties1);
        
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        
        // poll for new data
        while(true)
        {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records)
            {
                System.out.println("Key: " + record.key() + ", Value: " + record.value());
                System.out.println("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
            //consumer.close();

        }
        
        		
	}

}
