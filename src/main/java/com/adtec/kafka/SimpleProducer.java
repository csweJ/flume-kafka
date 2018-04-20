package com.adtec.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer extends Thread {
    private static final Logger LOGGER=LoggerFactory.getLogger(SimpleProducer.class);
    
    private  final KafkaProducer<String, String> producer;
    private  final String topic;
    private  final Boolean isAsync;
    
    public SimpleProducer(String topic,Boolean isAsync) {
        this.topic=topic;
        this.isAsync=isAsync;
        Properties prop=new Properties();
        //broker节点
        prop.put("bootstrap.servers", "10.10.20.101:21005,10.10.20.102:21005,10.10.20.103:21005,10.10.20.104:21005");
        //序列化类，
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //producer发送消息后是否等待broker的ACK，默认是0，1 表示等待ACK，保证消息的可靠性
        prop.put("request.required.acks", "1");
        //第一个泛型表示分区key，第二个表示消息类型
        producer=new KafkaProducer<String, String>(prop);
        
    }
    
    
    @Override
    public void run() {
        long customerNO=10000L;
        while(true) {
            
            long startTimestamp=System.currentTimeMillis();
            String messageStr=customerNO+"|"+startTimestamp+"|"+KafkaMain.getCount();
            
            try {
                producer.send(new ProducerRecord<String, String>(topic, KafkaMain.getCount().toString(), messageStr)).get();
               // LOGGER.info("Sent message: (" + KafkaMain.getCount() + ", " + messageStr + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            ++customerNO;
            if(customerNO==10099 ) {
                customerNO=10000L;
            }
            KafkaMain.inc();
           
        }
    }
    
}

