package com.adtec.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer extends Thread {
    private static final Logger LOGGER=LoggerFactory.getLogger(Producer.class);
    
    private  final KafkaProducer<String, String> producer;
    private  final String topic;
    private  final Boolean isAsync;
    
    public Producer(String topic,Boolean isAsync) {
        this.topic=topic;
        this.isAsync=isAsync;
        Properties prop=new Properties();
        //broker节点
        prop.put("bootstrap.servers", "10.10.20.52:21005,10.10.20.53:21005,10.10.20.54:21005");
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
            String messageStr=customerNO+","+startTimestamp+","+KafkaMain.getCount();
            if(isAsync) {
                //异步回调
                producer.send(new ProducerRecord<String, String>(topic, KafkaMain.getCount().toString(), messageStr), new DemoCallBack(startTimestamp, KafkaMain.getCount().toString(), messageStr));
            }else {
                try {
                    producer.send(new ProducerRecord<String, String>(topic, KafkaMain.getCount().toString(), messageStr)).get();
                   // LOGGER.info("Sent message: (" + KafkaMain.getCount() + ", " + messageStr + ")");
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
            }
            ++customerNO;
            if(customerNO==10099 ) {
                customerNO=10000L;
            }
            KafkaMain.inc();
           
        }
    }
    
}

class DemoCallBack implements Callback{
    private static final Logger LOGGER=LoggerFactory.getLogger(DemoCallBack.class);
    
    private final long startTime;
    private final String key;
    private final String message;
    
    public DemoCallBack(long startTime, String key,String message) {
        this.startTime=startTime;
        this.key=key;
        this.message=message;
    }
    
    
    //RecordMetadata 元数据记录，如分区和偏移位置offset  如果为空则说明发生错误
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime=System.currentTimeMillis()-startTime;
        if(metadata!=null) {
            LOGGER.info(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                        "), " +
                        "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        }else {
           LOGGER.error(exception.getMessage());
        }
        
    }
    
}