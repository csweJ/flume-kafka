package com.adtec.kafka;


public class KafkaMain {
    public static Long count = 0L; 
    
    public static void main(String[] args) {
        
        int threadNum=1;
        while(threadNum<=10) {
          
            new SimpleProducer("new_test_topic", false).start();
            ++threadNum;
            
        }
        
        
       
    }
    
    public synchronized static void inc() {
        ++count;
    }
    
    public static Long getCount() {
        return count;
    }
    
    

}
