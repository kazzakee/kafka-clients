package com.kz.kafka10.consumer;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessorThread implements Runnable{
	private static final Logger log = LoggerFactory.getLogger(ProcessorThread.class);
	private static AtomicLong consumedCount = new AtomicLong(0);
	private ConsumerRecords<String, String> consumerRecords;
     
    public ProcessorThread(ConsumerRecords<String, String> consumerRecords){
        this.consumerRecords = consumerRecords;
    }
 
    @Override
    public void run() {
        long startTime = System.nanoTime();
        processCommand();
        log.info("record processor took time="+((System.nanoTime()-startTime)/1000)+"µs");
    }
 
    private void processCommand() {
        try {
        	for (ConsumerRecord<String, String> record : consumerRecords) {
        		log.info("["+record.topic()+"-"+record.partition()+"] offset="+record.offset()+" timestamp="+record.timestamp()+" valueSize="+record.serializedValueSize());
        		consumedCount.incrementAndGet();
    		}
        } catch (Exception e) {
            log.error("Failed to process ",e);
        }
    }

	public static Long getConsumedCount() {
		return consumedCount.get();
	}
}