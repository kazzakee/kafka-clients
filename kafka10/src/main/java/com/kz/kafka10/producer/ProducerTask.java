package com.kz.kafka10.producer;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class ProducerTask implements Runnable {
	protected static final Logger log = LoggerFactory.getLogger(ProducerTask.class);
	
	protected int MAX_DELAY_MILLISEC = 50;
	protected Producer<String,String> producer;
	protected long events;
	protected String topic;
	protected Integer threadNumber;
	protected CountDownLatch latch;
	protected String threadName = "";

    public ProducerTask(Producer<String, String> producer, String topic, long events, int threadNumber, CountDownLatch latch) {
        this.producer = producer;
        this.topic = topic;
        this.events = events;
        this.threadNumber = threadNumber;
        this.latch = latch;
    }
    
    public void setLatch(CountDownLatch latch) {
    	this.latch = latch;
    }
    
    public void run() {
    	threadName = topic+"-ProducerTask-" + threadNumber;
    	Thread.currentThread().setName(Thread.currentThread().getName()+" | "+threadName);
        log.info("Task started");
        long counter=0;        
        for (long nEvents = 0; nEvents < events; nEvents++) {
           	try {
				producer.send(getNextRecord(nEvents), getNextCallback());
				if (++counter%1000 == 0) {
					log.info("sent [{} messages]", counter);
					delay(MAX_DELAY_MILLISEC);
				}
			} catch (Exception e) {
				log.error("Failed sending record",e);
			}
        }
        try {
			latch.countDown();
		} catch (Exception e) {
			log.error("countDown failed",e);
		}
        log.info("Task completed");
    }

	protected abstract ProducerRecord<String,String> getNextRecord(long eventNum);
	protected abstract String getNextKey(long eventNum);
	protected abstract String getNextValue(long eventNum);
	protected abstract Callback getNextCallback();

	protected void delay(int millies) {
		try {
			Thread.sleep(millies);
		} catch (InterruptedException e) {
			log.error("Interrupted", e);
		}
		
	}
}
