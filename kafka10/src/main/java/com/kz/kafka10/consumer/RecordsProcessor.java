package com.kz.kafka10.consumer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used as worker to process consumer records, if processing of records takes long
 * and needs to be multi-threaded. It has no reference to kafka consumer, only needs consumer records
 * to process.
 */
public class RecordsProcessor implements Runnable {
	protected static final Logger log = LoggerFactory.getLogger(RecordsProcessor.class);
	protected static AtomicInteger instanceNum = new AtomicInteger(0);
	protected static AtomicLong processedCount = new AtomicLong(0);
	protected ConsumerRecords<?, ?> records;
     
    public RecordsProcessor(ConsumerRecords<?, ?> records){
        this.records = records;
        instanceNum.incrementAndGet();
    }
 
    @Override
    public void run() {
    	//Thread.currentThread().setName("RecordsProcessor-"+instanceNum);
        long startTime = System.nanoTime();
        processRecords();
        log.info("Processed {} records in {}µs ",records.count(), ((System.nanoTime()-startTime)/1000));
    }
 
    protected void processRecords() {
        try {
        	for (ConsumerRecord<?, ?> record : records) {
        		// log the meta data for now
        		log.info("[{}-{}] offset={} timestamp={} valueSize={}",
        				record.topic(),record.partition(),record.offset(),record.timestamp(),record.serializedValueSize());
        		// TODO: add processing of consumer records here
        		log.info("** record.value()="+record.value());
        		// increase the processed record global counter across all threads
        		processedCount.incrementAndGet();
    		}
        } catch (Exception e) {
            log.error("Failed to process ",e);
        }
    }

	public static Long getProcessedCount() {
		return processedCount.get();
	}
}