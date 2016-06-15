package com.kz.kafka10.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kz.kafka10.utils.PropsUtil;

public class ProducerSample {
	protected static final Logger log = LoggerFactory.getLogger(ProducerSample.class);
	
	protected Producer<String, String> producer;
	protected ScheduledExecutorService executor;
	protected List<ProducerTask> producerTasks = new ArrayList<ProducerTask>();
	
	private List<String> topics;
	protected long events = 0;
	protected int poolSize;

	public ProducerSample(String[] topics, long events, int poolSize) {
		this.topics = Arrays.asList(topics);
		this.events = events;
		this.poolSize = poolSize;
	}

	public void start() throws Exception {
        executor = Executors.newScheduledThreadPool(poolSize*topics.size());
        CountDownLatch latch = new CountDownLatch(poolSize);
        for(String topic : topics) {
	        for(int threadNum=0; threadNum<poolSize; threadNum++) {
	        	log.info("Starting producer threads");
	    		producer = new KafkaProducer<String,String>(PropsUtil.loadProps("producer.properties"));
	    		ProducerTask producerTask = new ProducerTask(producer, topic, events, threadNum, latch) {
	    			Random rnd = new Random();
					@Override
				    protected ProducerRecord<String,String> getNextRecord(long eventNum) {
						return new ProducerRecord<String, String>(topic, getNextKey(eventNum), getNextValue(eventNum));
					}
					@Override
					protected Callback getNextCallback() {
						return new ProducerCallback();
					}
					@Override
					protected String getNextKey(long eventNum) {
						return null;
					}
					@Override
					protected String getNextValue(long eventNum) {
						return new Date().getTime() + "," + threadName + "-" + eventNum + ",www.example.com," + "192.168.2." + rnd.nextInt(255);
					}
				}; 
				producerTasks.add(producerTask);
				executor.execute(producerTask);
	        }
        }
    }

	public void shutdownAndAwaitTermination() {
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				executor.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
					log.error("Pool did not terminate");
				}
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	public void shutdown() {
		shutdownAndAwaitTermination();
		if (producer != null) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
			producer.close();
		}
	}

	protected class ProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata meta, Exception e) {
			if (e != null) {
				log.error("Callback rerurned an error.", e);
			}
			if(meta != null) {
				log.info("meta.offset(): {}, meta.partition(): {}",meta.offset(), meta.partition());
			}
		}
	}

	public static void main(String[] args) {
		String[] topics = "btmm,btmm".split(",");
		int threads = 2;
		long numOfMessages = 1000;
		log.info("Starting {} threads producing {} messages each", threads, numOfMessages);
		try {
			ProducerSample producerSample = new ProducerSample(topics, numOfMessages, threads);
			producerSample.start();
			// let it run for 60 seconds before shutting down
			try {
			    Thread.sleep(5000);
			} catch (InterruptedException ie) {
				log.error("Interrupted in sleep...");
			}
			producerSample.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
		log.info("All done. Exiting.");
	}
}
