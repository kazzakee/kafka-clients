package com.kz.kafka10.producer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kz.kafka10.utils.PropsUtil;
import com.kz.kafka10.utils.shutdown.ShutdownDelegate;
import com.kz.kafka10.utils.shutdown.Shutdownable;

public class ProducerSample implements Shutdownable {
	protected static final int MESSAGE_BYTES = 5*1024*1024;
	protected static final int BYTES_PER_SEC = 3*1024*1024;
	protected static final Logger log = LoggerFactory.getLogger(ProducerSample.class);
	protected static Properties props = new Properties();
	
	protected Producer<String, String> producer;
	protected ScheduledExecutorService executor;
	protected List<ProducerTask> producerTasks = new ArrayList<ProducerTask>();
	protected CountDownLatch latch = null;
	
	private List<String> topics;
	protected long events = 0;
	protected int poolSize;
	protected AtomicLong sentCount = new AtomicLong(0);
	protected AtomicLong failCount = new AtomicLong(0);
	protected AtomicLong ackCount = new AtomicLong(0);
	protected ObjectMapper mapper = new ObjectMapper();

    static {
    	PropsUtil.loadProps(props, "producer.properties");
    	PropsUtil.loadProps(props, "producer_override.properties");
    }

	public ProducerSample(String[] topics, long events, int poolSize) {
		this.topics = Arrays.asList(topics);
		this.events = events;
		this.poolSize = poolSize;
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
	}

	public void start() throws Exception {
        executor = Executors.newScheduledThreadPool(poolSize*topics.size());
        latch = new CountDownLatch(poolSize);
        for(String topic : topics) {
	        for(int threadNum=0; threadNum<poolSize; threadNum++) {
	        	log.info("Starting producer threads");
	    		producer = new KafkaProducer<String,String>(props);
	    		ProducerTask producerTask = new ProducerTask(producer, topic, events, threadNum, latch) {
	    			Random rnd = new Random();
					@Override
				    protected ProducerRecord<String,String> getNextRecord(long eventNum) {
						sentCount.incrementAndGet();
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
						try {
							return toJson(new RecordData(System.currentTimeMillis(), Thread.currentThread().getName(), 
									"192.168.22."+rnd.nextInt(255), new BigInteger(MESSAGE_BYTES, rnd).toString(MESSAGE_BYTES)));
						} catch (JsonProcessingException e) {
							throw new RuntimeException("Record generation failed", e);
						}
					}
				}; 
				producerTasks.add(producerTask);
				executor.execute(producerTask);
	        }
        }
    }

	@Override
	public CountDownLatch getLatch() {
		return latch;
	}

	@Override
	public void shutdown() {
		shutdownAndAwaitTermination();
		if (producer != null) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
			
			producer.close(10, TimeUnit.SECONDS);
		}
	}
	
	protected void shutdownAndAwaitTermination() {
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
				executor.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
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

	public long getSentCount() {
		return sentCount.get();
	}

	public long getFailCount() {
		return failCount.get();
	}

	public long getAckCount() {
		return ackCount.get();
	}
		
	public String toJson(Object data) throws JsonProcessingException {
		return mapper.writeValueAsString(data);
	}

	protected class ProducerCallback implements Callback {

		@Override
		public void onCompletion(RecordMetadata meta, Exception e) {
			if (e != null) {
				failCount.incrementAndGet();
				log.error("Callback rerurned an error.", e);
			}
			if(meta != null) {
				ackCount.incrementAndGet();
				if(ackCount.get()%1000==0)
					log.info("callback returned meta.offset(): {}, meta.partition(): {}",meta.offset(), meta.partition());
			}
		}
	}

	protected class RecordData {
		protected long timestamp;
		protected String threadName;
		protected String ip;
		protected String data;
		public RecordData(long timestamp, String threadName, String ip, String largeField) {
			this.timestamp = timestamp;
			this.threadName = threadName;
			this.ip = ip;
			this.data = largeField;
		}
		@Override
		public String toString() {
			return "timestamp="+timestamp + ",threadName=" + threadName+",largeField="+ data + ",ip="+ip;
		}
	}
	
	public static void main(String[] args) {
		long timeStart =  System.nanoTime();
		if(args.length < 1) {
			System.err.println("Usage: java com.kz.kafka10.producer.ProducerSample <topic1,topic2>");
			System.exit(0);
		}
		String[] topics = args[0].split(",");
		int threads = 1;
		long numOfMessages = 100;
		long TTL = numOfMessages*(MESSAGE_BYTES/BYTES_PER_SEC)*1000;
		log.info("Starting {} threads producing {} messages each", threads, numOfMessages);
		ProducerSample producerSample = new ProducerSample(topics, numOfMessages, threads);
		try {
			ShutdownDelegate shutdownWaiter = new ShutdownDelegate(producerSample, 60000, log);
			producerSample.start();
			shutdownWaiter.waitAndShutdown(TTL);
		} catch (Exception e) {
			log.error("Failed due to exception", e);
		}
		log.info("ProducerRecords sent={} success.ack={} failed.ack={}",
				producerSample.getSentCount(),producerSample.getAckCount(),producerSample.getFailCount());
		log.info("All done in {}ms", (System.nanoTime()-timeStart)/1000000);
	}
}
