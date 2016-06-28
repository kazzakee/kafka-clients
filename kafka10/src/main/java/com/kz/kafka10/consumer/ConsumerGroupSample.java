package com.kz.kafka10.consumer;
 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kz.kafka10.utils.PropsUtil;
 
/**
 * This class demonstrates a simple group consumer for kafka v0.10
 * Uses executor service to spawn multi-threaded consumer and records processor
 *
 */
public class ConsumerGroupSample {
	protected static final Logger log = LoggerFactory.getLogger(ConsumerGroupSample.class);
	protected static Properties props = new Properties();
    
    protected KafkaConsumer<String, String> consumer;
    protected ScheduledExecutorService executor = null;
    protected List<String> topics;
    protected int poolSize;
    protected List<ConsumerWorker> consumerList = new ArrayList<ConsumerWorker>();
    protected List<RecordsProcessor> processorList = new ArrayList<RecordsProcessor>();
 
    static {
    	PropsUtil.loadProps(props, "consumer.properties");
    	PropsUtil.loadProps(props, "consumer_override.properties");
    }
    
    public ConsumerGroupSample(String[] topics, int poolSize) {
    	this.topics = Arrays.asList(topics);
    	this.poolSize = poolSize;
    }
  
    public void start() throws Exception {
        executor = Executors.newScheduledThreadPool(topics.size()*poolSize);
        for(int k=0; k<topics.size(); k++) {
        	log.info("Starting consumer threads");
	        consumer = new KafkaConsumer<String, String>(props);
	    	ConsumerWorker consumeLoop = new ConsumerWorker(consumer, topics) {
	    		@Override
				protected void process(ConsumerRecords<?, ?> records) {
	    			if(records.count() > 0) {
						//Add consumer records processor thread
						RecordsProcessor processor = new RecordsProcessor(records);
						processorList.add(processor);
						executor.execute(processor);
	    			}
				}
			};
			consumerList.add(consumeLoop);
			executor.execute(consumeLoop);
        }
    }

	public void shutdown() {
		log.info("Shutdown() started");
		for(ConsumerWorker consumeLoop: consumerList)
			consumeLoop.setRunning(false);
		if (executor != null)
			executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				log.info("Timed out waiting for consumer threads to shut down, exiting...");
			}
		} catch (InterruptedException e) {
			log.error("Interrupted during shutdown, exiting...");
		}
		log.info("total records consumed={}",ConsumerWorker.getConsumedCount());
		log.info("total records processed={}",RecordsProcessor.getProcessedCount());
		log.info("Shutdown() completed");
	}

    public static void main(String[] args) {
    	String bootstrapServers = "";
    	String groupId = "";
    	int poolSize = 2; //threads per topic
    	String topics = ""; 
    	// process command line overrides
    	if(args!=null && args.length<5) {
	    	switch(args.length) {
	    	case 4:
	    		groupId = args[3];
		        log.info("groupId={}", groupId);
		        props.put("group.id", groupId);
	    	case 3:
		        bootstrapServers = args[2];
		        log.info("bootstrapServers={}", bootstrapServers);
		        props.put("bootstrap.servers", bootstrapServers);
	    	case 2:
		        poolSize = Integer.parseInt(args[1]);
		        log.info("poolSize={}", poolSize);
	    	case 1:
		        topics = args[0];
		        log.info("topics="+topics);
		        break;
	    	}
    	} else {
    		System.err.println("Invalid number of args");
    		System.err.printf("Usage: java %s <topic1,topic3> [threadPoolSize] [bootstrap.server] [groupId]",ConsumerGroupSample.class.getCanonicalName());
    		System.err.printf("\rdefault: threadPoolSize=1, bootstrap.server and groupId are read from consumer.properties ",ConsumerGroupSample.class.getCanonicalName());
    		System.exit(1);
    	}
        try {
        	ConsumerGroupSample consumerGroup = new ConsumerGroupSample(topics.split(","), poolSize);
			consumerGroup.start();
			// let it run for 60 seconds before shutting down
			try {
			    Thread.sleep(60000);
			} catch (InterruptedException ie) {
				log.error("Interrupted in sleep...");
			}
			consumerGroup.shutdown();
		} catch (Exception e) {
			log.error("Exception encountered", e);
		}
    }
}