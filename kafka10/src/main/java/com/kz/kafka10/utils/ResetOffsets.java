package com.kz.kafka10.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResetOffsets {
	private static final String RESET_OFFSETS_STRATEGY = "auto.offset.reset";
	protected static final String GROUP_ID = "group.id";
	protected static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	protected static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	protected static final String TOPIC = "topic";

	protected static Logger log = LoggerFactory.getLogger(ResetOffsets.class);
	
	private KafkaConsumer<?, ?> consumer;
	private Properties props;
	private String topic;
	private Map<TopicPartition, Long> partitionOffsets = new HashMap<TopicPartition, Long>();
	private int partitionCount;
	
	private OffsetResetStrategy resetStrategy = OffsetResetStrategy.NONE;

	public static void main(String[] args) {		
		if (args.length != 1) {
			log.info("java com.apple.ireporter.agent.utility.ResetOffset <propFilePath>");
			return;
		} 
		String propPath = args[0];
		Properties props = new Properties();
		loadProps(props, propPath);
		ResetOffsets offsetResetUtil =  new ResetOffsets(props);
		try {
			offsetResetUtil.reset();
		} catch (Exception e) {
			log.error("Exception encountered", e);
		} finally {
			offsetResetUtil.closeConsumer();
		}
		log.info("ResetOffsets completed");
	}
	
	public ResetOffsets(Properties props) {
		this.props = props;
		init();
	}
	
	public void reset() {
		try {
			int tickCount = 0;
			int singleTick = 1000; //1 sec
			consumer.subscribe(Arrays.asList(topic), new GroupRebalanceListener(consumer, resetStrategy));
			partitionCount = consumer.partitionsFor(topic).size();
			while ( tickCount++ < 10) {
				@SuppressWarnings("unchecked")
				ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(singleTick);
				if(records.count() > 0) {
					log.info("records.count()="+records.count());
					for (ConsumerRecord<String, String> record : records) {
						log.info("topic-partition={}-{}, offset={}, key={}", new Object[]{record.topic(), record.partition(), record.offset(), record.key()});
						if(!partitionOffsets.containsKey(record.partition()))
							partitionOffsets.put(new TopicPartition(topic, record.partition()), record.offset());//store first consumed offset
					}
					if(partitionOffsets.size() == partitionCount) {
						try {
							resetOffset(partitionOffsets.keySet());
							consumer.commitSync();
							break;
						} catch (CommitFailedException e) {
							log.error("CommitSync failed", e);
						}
					}
				}
			}
		} catch (WakeupException e) {
			// ignore, we're closing
		} catch (Exception e) {
			log.error("Unexpected error", e);
		} finally {
			consumer.unsubscribe();
		}
	}

	protected void init() {
		printConfig();
		consumer = new KafkaConsumer<String, String>(props);
		this.topic = props.getProperty(TOPIC);
		this.resetStrategy = OffsetResetStrategy.valueOf(props.getProperty(RESET_OFFSETS_STRATEGY).toUpperCase());
	}

	public void closeConsumer() {
		if (consumer != null)
			consumer.close();
	}

	protected void printConfig() {
		String zkString = props.getProperty(ZOOKEEPER_CONNECT);
		String bootstrap = props.getProperty(BOOTSTRAP_SERVERS);
		String topic = props.getProperty(TOPIC);
		String groupId = props.getProperty(GROUP_ID);

		String params = "Using bootstrap.servers= " + bootstrap + ", zkString= " + zkString + ", topic=" + topic + ", groupId: " + groupId;
		log.info("Using: {}", params);
	}
	
	protected static void loadProps(Properties props, String filename) {
		try {
			InputStream inputStream = ResetOffsets.class.getResourceAsStream(filename);
			props.load(inputStream);
		} catch (IOException e) {
			log.error("Failed to load properties: {}", e, filename);
			throw new RuntimeException(e.getMessage());
		}		
	}
	
	public void resetOffset(Collection<TopicPartition> partitions) {
		log.info("resetOffset called");
		if (!partitions.isEmpty()) {
			log.info("seeking partitions offsets to {}", resetStrategy);
			if(resetStrategy == OffsetResetStrategy.EARLIEST) {
				consumer.seekToBeginning(partitions);							
			}
			if (resetStrategy == OffsetResetStrategy.LATEST) {
				consumer.seekToEnd(partitions);
			}
			for(TopicPartition topicPart : partitions) {
				if(resetStrategy == OffsetResetStrategy.NONE)
					consumer.seek(topicPart, partitionOffsets.get(topicPart));							
				log.info("after seek, committed offset for {}-{} is {}", new Object[]{topicPart.topic(), topicPart.partition(), consumer.position(topicPart)});
			}
			consumer.commitAsync();
		}
	}
	
	public static class GroupRebalanceListener implements ConsumerRebalanceListener {
		protected static final Logger log = LoggerFactory.getLogger(GroupRebalanceListener.class);
		private KafkaConsumer<?,?> consumer = null;
		private OffsetResetStrategy resetTo;
		
		public GroupRebalanceListener(KafkaConsumer<?,?> consumer) {
			this(consumer, OffsetResetStrategy.NONE);
		}
		
		public GroupRebalanceListener(KafkaConsumer<?,?> consumer, OffsetResetStrategy resetTo) {
			this.consumer = consumer;
			this.resetTo = resetTo;
		}
		
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			printPartitions(partitions, new StringBuffer("revoked: "));
			if (!partitions.isEmpty()) 
				consumer.commitAsync();
		}
		
		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			printPartitions(partitions, new StringBuffer("assigned: "));
			for(TopicPartition topicPart : partitions) {
				log.info("last committed offset for {}-{} is {}", new Object[]{topicPart.topic(), topicPart.partition(), consumer.position(topicPart)});
			}
			resetOffset(partitions);
		}

		protected void resetOffset(Collection<TopicPartition> partitions) {
			if (!partitions.isEmpty() && resetTo != OffsetResetStrategy.NONE) {
				log.info("seeking partitions offsets to {}", resetTo);
				if(resetTo == OffsetResetStrategy.EARLIEST) {
					consumer.seekToBeginning(partitions);							
				}
				if (resetTo == OffsetResetStrategy.LATEST) {
					consumer.seekToEnd(partitions);
				}
				for(TopicPartition topicPart : partitions) {
					log.info("after seek, committed offset for {}-{} is {}", new Object[]{topicPart.topic(), topicPart.partition(), consumer.position(topicPart)});
				}
				consumer.commitAsync();
			}
		}
		
		protected void printPartitions(Collection<TopicPartition> partitions, StringBuffer msg) {
			if(!partitions.isEmpty()) {
				for(TopicPartition topicparts : partitions) {
					msg.append(topicparts.topic()).append("-").append(topicparts.partition()).append(",");
				}
				log.info(msg.toString());	
			}
		}
	}
}
