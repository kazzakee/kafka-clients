package com.kz.kafka10.consumer;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupRebalanceListener implements ConsumerRebalanceListener {
	protected static final Logger log = LoggerFactory.getLogger(GroupRebalanceListener.class);

	private KafkaConsumer<?,?> consumer = null;
	private OffsetResetStrategy resetStrategy;

	private boolean forceOffsetReset = false;
	
	public GroupRebalanceListener(KafkaConsumer<?,?> consumer) {
		this.consumer = consumer;
	}
	
	public GroupRebalanceListener(KafkaConsumer<?,?> consumer, OffsetResetStrategy resetTo) {
		this(consumer);
		this.resetStrategy = resetTo;
	}
	
	public GroupRebalanceListener(KafkaConsumer<?,?> consumer, OffsetResetStrategy resetTo, boolean forceOffsetReset) {
		this(consumer, resetTo);
		this.forceOffsetReset = forceOffsetReset;
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
		if(forceOffsetReset)
			resetOffset(partitions);
	}

	private void resetOffset(Collection<TopicPartition> partitions) {
		if (!partitions.isEmpty() && resetStrategy != OffsetResetStrategy.NONE) {
			log.info("force seeking partitions offsets to {}", resetStrategy);
			if(resetStrategy == OffsetResetStrategy.EARLIEST) {
				consumer.seekToBeginning(partitions);							
			}
			if (resetStrategy == OffsetResetStrategy.LATEST) {
				consumer.seekToEnd(partitions);
			}
			for(TopicPartition topicPart : partitions) {
				log.info("after force seek, offset for {}-{} is {}", new Object[]{topicPart.topic(), topicPart.partition(), consumer.position(topicPart)});
			}
			consumer.commitAsync();
		}
	}
	
	private void printPartitions(Collection<TopicPartition> partitions, StringBuffer msg) {
		if(!partitions.isEmpty()) {
			for(TopicPartition topicparts : partitions) {
				msg.append(topicparts.topic()).append("-").append(topicparts.partition()).append(",");
			}
			log.info(msg.toString());	
		}
	}
}