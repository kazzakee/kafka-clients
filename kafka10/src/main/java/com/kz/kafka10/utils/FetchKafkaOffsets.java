package com.kz.kafka10.utils;

import java.util.Date;
import java.util.Properties;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.kafka.common.protocol.Errors;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetch the offsets from kafka corresponding to a given time.
 */
public class FetchKafkaOffsets {
	protected static final Logger log = LoggerFactory.getLogger(FetchKafkaOffsets.class);
	protected static final String[] BROKER_HOST_URLS;
	protected static String currentlyQueryingBrokerUrl = "";

	static {
		Properties hosts = PropsUtil.loadProps("consumer.properties");
		BROKER_HOST_URLS = hosts.getProperty("bootstrap.servers").split(",");
	}
	
	public static void main(String[] args) {
		String topic = "test-topic";
		try {
			String clientId = "TestOffset-"+topic;
			short partitionCount = 5;
			short prevHours = -1;
			for(String brokerUrl : BROKER_HOST_URLS){
				currentlyQueryingBrokerUrl = brokerUrl;
				SimpleConsumer consumer = new SimpleConsumer(brokerUrl.split(":")[0], Integer.parseInt(brokerUrl.split(":")[1]), 100000, 64 * 1024, clientId);
				for(int partition = 1; partition<partitionCount; partition++){
					for(int prevH=1; prevH > prevHours; prevH--)
						getAndPrintOffsets(topic, partition, DateTimeUtils.toWholeHour(new Date(), prevH).getTime(), clientId, consumer);
				}
				consumer.close();
			}
		} catch (Throwable t) {
			log.error("Failed",t);
		}
	}
	
	protected static void getAndPrintOffsets(String topic, int partition, long hourstamp, String clientId, SimpleConsumer consumer){
		long[] list = getOffsets(consumer, topic, partition, hourstamp, clientId, 10);
		if(list!=null && list.length>0) {
			log.info("recd {} offsets for [{}-{}] @{} around {}", list.length, topic,partition,(new DateTime(hourstamp)).toString(), DateTimeUtils.printableDate(new Date(hourstamp)));
			for(long offset : list) 
				log.info("got offset={}",offset);
		}
	}

	protected static long[] getOffsets(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientId, int offsetCount) {
		java.util.Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new java.util.HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(whichTime, offsetCount));
		OffsetResponse response = consumer.getOffsetsBefore(new OffsetRequest(requestInfo, (short) 0, clientId));
		
		if (response.hasError()) {
			short code = response.errorCode(topic, partition);
			log.error("Failed in offset for [{}-{}] @{} from Broker {}, Reason: {}-{}",topic,partition,(new DateTime(whichTime)).toString(), 
					currentlyQueryingBrokerUrl, code, Errors.forCode(code));
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets;
	}
}
