package com.kz.kafka10.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
 * This class demonstrates how we can fetch the offsets from kafka corresponding to a given time.
 */
public class FetchKafkaOffsets {
	protected static final Logger log = LoggerFactory.getLogger(FetchKafkaOffsets.class);
	protected static final String[] BROKER_HOST_URLS;
	protected static String currentlyQueryingBrokerUrl = "";
	protected static String CLIENT_ID = "TestOffset";

	static {
		Properties props = PropsUtil.loadProps("consumer_override.properties");
		BROKER_HOST_URLS = props.getProperty("bootstrap.servers").split(",");
	}
	
	public static void main(String[] args) {
		String topic = "cloudphotosws";
		try {
			String clientId = CLIENT_ID;
			short partitionCount = 6;
			short prevHours = -5;
			for(String brokerUrl : BROKER_HOST_URLS){
				currentlyQueryingBrokerUrl = brokerUrl;
				log.debug("* Fetch from broker={}",currentlyQueryingBrokerUrl);
				SimpleConsumer consumer = new SimpleConsumer(brokerUrl.split(":")[0], Integer.parseInt(brokerUrl.split(":")[1]), 100000, 64 * 1024, clientId);
				for(int partition = 0; partition<partitionCount; partition++){
					for(int prevH=1; prevH > prevHours; prevH--) {
						log.debug("== querying around time {}", DateTimeUtils.toWholeHour(new Date(), prevH));
						getAndPrintOffsets(topic, partition, DateTimeUtils.toWholeHour(new Date(), prevH).getTime(), clientId, consumer);
					}
				}
				consumer.close();
			}
		} catch (Throwable t) {
			log.error("Failed",t);
		}
	}
	
	protected static void getAndPrintOffsets(String topic, int partition, long hourstamp, String clientId, SimpleConsumer consumer){
		List<Long> list = getOffsets(consumer, topic, partition, hourstamp, clientId, 5);
		if(list!=null && list.size()>0) {
			log.info("=== successfully got offsets {} for [{}-{}] around {}", list, topic,partition, DateTimeUtils.printableDate(new Date(hourstamp)));
		}
	}

	protected static List<Long> getOffsets(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientId, int offsetCount) {
		List<Long> list = new ArrayList<Long>();
		java.util.Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new java.util.HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		long bufferTime = 2*60*1000; //2 mins
		requestInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(whichTime+bufferTime , offsetCount));
		OffsetResponse response = consumer.getOffsetsBefore(new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId));
		
		if (response.hasError()) {
			short code = response.errorCode(topic, partition);
			log.debug("*** failed to get offset for [{}-{}] @{} from Broker {}, Reason: {}-{}",topic,partition,(new DateTime(whichTime)).toString(), 
					currentlyQueryingBrokerUrl, code, Errors.forCode(code));
		}
		long[] offsets = response.offsets(topic, partition);
		for(long offset : offsets) 
			list.add(offset);
		return list;
	}
}
