package com.kz.kafka10.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
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
	protected static Map<String,SimpleConsumer> consumers = new HashMap<String,SimpleConsumer>();
	protected static long recordCount = 0;

	static {
		Properties props = PropsUtil.loadProps("consumer_override.properties");
		BROKER_HOST_URLS = props.getProperty("bootstrap.servers").split(",");
	}
	
	public static void main(String[] args) {
		if(args.length < 1) {
			System.err.println("Usage: java com.kz.kafka10.utils.FetchKafkaOffsets <topicname>");
			System.exit(0);
		}
		String topic = args[0];
		try {
			String clientId = CLIENT_ID;
			short partitionCount = 2;
			short prevDuration = -10;
			for(int partition = 0; partition<partitionCount; partition++){
				for(int prevD=1; prevD > prevDuration; prevD--) {
					for(String brokerUrl : BROKER_HOST_URLS){
						currentlyQueryingBrokerUrl = brokerUrl;
						log.debug("* Fetch from broker={}",currentlyQueryingBrokerUrl);
						SimpleConsumer consumer = getConsumerInstance(brokerUrl);
							log.debug("== querying around time {}", DateTimeUtils.toWholeMinute(new Date(), prevD));
							if(getAndPrintOffsets(topic, partition, DateTimeUtils.toWholeMinute(new Date(), prevD).getTime(), clientId, consumer))
								break;
					}
				}
			}
			log.info("total record count={}",recordCount);
		} catch (Throwable t) {
			log.error("Failed",t);
		} finally {
			for(String broker : consumers.keySet()){
				SimpleConsumer consumer = consumers.get(broker);
				consumer.close();
				log.info("closed consumer for {}",broker);
			}
		}
	}

	private static SimpleConsumer getConsumerInstance(String brokerUrl) {
		if(!consumers.containsKey(brokerUrl)) {
			SimpleConsumer consumer = new SimpleConsumer(brokerUrl.split(":")[0], Integer.parseInt(brokerUrl.split(":")[1]), 100000, 64 * 1024, CLIENT_ID);
			consumers.put(brokerUrl, consumer);
		} 
		return consumers.get(brokerUrl);
	}
	
	protected static boolean getAndPrintOffsets(String topic, int partition, long hourstamp, String clientId, SimpleConsumer consumer){
		List<Long> list = getOffsets(consumer, topic, partition, hourstamp, clientId, 2);
		if(list!=null && list.size()>0) {
			recordCount  += list.get(0).longValue();
			log.info("=== successfully got offsets {} for [{}-{}] @{}", list, topic,partition, DateTimeUtils.printableDate(new Date(hourstamp)));
			return true;
		} else {
			log.info("=== *FAILED* to get offset for [{}-{}] @{}",topic,partition,(new DateTime(hourstamp)).toString());
			return false;
		}
	}

	protected static List<Long> getOffsets(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientId, int offsetCount) {
		List<Long> list = new ArrayList<Long>();
		java.util.Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new java.util.HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		long bufferTime = 0*60*1000; //2 mins
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

	@SuppressWarnings("unused")
	private static void fetchOffsetsNew(SimpleConsumer consumer, String topic, int partition, String clientId, long whichTime, int offsetCount) {
		List<TopicAndPartition> ptlist = new ArrayList<>();
		//for(int i=0; i<3; i++)
		{
			ptlist.add(new TopicAndPartition(topic, partition));
		}
		OffsetFetchRequest ofreq = new OffsetFetchRequest("group-offset-test", ptlist, (new Random()).nextInt(), clientId);
		OffsetFetchResponse ofres = consumer.fetchOffsets(ofreq);
		Map<TopicAndPartition, OffsetMetadataAndError> omap = ofres.offsets();
		for(int i=0; i<ptlist.size(); i++) {
			if(omap.containsKey(ptlist.get(i))){
				OffsetMetadataAndError reo = omap.get(ptlist.get(i));
				log.info("partition={}, offset={}, error=[{}-{}]", ptlist.get(i).partition(), reo.offset(), reo.error(), ErrorMapping.exceptionNameFor(reo.error()));				
			}				
		}
	}
}
