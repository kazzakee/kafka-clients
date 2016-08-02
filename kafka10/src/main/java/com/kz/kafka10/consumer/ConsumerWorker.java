package com.kz.kafka10.consumer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class provides basic worker polling and consuming from given topics.
 * Processing of consumed records is left unimplemented for subclasses.
 *
 */
public abstract class ConsumerWorker implements Runnable {
	protected static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);
	protected static final long POLL_TIME = 200; // ms
	protected static AtomicLong consumedCount = new AtomicLong(0);

	protected KafkaConsumer<?, ?> consumer;
	protected List<String> topics;
	protected int maxRecords;
	protected String resetStrategy = "NONE";//default
	protected Boolean forceOffsetReset = false;//default
	protected boolean running = true;
	protected CountDownLatch shutdownLatch;

	public ConsumerWorker(KafkaConsumer<?, ?> consumer, List<String> topics, int maxRecords, CountDownLatch latch) {
		this.consumer = consumer;
		this.topics = topics;
		this.maxRecords = maxRecords;
		this.shutdownLatch = latch;
	}
	
	public ConsumerWorker(KafkaConsumer<?, ?> consumer, List<String> topics, int maxRecords, String resetStrategy, boolean forceOffsetReset, CountDownLatch latch) {
		this(consumer, topics, maxRecords, latch);
		this.resetStrategy = resetStrategy;
		this.forceOffsetReset = forceOffsetReset;
	}

	protected abstract void process(ConsumerRecords<?, ?> records);

	public void run() {
		try {
			consumer.subscribe(topics, new GroupRebalanceListener(consumer, OffsetResetStrategy.valueOf(resetStrategy), forceOffsetReset));
			while (running) {
				ConsumerRecords<?, ?> records = consumer.poll(POLL_TIME);
				process(records);
				consumedCount.addAndGet(records.count());
				try {
					consumer.commitSync();
				} catch (CommitFailedException e) {
					// application-specific rollback of processed records
					log.error("CommitSync failed", e);
				}
				if(consumedCount.get() >= maxRecords) {
					log.info("Stop polling as total consumed >= maxRecords ({})",maxRecords);
					break;
				}
			}
		} catch (WakeupException e) {
			// ignore, we're closing
		} catch (Exception e) {
			log.error("Unexpected error", e);
		} finally {
			if (consumer != null) {
				consumer.unsubscribe();
				consumer.close();
			}
			try {
				shutdownLatch.countDown();
			} catch (Exception e) {
				log.error("countDown failed", e);
			}
		}
		log.info("Task completed");
	}

	
	
	public void shutdown() throws InterruptedException {
		consumer.wakeup();
		shutdownLatch.await();
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public static Long getConsumedCount() {
		return consumedCount.get();
	}
}