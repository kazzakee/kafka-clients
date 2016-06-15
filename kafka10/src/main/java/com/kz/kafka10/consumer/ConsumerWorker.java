package com.kz.kafka10.consumer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract provides basic worker polling and consuming from given topics.
 * Processing of consumed records is left unimplemented for sub-classes 
 *
 */
public abstract class ConsumerWorker implements Runnable {
	protected static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);
	protected static final long POLL_TIME = 200; // ms
	protected static AtomicLong consumedCount = new AtomicLong(0);

	protected final KafkaConsumer<?, ?> consumer;
	protected final List<String> topics;
	protected final CountDownLatch shutdownLatch;
	protected boolean running = true;

	public ConsumerWorker(KafkaConsumer<?, ?> consumer, List<String> topics) {
		this.consumer = consumer;
		this.topics = topics;
		this.shutdownLatch = new CountDownLatch(1);
	}

	protected abstract void process(ConsumerRecords<?, ?> records);

	public void run() {
		try {
			consumer.subscribe(topics);
			while (running) {
				ConsumerRecords<?, ?> records = consumer.poll(POLL_TIME);
				process(records);
				consumedCount.incrementAndGet();
				try {
					consumer.commitSync();
				} catch (CommitFailedException e) {
					// application-specific rollback of processed records
					log.error("CommitSync failed", e);
				}
			}
		} catch (WakeupException e) {
			// ignore, we're closing
		} catch (Exception e) {
			log.error("Unexpected error", e);
		} finally {
			if (consumer != null)
				consumer.close();
			shutdownLatch.countDown();
		}
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