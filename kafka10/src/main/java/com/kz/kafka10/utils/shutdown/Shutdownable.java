package com.kz.kafka10.utils.shutdown;

import java.util.concurrent.CountDownLatch;

public interface Shutdownable {
	void shutdown();
	CountDownLatch getLatch();
}
