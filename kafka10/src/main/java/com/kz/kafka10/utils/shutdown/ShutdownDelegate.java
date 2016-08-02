package com.kz.kafka10.utils.shutdown;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

public class ShutdownDelegate {
	private Shutdownable app = null;
	private AtomicBoolean interrupted = new AtomicBoolean(false);
	private int maxWaitTime = 10000;
	private Logger log = null;

	public ShutdownDelegate(Shutdownable app) {
		this.app = app;
	}
	
	public ShutdownDelegate(Shutdownable app, Logger log) {
		this(app);
		this.log = log;		
	}
	
	public ShutdownDelegate(Shutdownable app, int maxwaittime, Logger log) {
		this(app, log);
		this.maxWaitTime  = maxwaittime;
	}
		
	public void waitAndShutdown() {
		this.waitAndShutdown(this.maxWaitTime, this.log);
	}

	public void waitAndShutdown(long maxWaitTime) {
		this.waitAndShutdown(maxWaitTime, this.log);
	}

	protected void checkInterrupt() {
		if(app!=null && app.getLatch()!=null && app.getLatch().getCount() < 1)
			this.interrupted.set(true);
	}

	private void waitAndShutdown(long maxWaitTime, Logger log) {
		// let it wait for given time or until interrupted
		int waitTime = 0;
		try {
			while(maxWaitTime>waitTime) {
				checkInterrupt();
				if(!interrupted.get())
					Thread.sleep(1000);
				else
					throw new InterruptedException("proceed to shutdown");
				waitTime++;
			}
		} catch (InterruptedException ie) {
			if(log!=null)
				log.info(ie.getMessage());
			else
				System.err.println(ie.getMessage());
		} finally {
			app.shutdown();
		}
	}
}
