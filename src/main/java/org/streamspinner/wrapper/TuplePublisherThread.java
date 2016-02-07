/*
 * Copyright 2005-2009 StreamSpinner Project
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.streamspinner.wrapper;

import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.engine.TupleSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TuplePublisherThread extends Thread {

	private static class EventEntry {
		protected long executiontime;
		protected String source;
		protected TupleSet ts;
	}

	private ArrivalTupleListener atl = null;
	private LinkedBlockingQueue<EventEntry> queue = null;
	private boolean running = false;

	public TuplePublisherThread(ArrivalTupleListener atl){
		this.atl = atl;
		this.queue = new LinkedBlockingQueue<EventEntry>();
	}

	public void startQueueMonitoring(){
		if(running)
			return;
		running = true;
		start();
	}

	public void stopQueueMonitoring(){
		if(! running)
			return;
		running = false;
	}

	public void addTupleSet(long executiontime, String source, TupleSet ts){
		EventEntry ee = new EventEntry();
		ee.executiontime = executiontime;
		ee.source = source;
		ee.ts = ts;
		queue.add(ee);
	}

	public void run(){
		EventEntry ee = null;
		while(running){
			try {
				ee = queue.poll(100, TimeUnit.MILLISECONDS);
			} catch(InterruptedException ie){
				ie.printStackTrace();
			}
			if(ee != null && running){
				try {
					atl.receiveTupleSet(ee.executiontime, ee.source, ee.ts);
				} catch(StreamSpinnerException sse){
					sse.printStackTrace();
				}
			}
		}
	}

}
