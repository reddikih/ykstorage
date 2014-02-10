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
package org.streamspinner.optimizer;

import org.streamspinner.LogManager;
import org.streamspinner.LogEntry;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.SourceSet;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.Tuple;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Iterator;


public class LogManagerImpl implements LogManager {

	private Hashtable ratetable;
	private Hashtable lastexectable;

	public LogManagerImpl(){
		ratetable = null;
		lastexectable = null;
	}

	public void clean() throws StreamSpinnerException {
		init();
	}

	public int estimateCacheHitRate(OperatorGroup op) throws StreamSpinnerException {
		if(! ratetable.containsKey(op))
			return 0;
		CacheHitRate count = (CacheHitRate)(ratetable.get(op));
		return count.getAverageHitRate();
	}

	public void init() throws StreamSpinnerException {
		ratetable = new Hashtable();
		lastexectable = new Hashtable();
	}

	public void receiveLog(LogEntry le) throws StreamSpinnerException {
		OperatorGroup op = le.getOperatorGroup();
		long exectime = le.getExecutionTime();
		ORNode out = op.getANDNodes()[0].getOutputORNode();

		if(! ratetable.containsKey(op))
			ratetable.put(op, new CacheHitRate());

		if(! lastexectable.containsKey(out)){
			lastexectable.put(out, le);
			return;
		}

		LogEntry lastexec = (LogEntry)(lastexectable.get(out));
		if(lastexec.getOperatorGroup().equals(op)){
			LogEntry merged = new LogEntry(exectime, op, lastexec.getCounterValue() + le.getCounterValue());
			lastexectable.put(out, merged);
		}
		else {
			lastexectable.put(out, le);
			CacheHitRate count = (CacheHitRate)(ratetable.get(op));
			count.add(lastexec.estimateCacheHitRate(exectime, op));
		}
	}


	public void start() throws StreamSpinnerException {
		;
	}

	public void stop() throws StreamSpinnerException {
		;
	}

	private class CacheHitRate {
		private int count;
		private int average;

		public CacheHitRate(){
			clear();
		}

		public void clear(){
			count = 0;
			average = 0;
		}

		public void add(int rate){
			average = (average * count + rate) / (count + 1);
			count++;
		}

		public int getAverageHitRate(){
			return average;
		}
	}

}
