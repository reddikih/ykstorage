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
package org.streamspinner;

import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.OperatorGroup;
import java.util.ArrayList;
import java.util.Iterator;

public class LogEntry {

	private long executiontime;
	private OperatorGroup op;
	private int tuplecounter;

	public LogEntry(long executiontime, OperatorGroup op){
		this.executiontime = executiontime;
		this.op = op;
		tuplecounter = 0;
	}

	public LogEntry(long executiontime, OperatorGroup op, TupleSet ts) throws StreamSpinnerException {
		this(executiontime, op);
		add(ts);
	}

	public LogEntry(long executiontime, OperatorGroup op, int count){
		this(executiontime, op);
		tuplecounter = count;
	}

	public void add(TupleSet ts) throws StreamSpinnerException {
		ts.beforeFirst();
		while(ts.next())
			tuplecounter++;
	}

	public void add(Tuple t){
		tuplecounter++;
	}

	public long getExecutionTime(){
		return executiontime;
	}

	public OperatorGroup getOperatorGroup(){
		return op;
	}

	public int getCounterValue(){
		return tuplecounter;
	}

	public double getWindowOverlap(long targettime, OperatorGroup target){
		SourceSet ss = op.getSourceSet();
		SourceSet targetss = target.getSourceSet();

		long total = 1;
		long overlap = 1;

		for(Iterator it = targetss.iterator(); it.hasNext(); ){
			String source = (String)(it.next());
			long window = ss.getWindowsize(source);
			long targetwindow = targetss.getWindowsize(source);

			if(window == Long.MAX_VALUE || targetwindow == Long.MAX_VALUE)
				continue;
			if(executiontime < targettime - targetwindow)
				return 0.0;
			if(executiontime - window > targettime - targetwindow)
				return 0.0;

			total *= window;
			overlap *= executiontime - (targettime - targetwindow);
		}
		return ((double)overlap) / ((double)total);
	}

	public int estimateCacheHitRate(long targettime, OperatorGroup target){
		double overlap = getWindowOverlap(targettime, target);
		return (int)(getCounterValue() * overlap);
	}
}

