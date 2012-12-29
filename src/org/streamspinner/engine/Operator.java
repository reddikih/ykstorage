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
package org.streamspinner.engine;

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.Operators;
import org.streamspinner.DataTypes;
import org.streamspinner.LogEntry;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.Predicate;
import org.streamspinner.query.ORNode;
import java.util.*;

public abstract class Operator {

	public static LogEntry process(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm) throws StreamSpinnerException {
		return null;
	}

	public static Comparable getComparableObject(Tuple t, String type, int index) throws StreamSpinnerException {
		Object obj = t.getObject(index);
		if(obj instanceof Comparable)
			return (Comparable)obj;
		else
			throw new StreamSpinnerException("" + type + " is not comparable type");
	}

	public static LogEntry consumeCacheData(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm) throws StreamSpinnerException {
		LogEntry rval = new LogEntry(executiontime, op);

		if(op.getType().equals(Operators.ROOT) || op.size() == 0 || op.isCacheConsumer() == false)
			return rval;


		ORNode outputn = op.getANDNodes()[0].getOutputORNode();
		Queue outputq = qm.getQueue(plan, op.getMasterSet(), outputn);

		Cache c = cm.getCache(plan, op);
		if(c == null || (! c.isReusable(executiontime, op)) )
			return rval;
		Queue[] inputq = new Queue[op.getInputORNodes().length];
		for(int i=0; i < inputq.length; i++){
			inputq[i] = qm.getQueue(plan, op.getMasterSet(), op.getInputORNodes()[i]);
			if(! inputq[i].hasEntry(c.getCreator()))
				return rval;
		}

		TupleSet cachedata = c.getTupleSet(executiontime, op);
		cachedata.beforeFirst();
		while(cachedata.next()){
			Tuple t = cachedata.getTuple();
			outputq.append(t);
			rval.add(t);
		}
		return rval;
	}

	public static void produceCacheData(long executiontime, ExecutionPlan plan, OperatorGroup op, CacheManager cm, TupleSet result) throws StreamSpinnerException {
		op.setLastExecutionTime(executiontime);
		if(op.getType().equals(Operators.ROOT) || op.size() == 0 || op.isCacheProducer() == false)
			return;
		Cache c = cm.getCache(plan, op);
		if(c == null)
			return;
		c.updateCache(executiontime, op, result);
	}

	public static TupleSet getDeltaTupleSet(long executiontime, ExecutionPlan plan, OperatorGroup op, ORNode node, Queue q, CacheManager cm) throws StreamSpinnerException {
		if(op.isCacheConsumer() == false)
			return q.getDeltaTupleSet(op, executiontime);

		Cache c = cm.getCache(plan, op);
		OperatorGroup creator = c.getCreator();
		if(c == null || (! q.hasEntry(creator)) || (! c.isReusable(executiontime, op)) )
			return q.getDeltaTupleSet(op, executiontime);

		return q.getDeltaTupleSet(creator, op, executiontime);
	}

	public static TupleSet getWindowTupleSet(long executiontime, ExecutionPlan plan, OperatorGroup op, ORNode node, Queue q, CacheManager cm) throws StreamSpinnerException {
		if(op.isCacheConsumer() == false)
			return q.getWindowTupleSet(op, executiontime);

		Cache c = cm.getCache(plan, op);
		OperatorGroup creator = c.getCreator();
		if(c == null || (! q.hasEntry(creator)) || (! c.isReusable(executiontime, op)) )
			return q.getWindowTupleSet(op, executiontime);

		return q.getWindowTupleSet(creator, op, executiontime);
	}
}
