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
import org.streamspinner.DataTypes;
import org.streamspinner.Operators;
import org.streamspinner.LogEntry;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.Predicate;
import org.streamspinner.query.PredicateSet;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.AttributeList;
import java.util.Iterator;


public abstract class NestedLoopJoinOperator extends Operator {

	public static void swap(Object[] array, int index1, int index2){
		Object tmp = array[index1];
		array[index1] = array[index2];
		array[index2] = tmp;
	}

	public static LogEntry process(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm) throws StreamSpinnerException {
		ANDNode[] node = op.getANDNodes();

		ORNode[] inputn = node[0].getInputORNodes();
		Queue[] inputq = new Queue[inputn.length];
		for(int i=0; i < inputn.length; i++)
			inputq[i] = qm.getQueue(plan, op.getMasterSet(), inputn[i]);

		ORNode[] outputn = new ORNode[node.length];
		Queue[] outputq = new Queue[node.length];
		for(int i=0; i < node.length; i++){
			outputn[i] = node[i].getOutputORNode();
			outputq[i] = qm.getQueue(plan, node[i].getMasterSet(), outputn[i]);
		}
		if(! inputn[0].getSources().contains(outputn[0].getSources().iterator().next())){
			swap(inputn, 0, 1);
			swap(inputq, 0, 1);
		}

		TupleSet delta1, delta2, window1, window2;
		window1 = getWindowTupleSet(executiontime, plan, op, inputn[0], inputq[0], cm);
		delta1 = getDeltaTupleSet(executiontime, plan, op, inputn[0], inputq[0], cm);
		window2 = getWindowTupleSet(executiontime, plan, op, inputn[1], inputq[1], cm);
		delta2 = getDeltaTupleSet(executiontime, plan, op, inputn[1], inputq[1], cm);

		LogEntry le = null;
		if(op.isCacheConsumer() == true)
			le = consumeCacheData(executiontime, plan, op, qm, cm);
		else
			le = new LogEntry(executiontime, op);

		Schema scm = null;
		if(delta1.getSchema() != null)
			scm = delta1.getSchema();
		else
			scm = window1.getSchema();
		if(delta2.getSchema() != null)
			scm = scm.concat(delta2.getSchema());
		else
			scm = scm.concat(window2.getSchema());

		OnMemoryTupleSet result = new OnMemoryTupleSet(scm);
		evaluateAll(delta1, delta2, node, outputq, result, le);
		evaluateAll(delta1, window2, node, outputq, result, le);
		evaluateAll(window1, delta2, node, outputq, result, le);

		delta1.close();
		window1.close();
		delta2.close();
		window2.close();

		produceCacheData(executiontime, plan, op, cm, result);

		for(int i=0; i < inputq.length; i++)
			inputq[i].moveToWindow(op, executiontime);

		return le;
	}

	private static void evaluateAll(TupleSet ts1, TupleSet ts2, ANDNode[] node, Queue[] outputq, OnMemoryTupleSet result, LogEntry le) throws StreamSpinnerException {
		if(ts1.first() == false)
			return;

		Schema s1 = ts1.getSchema();
		Schema s2 = ts2.getSchema();

		do {
			Tuple t1 = ts1.getTuple();
			if(ts2.first() == false)
				return;
			do {
				Tuple t2 = ts2.getTuple();
				for(int i=0; i < node.length; i++){
					PredicateSet conds = node[i].getPredicateSet();
					if(evaluatePredicateSet(conds, t1, t2, s1, s2) == true){
						Tuple newtuple = new Tuple(t1, t2);
						outputq[i].append(newtuple);
						result.append(newtuple);
						le.add(newtuple);
					}
				}
			} while(ts2.next());
		}while(ts1.next());
	}

	private static boolean evaluatePredicateSet(PredicateSet conds, Tuple t1, Tuple t2, Schema s1, Schema s2) throws StreamSpinnerException {
		Iterator it = conds.iterator();
		while(it.hasNext()){
			Predicate p = (Predicate)(it.next());
			if(evaluatePredicate(p, t1, t2, s1, s2) == false)
				return false;
		}
		return true;
	}

	private static boolean evaluatePredicate(Predicate cond, Tuple t1, Tuple t2, Schema s1, Schema s2) throws StreamSpinnerException {
		Predicate p = cond;

		if(! s1.contains(cond.getLeftString()) )
			p = cond.reverse();

		int index1 = s1.getIndex(p.getLeftString());
		int index2 = s2.getIndex(p.getRightString());
		String type1 = s1.getType(index1);
		String type2 = s2.getType(index2);

		Comparable v1 = Operator.getComparableObject(t1, type1, index1);
		Comparable v2 = Operator.getComparableObject(t2, type2, index2);

		return cond.evaluate(v1, v2);
	}

}
