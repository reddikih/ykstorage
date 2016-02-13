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
import java.util.Iterator;

public abstract class SelectionOperator extends Operator {

	public static LogEntry process(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm) throws StreamSpinnerException {
		ANDNode[] node = op.getANDNodes();

		ORNode inputn = node[0].getInputORNodes()[0];
		Queue inputq = qm.getQueue(plan, op.getMasterSet(), inputn);

		ORNode[] outputn = new ORNode[node.length];
		Queue[] outputq = new Queue[node.length];
		for(int i=0; i < outputn.length; i++){
			outputn[i] = node[i].getOutputORNode();
			outputq[i] = qm.getQueue(plan, node[i].getMasterSet(), outputn[i]);
		}

		TupleSet ts = getDeltaTupleSet(executiontime, plan, op, inputn, inputq, cm);
		Schema s = ts.getSchema();

		LogEntry le = null;
		if(op.isCacheConsumer() == true)
			le = consumeCacheData(executiontime, plan, op, qm, cm);
		else
			le = new LogEntry(executiontime, op);

		OnMemoryTupleSet result = new OnMemoryTupleSet(s);
		while(ts.next()){
			Tuple t = ts.getTuple();
			for(int i=0; i < node.length; i++){
				PredicateSet conds = node[i].getPredicateSet();
				Iterator it = conds.iterator();
				boolean flag = true;
				while(it.hasNext()){
					Predicate cond = (Predicate)(it.next());
					flag &= evaluate(cond, t, s);
					if(flag == false)
						break;
				}
				if(flag == true){
					result.append(t);
					outputq[i].append(t);
					le.add(t);
				}
			}
		}
		ts.close();
		produceCacheData(executiontime, plan, op, cm, result);
		inputq.moveToWindow(op, executiontime);
		return le;
	}


	private static boolean evaluate(Predicate cond, Tuple t, Schema s) throws StreamSpinnerException {
		Predicate p = cond;
		if(cond.isConstant(Predicate.LEFT))
			cond = cond.reverse();

		int index = s.getIndex(p.getLeftString());
		String type = s.getType(index);

		if(type.equals(DataTypes.LONG)){
			long value = t.getLong(index);
			//long constant = (new Long(p.getRightString())).longValue();
			long constant = (new Double(p.getRightString())).longValue();
			return cond.evaluate(value, constant);
		}
		else if(type.equals(DataTypes.DOUBLE)){
			double value = t.getDouble(index);
			double constant = (new Double(p.getRightString())).doubleValue();
			return cond.evaluate(value, constant);
		}
		else if(type.equals(DataTypes.STRING)){
			String value = t.getString(index);
			String constant = p.getRightString();
			if(constant.startsWith("'") && constant.endsWith("'"))
				constant = constant.substring(1, constant.length() -1);
			return cond.evaluate(value, constant);
		}
		else if(DataTypes.isArray(type))
			throw new StreamSpinnerException("ARRAY is not comparable type");
		else if(type.equals(DataTypes.OBJECT))
			throw new StreamSpinnerException("OBJECT is not comparable type");
		else 
			throw new StreamSpinnerException("Unknown type: " + type);
	}

}

