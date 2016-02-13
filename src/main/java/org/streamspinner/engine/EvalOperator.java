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
import org.streamspinner.query.FunctionParameter;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.AttributeList;


public abstract class EvalOperator extends Operator {

	public static LogEntry process(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm) throws StreamSpinnerException {
		ANDNode[] node = op.getANDNodes();

		ORNode inputn = node[0].getInputORNodes()[0];
		Queue inputq = qm.getQueue(plan, op.getMasterSet(), inputn);

		Queue[] outputq = new Queue[node.length];
		for(int i=0; i < node.length; i++){
			ORNode outputn = node[i].getOutputORNode();
			outputq[i] = qm.getQueue(plan, node[i].getMasterSet(), outputn);
			if(outputq[i] == null)
				throw new StreamSpinnerException("no input queue for " + outputn.toString());
		}

		TupleSet ts = getDeltaTupleSet(executiontime, plan, op, inputn, inputq, cm);

		LogEntry le = null;
		if(op.isCacheConsumer() == true)
			le = consumeCacheData(executiontime, plan, op, qm, cm);
		else
			le = new LogEntry(executiontime, op);

		if(ts.first() == false)
			return le;

		FunctionParameter fp = node[0].getFunctionParameter();
		Schema origschema = ts.getSchema();

		int[] indexes = new int[fp.getArguments().size()];
		for(int i=0; i < indexes.length; i++)
			indexes[i] = origschema.getIndex(fp.getArguments().getString(i));

		Function f = Function.getInstance(fp, origschema);
		Schema newschema = origschema.append(fp.toString(), f.getReturnType());

		OnMemoryTupleSet result = new OnMemoryTupleSet(newschema);
		do {
			Tuple t = ts.getTuple();
			Object[] values = new Object[indexes.length];
			for(int i=0; i < values.length; i++)
				values[i] = t.getObject(indexes[i]);
			Object returnvalue = f.invoke(values);
			Tuple newtuple = t.appendColumn();
			newtuple.setObject(newtuple.size() -1, returnvalue);
			for(int i=0; i < node.length; i++)
				outputq[i].append(newtuple);
			result.append(newtuple);
			le.add(newtuple);
		} while(ts.next());

		ts.close();
		produceCacheData(executiontime, plan, op, cm, result);
		inputq.moveToWindow(op, executiontime);
		return le;
	}

}

