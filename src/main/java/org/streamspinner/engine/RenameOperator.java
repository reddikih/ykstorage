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
import org.streamspinner.query.RenameParameter;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.AttributeList;


public abstract class RenameOperator extends Operator {

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

		RenameParameter rp = node[0].getRenameParameter();
		Schema outputschema = outputq[0].getSchema();
		OnMemoryTupleSet result = new OnMemoryTupleSet(outputschema);

		do {
			Tuple t = ts.getTuple();
			for(int i=0; i < node.length; i++)
				outputq[i].append(t);
			result.append(t);
			le.add(t);
		} while(ts.next());

		ts.close();
		produceCacheData(executiontime, plan, op, cm, result);
		inputq.moveToWindow(op, executiontime);
		return le;
	}

}

