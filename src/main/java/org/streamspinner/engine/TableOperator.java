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
import org.streamspinner.ArchiveManager;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.TableManipulationParameter;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.AttributeList;


public abstract class TableOperator extends Operator {

	public static LogEntry process(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm) throws StreamSpinnerException {
		throw new StreamSpinnerException("This operator needs ArchiveManager");
	}

	public static LogEntry process(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm, ArchiveManager am) throws StreamSpinnerException {
		ANDNode[] node = op.getANDNodes();

		Queue[] outputq = new Queue[node.length];
		for(int i=0; i < node.length; i++){
			ORNode outputn = node[i].getOutputORNode();
			outputq[i] = qm.getQueue(plan, node[i].getMasterSet(), outputn);
			if(outputq[i] == null)
				throw new StreamSpinnerException("no output queue for " + outputn.toString());
		}

		String status = "";
		try {
			if(am == null)
				throw new StreamSpinnerException("ArchiveManager does not exist");

			TableManipulationParameter tp = node[0].getTableManipulationParameter();
			String operation = tp.getOperationType();

			if(operation.equals(TableManipulationParameter.STORE)){
				ORNode inputn = node[0].getInputORNodes()[0];
				Queue inputq = qm.getQueue(plan, op.getMasterSet(), inputn);
				TupleSet ts = inputq.getDeltaTupleSet(op, executiontime);
				if(ts.first() == false)
					return new LogEntry(executiontime, op);
				am.insert(tp.getParameter(), ts);
				inputq.moveToWindow(op, executiontime);
				ts.close();
			}
			else if(operation.equals(TableManipulationParameter.CREATE)){
				Schema newschema = Schema.parse(tp.getParameter());
				am.createTable(newschema.getBaseTableNames()[0], newschema);
			}
			else if(operation.equals(TableManipulationParameter.DROP)){
				am.dropTable(tp.getParameter());
			}
			else
				throw new StreamSpinnerException("Unknown table operation: "+ operation);
			status = "OK";
		} catch(StreamSpinnerException sse){
			status = "Failed:" + sse.getMessage();
		}

		Schema outputschema = outputq[0].getSchema();

		Tuple t = new Tuple(outputschema.size());
		t.setString(0, status);
		for(String tablename : outputschema.getBaseTableNames())
			t.setTimestamp(tablename, executiontime);

		for(int i=0; i < node.length; i++)
			outputq[i].append(t);

		LogEntry le = new LogEntry(executiontime, op);
		le.add(t);
		return le;
	}

}

