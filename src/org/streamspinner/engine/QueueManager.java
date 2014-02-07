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
import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import org.streamspinner.Recoverable;
import org.streamspinner.InternalState;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.AttributeList;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.MasterSet;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

public class QueueManager implements Recoverable {

	private InformationSourceManager ism;
	private HashMap<String,HashMap<MasterSet,HashMap<ORNode,Queue>>> repository;

	public QueueManager(InformationSourceManager ism){
		this.ism = ism;
		repository = new HashMap<String,HashMap<MasterSet,HashMap<ORNode,Queue>>>();
	}

	public void assignQueues(ExecutionPlan plan) throws StreamSpinnerException {
		OperatorGroup[] op = plan.getOperators();

		for(int i=0; op != null && i < op.length; i++){
			MasterSet m = op[i].getMasterSet();
			SourceSet s = op[i].getSourceSet();

			ORNode[] inputs = op[i].getInputORNodes();
			for(int j=0; inputs != null && j < inputs.length; j++){
				if(inputs[j].isBase()){
					MasterSet wildcard = new MasterSet();
					Queue q = getQueue(plan, wildcard, inputs[j]);
					if(q == null)
						q = createNewQueue(plan, wildcard, inputs[j]);
					registQueue(plan, m, inputs[j], q);
				}
				Queue q = getQueue(plan, m, inputs[j]);
				if(q == null)
					q = createNewQueue(plan, m, inputs[j]);
				if(! q.hasEntry(op[i]))
					q.createEntry(op[i], s);
			}

			for(ANDNode an : op[i].getANDNodes()){
				ORNode output = an.getOutputORNode();
				if(output != null){
					Queue q = getQueue(plan, an.getMasterSet(), output);
					if(q == null)
						q = createNewQueue(plan, an.getMasterSet(), output);
					if(q instanceof PushBasedQueue)
						op[i].setExecutable(true);
					else
						op[i].setExecutable(false);
				}
			}
		}
	}

	public void deleteQueues(ExecutionPlan plan) throws StreamSpinnerException {
		OperatorGroup[] op = plan.getOperators();
		for(int i=0; op != null && i < op.length; i++){
			MasterSet m = op[i].getMasterSet();

			ORNode[] inputs = op[i].getInputORNodes();
			for(int j=0; inputs != null && j < inputs.length; j++){
				Queue q = getQueue(plan, m, inputs[j]);
				if(q != null){
					q.removeEntry(op[i]);
					if(q.getEntryCount() == 0)
						deleteQueue(plan, m, inputs[j]);
				}
			}
		}
	}


	public Queue createNewQueue(ExecutionPlan plan, MasterSet m, ORNode on) throws StreamSpinnerException {
		SchemaGenerator sg = new SchemaGenerator(ism, plan);
		Schema s = sg.getSchema(on);
		Queue rval = null;
		String type = s.getTableType();
		if(type.equals(Schema.RDB)){
			String tablename = (String)(on.getSources().iterator().next());
			InformationSource is = ism.getSourceFromTableName(tablename);
			rval = new PullBasedQueue(on, s, is);
		}
		else if(type.equals(Schema.HISTORY)){
			String tablename = (String)(on.getSources().iterator().next());
			InformationSource is = ism.getSourceFromTableName(tablename);
			rval = new HybridQueue(on, s, is);
		}
		else 
			rval = new PushBasedQueue(s);
		registQueue(plan, m, on, rval);
		return rval;
	}

	public Queue getQueue(ExecutionPlan plan, MasterSet m, ORNode on){
		if(! repository.containsKey(plan.getPlanID()))
			return null;
		HashMap<MasterSet,HashMap<ORNode,Queue>> mastertable = repository.get(plan.getPlanID());
		if(! mastertable.containsKey(m))
			return null;
		HashMap<ORNode,Queue> ontable = mastertable.get(m);
		if(! ontable.containsKey(on))
			return null;
		else
			return ontable.get(on);
	}

	public void registQueue(ExecutionPlan plan, MasterSet m, ORNode on, Queue q) {
		if(! repository.containsKey(plan.getPlanID()))
			repository.put(plan.getPlanID(), new HashMap<MasterSet,HashMap<ORNode,Queue>>());
		HashMap<MasterSet,HashMap<ORNode,Queue>> mastertable = repository.get(plan.getPlanID());
		if(! mastertable.containsKey(m))
			mastertable.put(m, new HashMap<ORNode,Queue>());
		HashMap<ORNode,Queue> ontable = mastertable.get(m);
		ontable.put(on, q);
	}


	public void deleteQueue(ExecutionPlan plan, MasterSet m, ORNode on) throws StreamSpinnerException {
		if(! repository.containsKey(plan.getPlanID()))
			return;
		HashMap<MasterSet,HashMap<ORNode,Queue>> mastertable = repository.get(plan.getPlanID());
		if(! mastertable.containsKey(m))
			return;
		HashMap<ORNode,Queue> ontable = mastertable.get(m);
		if(! ontable.containsKey(on))
			return;
		Queue q = ontable.get(on);
		q.collectGarbage(Long.MAX_VALUE);
		ontable.remove(on);
		if(ontable.size() == 0)
			mastertable.remove(m);
		if(mastertable.size() == 0)
			repository.remove(plan.getPlanID());
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof QueueManagerInternalState;
	}

	public InternalState getInternalState(){
		QueueManagerInternalState rval = new QueueManagerInternalState();
		for(String planid : repository.keySet()){
			HashMap<MasterSet,HashMap<ORNode,Queue>> mastertable = repository.get(planid);
			for(MasterSet master : mastertable.keySet()){
				HashMap<ORNode,Queue> ontable = mastertable.get(master);
				for(ORNode on : ontable.keySet()){
					Queue q = ontable.get(on);
					QueueInternalState state = q.getInternalState();
					state.setExecutionPlanID(planid);
					state.setMasterSet(master);
					state.setORNode(on);
					rval.addQueueInternalState(state);
				}
			}
		}
		return rval;
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(! isAcceptable(state))
			throw new StreamSpinnerException("Unknown state object is given");

		QueueManagerInternalState qmis = (QueueManagerInternalState)state;
		for(QueueInternalState qs : qmis.getQueueInternalStates()){
			if(! repository.containsKey(qs.getExecutionPlanID()))
				continue;
			HashMap<MasterSet,HashMap<ORNode,Queue>> mastertable = repository.get(qs.getExecutionPlanID());
			if(! mastertable.containsKey(qs.getMasterSet()))
				continue;
			HashMap<ORNode,Queue> ontable = mastertable.get(qs.getMasterSet());
			if(! ontable.containsKey(qs.getORNode()))
				continue;
			Queue q = ontable.get(qs.getORNode());
			q.setInternalState(qs);
		}
	}

}


