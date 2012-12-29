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

import org.streamspinner.Mediator;
import org.streamspinner.InternalState;
import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import org.streamspinner.ArchiveManager;
import org.streamspinner.SystemManager;
import org.streamspinner.LogManager;
import org.streamspinner.LogEntry;
import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.ExceptionListener;
import org.streamspinner.Operators;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.MasterSet;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Collections;
import java.util.Set;
import java.util.Iterator;

public class MediatorImpl implements Mediator {

	private ArchiveManager archivem;
	private LogManager logm;
	private SystemManager sysm;
	private InformationSourceManager sourcem;
	private QueueManager qm;
	private CacheManager cm;
	private Distributor dist;
	private ExceptionNotifier exception;
	private Set<ExecutionPlan> plans;

	public MediatorImpl(InformationSourceManager ism){
		dist = new Distributor();
		exception = new ExceptionNotifier();
		plans = Collections.synchronizedSet(new HashSet<ExecutionPlan>());
		archivem = null;
		logm = null;
		sysm = null;
		sourcem = ism;
		qm = new QueueManager(sourcem);
		cm = new CacheManager();

		sourcem.setMediator(this);
	}

	public void setArchiveManager(ArchiveManager am){
		archivem = am;
	}

	public void setLogManager(LogManager lm){
		logm = lm;
	}

	public void setSystemManager(SystemManager sm){
		sysm = sm;
		dist.setSystemManager(sm);
	}

	public void setInformationSourceManager(InformationSourceManager ism){
		sourcem = ism;
		qm = new QueueManager(sourcem);
	}

	public void addArrivalTupleListener(Object qid, ArrivalTupleListener listener){
		dist.addArrivalTupleListener(qid, listener);
	}

	public void removeArrivalTupleListener(Object qid, ArrivalTupleListener listener){
		dist.removeArrivalTupleListener(qid, listener);
	}

	public void addExceptionListener(Object qid, ExceptionListener listener){
		exception.addExceptionListener(qid, listener);
	}

	public void removeExceptionListener(Object qid, ExceptionListener listener){
		exception.removeExceptionListener(qid, listener);
	}

	public void registExecutionPlan(ExecutionPlan plan) throws StreamSpinnerException {
		synchronized(plans){
			plans.add(plan);
			qm.assignQueues(plan);
			cm.assignCacheArea(plan);
		}

		if(plan.getTriggers() == null || plan.getTriggers().length == 0)
			execute(System.currentTimeMillis(), null, plan);
	}

	public void deleteExecutionPlan(ExecutionPlan plan) throws StreamSpinnerException {
		if(! plans.contains(plan))
			return;
		synchronized(plans){
			plans.remove(plan);
			qm.deleteQueues(plan);
			cm.deleteCacheArea(plan);
		}
	}

	public void updateExecutionPlan(ExecutionPlan plan) throws StreamSpinnerException {
		registExecutionPlan(plan);
	}

	public void receiveTupleSet(long executiontime, String source, TupleSet ts) throws StreamSpinnerException {
		long totalexecution = 0;
		synchronized(plans){
			for(ExecutionPlan plan : plans){
				try {
					insert(executiontime, source, plan, ts);
					long executionbegin = System.currentTimeMillis();
					execute(executiontime, source, plan);
					long executionend = System.currentTimeMillis();
					totalexecution += executionend - executionbegin;
					afterExec(executiontime, source, plan);
				} catch(StreamSpinnerException sse){
					exception.notifyException(executiontime, plan.getQueryIDSet(), sse);
					throw sse;
				}
			}
		}
		if(sysm != null){
			sysm.dataReceived(executiontime, source, ts);
			sysm.executionPerformed(executiontime, source, totalexecution, System.currentTimeMillis() - executiontime);
		}
	}

	public void sync() throws StreamSpinnerException {
		;
	}

	public void start() throws StreamSpinnerException {
		if(sourcem == null || qm == null)
			throw new StreamSpinnerException("Some components are not given");
	}

	public void stop() throws StreamSpinnerException {
		archivem = null;
		logm = null;
		sysm = null;
		sourcem.setMediator(null);
		sourcem = null;
		qm = null;
		cm = null;
		dist = null;
		exception = null;
	}


	private void insert(long executiontime, String source, ExecutionPlan plan, TupleSet ts) throws StreamSpinnerException {
		OperatorGroup[] op = plan.getOperators();
		boolean found = false;
		for(int i=0; i < op.length; i++){
			ORNode[] inputs = op[i].getInputORNodes();
			for(int j=0; j < inputs.length; j++){
				SourceSet ss = inputs[j].getSources();
				if(inputs[j].isBase() && ss.contains(source)){
					Queue q = qm.getQueue(plan, op[i].getMasterSet(), inputs[j]);
					ts.beforeFirst();
					q.append(ts);
					found = true;
					break;
				}
			}
			if(found)
				break;
		}
	}


	private void execute(long executiontime, String source, ExecutionPlan plan) throws StreamSpinnerException {
		OperatorGroup[] op = null;
		if(source != null)
			op = plan.getOperatorsOnMaster(source);
		else
			op = plan.getOperators();

		if(op == null || op.length <= 0)
			return;
		for(int i=0; i < op.length; i++){
			if(op[i].isExecutable() == false)
				continue;

			String type = op[i].getType();
			LogEntry le = null;

			if(type.equals(Operators.ROOT))
				dist.deliver(executiontime, plan, op[i], qm, archivem);
			else if(type.equals(Operators.SELECTION))
				le = SelectionOperator.process(executiontime, plan, op[i], qm, cm);
			else if(type.equals(Operators.JOIN))
				le = NestedLoopJoinOperator.process(executiontime, plan, op[i], qm, cm);
			else if(type.equals(Operators.PROJECTION))
				le = ProjectionOperator.process(executiontime, plan, op[i], qm, cm);
			else if(type.equals(Operators.EVAL))
				le = EvalOperator.process(executiontime, plan, op[i], qm, cm);
			else if(type.equals(Operators.GROUP))
				le = GroupOperator.process(executiontime, plan, op[i], qm, cm);
			else if(type.equals(Operators.RENAME))
				le = RenameOperator.process(executiontime, plan, op[i], qm, cm);
			else if(type.equals(Operators.STORE) || type.equals(Operators.CREATE) || type.equals(Operators.DROP))
				le = TableOperator.process(executiontime, plan, op[i], qm, cm, archivem);
			else
				throw new StreamSpinnerException("Unknown operator: " + type);

			if(le != null && logm != null)
				logm.receiveLog(le);

			op[i].setLastExecutionTime(executiontime);
		}
	}

	private void afterExec(long executiontime, String source, ExecutionPlan plan) throws StreamSpinnerException {
		OperatorGroup[] op = plan.getOperatorsOnMaster(source);
		if(op == null || op.length <= 0)
			return;
		for(int i=0; i < op.length; i++){
			ORNode[] on = op[i].getInputORNodes();
			for(int j=0; j < on.length; j++){
				Queue q = qm.getQueue(plan, op[i].getMasterSet(), on[j]);
				if(q != null)
					q.collectGarbage(executiontime);
			}
			Cache c = cm.getCache(plan, op[i]);
			if(c != null)
				c.collectGarbage(executiontime);
		}
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof MediatorInternalState;
	}

	public InternalState getInternalState(){
		synchronized(plans){
			MediatorInternalState rval = new MediatorInternalState();
			rval.setQueueManagerInternalState(qm.getInternalState());
			return rval;
		}
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(isAcceptable(state)){
			synchronized(plans){
				MediatorInternalState mstate = (MediatorInternalState)state;
				qm.setInternalState(mstate.getQueueManagerInternalState());
			}
		}
		else
			throw new StreamSpinnerException("Unknown state object is given");
	}

}
