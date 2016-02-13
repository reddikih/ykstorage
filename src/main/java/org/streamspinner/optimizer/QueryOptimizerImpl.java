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

import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.QueryOptimizer;
import org.streamspinner.LogManager;
import org.streamspinner.SystemManager;
import org.streamspinner.Mediator;
import org.streamspinner.Operators;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.Query;
import org.streamspinner.query.MasterSet;
import org.streamspinner.query.DAG;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.OperatorGroup;
import java.util.Hashtable;

public class QueryOptimizerImpl implements QueryOptimizer{

	public static final String MODE_GROUP_BY_TRIGGER = "groupbytrigger";
	public static final String MODE_DYNAMIC = "dynamic";
	public static final String MODE_ALL_IN_ONE = "allinone";

	private Hashtable<MasterSet,OptimizedExecutionPlan> plantable;
	private LogManager log;
	private SystemManager sysm;
	private Mediator mediator;
	private String mode;
	private double cacheconsumerthreshold;

	public QueryOptimizerImpl(String mode) throws StreamSpinnerException {
		plantable = null;
		log = null;
		sysm = null;
		mediator = null;
		this.mode = mode;
		if(mode != null && 
				(! mode.equalsIgnoreCase(MODE_GROUP_BY_TRIGGER)) &&
				(! mode.equalsIgnoreCase(MODE_DYNAMIC)) &&
				(! mode.equalsIgnoreCase(MODE_ALL_IN_ONE)) )
			throw new StreamSpinnerException("unknown mode: " + mode);

		cacheconsumerthreshold = 0.0;
	}

	public void init() throws StreamSpinnerException {
		plantable = new Hashtable<MasterSet,OptimizedExecutionPlan>();
	}

	private OptimizedExecutionPlan getPlan(Query q) throws StreamSpinnerException {
		MasterSet key;
		if(mode.equalsIgnoreCase(MODE_GROUP_BY_TRIGGER))
			key = q.getMasterClause(); // one plan per one masterset
		else
			key = new MasterSet();  // only one plan for all mastersets

		OptimizedExecutionPlan plan;
		if(! plantable.containsKey(key)){
			plan = new OptimizedExecutionPlan();
			if(mode.equalsIgnoreCase(MODE_ALL_IN_ONE))
				plan.enableIgnoringMaster(true);
			else
				plan.enableIgnoringMaster(false);
			plantable.put(key, plan);
			mediator.registExecutionPlan(plan);
		}
		else
			plan = plantable.get(key);
		return plan;
	}


	public void add(Query q, DAG d, ArrivalTupleListener atl) throws StreamSpinnerException{
		synchronized(plantable){
			OptimizedExecutionPlan plan = getPlan(q);

			if(plan != null && mediator != null){
				plan.addQuery(q, d);
				OperatorGroup[] op = plan.getOperators();
				for(int i=0; i < op.length; i++){
					if(mode.equalsIgnoreCase(MODE_DYNAMIC)){
						op[i].setCacheProducer(true);
						op[i].setCacheConsumer(true);
					}
					else {
						op[i].setCacheProducer(false);
						op[i].setCacheConsumer(false);
					}
				}
				mediator.updateExecutionPlan(plan);
			}
		}
		mediator.addArrivalTupleListener(q.getID(), atl);
	}

	public void remove(Query q, ArrivalTupleListener atl) throws StreamSpinnerException {
		synchronized(plantable){
			OptimizedExecutionPlan plan = getPlan(q);
			if(plan != null && mediator != null){
				mediator.removeArrivalTupleListener(q.getID(), atl);
				plan.deleteQuery(q);
				if(plan.getQueryIDSet().size() == 0)
					mediator.deleteExecutionPlan(plan);
				else
					mediator.updateExecutionPlan(plan);
			}
		}
	}

	public void setLogManager(LogManager lm){
		log = lm;
	}

	public void setCacheConsumerThreshold(double t){
		cacheconsumerthreshold = t;
	}

	public void setSystemManager(SystemManager sm){
		sysm = sm;
	}

	public void setMediator(Mediator m){
		mediator = m;
	}

	public void start() throws StreamSpinnerException {
		if(mediator == null)
			throw new StreamSpinnerException("mediator is null");
		if(log == null)
			throw new StreamSpinnerException("log is null");
	}

	public void stop() throws StreamSpinnerException {
		if(mediator != null){
			for(OptimizedExecutionPlan plan : plantable.values())
				mediator.deleteExecutionPlan(plantable.get(plan));
		}
	}

}
