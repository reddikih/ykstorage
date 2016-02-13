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

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.QueryOptimizer;
import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.Mediator;
import org.streamspinner.LogManager;
import org.streamspinner.SystemManager;
import org.streamspinner.query.Query;
import org.streamspinner.query.DAG;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.DefaultExecutionPlan;
import java.util.Hashtable;

public class DummyQueryOptimizer implements QueryOptimizer {

	private Hashtable plantable;
	private LogManager log;
	private SystemManager sysm;
	private Mediator mediator;

	public DummyQueryOptimizer(){
		plantable = null;
	}

	public void init() throws StreamSpinnerException {
		plantable = new Hashtable();
	}

	public void add(Query q, DAG d, ArrivalTupleListener atl) throws StreamSpinnerException {
		ExecutionPlan ep = new DefaultExecutionPlan(d);
		plantable.put(q.getID(), ep);
		mediator.addArrivalTupleListener(q.getID(), atl);
		mediator.registExecutionPlan(ep);
	}

	public void remove(Query q, ArrivalTupleListener atl) throws StreamSpinnerException {
		if(! plantable.containsKey(q.getID()))
			return;
		ExecutionPlan ep = (ExecutionPlan)(plantable.get(q.getID()));
		mediator.deleteExecutionPlan(ep);
		mediator.removeArrivalTupleListener(q.getID(), atl);
	}

	public void setLogManager(LogManager lm){
		log = lm;
	}

	public void setCacheConsumerThreshold(double t){
		;
	}

	public void setSystemManager(SystemManager sm){
		sysm = sm;
	}

	public void setMediator(Mediator m) {
		mediator = m;
	}

	public void start() throws StreamSpinnerException {
		if(mediator == null)
			throw new StreamSpinnerException("mediator is null");
	}

	public void stop() throws StreamSpinnerException {
		;
	}

}
