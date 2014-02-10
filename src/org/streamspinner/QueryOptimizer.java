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
package org.streamspinner;

import org.streamspinner.query.Query;
import org.streamspinner.query.DAG;

public interface QueryOptimizer {

	public void init() throws StreamSpinnerException;
	public void add(Query q, DAG d, ArrivalTupleListener listener) throws StreamSpinnerException;
	public void remove(Query q, ArrivalTupleListener listener) throws StreamSpinnerException;
	public void setLogManager(LogManager lm);
	public void setCacheConsumerThreshold(double t);
	public void setSystemManager(SystemManager sm);
	public void setMediator(Mediator m);
	public void start() throws StreamSpinnerException;
	public void stop() throws StreamSpinnerException;

}
