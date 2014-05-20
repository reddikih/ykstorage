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

import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.engine.TupleSet;


public interface Mediator extends ArrivalTupleListener, Recoverable {

	public void setArchiveManager(ArchiveManager am);
	public void setLogManager(LogManager lm);
	public void setSystemManager(SystemManager sm);
	public void setInformationSourceManager(InformationSourceManager ism);
	public void addArrivalTupleListener(Object qid, ArrivalTupleListener listener);
	public void removeArrivalTupleListener(Object qid, ArrivalTupleListener listener);
	public void addExceptionListener(Object qid, ExceptionListener listener);
	public void removeExceptionListener(Object qid, ExceptionListener listener);
	public void registExecutionPlan(ExecutionPlan plan) throws StreamSpinnerException ;
	public void deleteExecutionPlan(ExecutionPlan plan) throws StreamSpinnerException ;
	public void updateExecutionPlan(ExecutionPlan plan) throws StreamSpinnerException ;
	public void receiveTupleSet(long executiontime, String source, TupleSet ts) throws StreamSpinnerException ;
	public void start() throws StreamSpinnerException ;
	public void stop() throws StreamSpinnerException ;
	public void sync() throws StreamSpinnerException ;

	public InternalState getInternalState() ;
	public void setInternalState(InternalState state) throws StreamSpinnerException ;

}
