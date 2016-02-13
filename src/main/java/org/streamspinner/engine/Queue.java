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

import org.streamspinner.Recoverable;
import org.streamspinner.InternalState;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.OperatorGroup;


public interface Queue {

	public void createEntry(OperatorGroup id, SourceSet windows) throws StreamSpinnerException ;

	public boolean hasEntry(OperatorGroup id);

	public void removeEntry(OperatorGroup id) throws StreamSpinnerException ;

	public int getEntryCount();

	public Schema getSchema();

	public void append(Tuple t) throws StreamSpinnerException ;

	public void append(TupleSet ts) throws StreamSpinnerException ;

	public TupleSet collectGarbage(long executiontime) throws StreamSpinnerException ;

	public TupleSet getDeltaTupleSet(OperatorGroup id, long executiontime) throws StreamSpinnerException ;

	public TupleSet getDeltaTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException ;

	public TupleSet getWindowTupleSet(OperatorGroup id, long executiontime) throws StreamSpinnerException ;

	public TupleSet getWindowTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException ;

	public void moveToWindow(OperatorGroup id, long executiontime) throws StreamSpinnerException ;

	public boolean isAcceptable(InternalState state);

	public QueueInternalState getInternalState();

	public void setInternalState(InternalState state) throws StreamSpinnerException ;

}

