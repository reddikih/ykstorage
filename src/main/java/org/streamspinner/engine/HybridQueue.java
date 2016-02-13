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
import org.streamspinner.InformationSource;
import org.streamspinner.InternalState;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.OperatorGroup;
import java.io.Serializable;
import java.util.*;


public class HybridQueue implements Queue {

	private PushBasedQueue innerqueue;
	private ORNode node;
	private InformationSource is;
	private boolean extracted;

	public HybridQueue(ORNode gid, Schema s, InformationSource source){
		innerqueue = new PushBasedQueue(s);
		node = gid;
		is = source;
		extracted = false;
	}

	public void createEntry(OperatorGroup id, SourceSet windows) throws StreamSpinnerException {
		innerqueue.createEntry(id, windows);
	}

	public boolean hasEntry(OperatorGroup id){
		return innerqueue.hasEntry(id);
	}

	public void removeEntry(OperatorGroup id) throws StreamSpinnerException {
		innerqueue.removeEntry(id);
	}

	public int getEntryCount(){
		return innerqueue.size();
	}

	public Schema getSchema(){
		return innerqueue.getSchema();
	}

	public void append(Tuple t) throws StreamSpinnerException {
		Tuple newt = new Tuple(t.size());
		for(int i=0; i < t.size(); i++)
			newt.setObject(i, t.getObject(i));
		newt.setTimestamp(node.getSources().iterator().next(), t.getMinTimestamp());
		innerqueue.append(newt);
	}

	public void append(TupleSet ts) throws StreamSpinnerException {
		ts.beforeFirst();
		while(ts.next())
			append(ts.getTuple());
	}

	public TupleSet collectGarbage(long executiontime) throws StreamSpinnerException {
		return innerqueue.collectGarbage(executiontime);
	}

	public TupleSet getDeltaTupleSet(OperatorGroup qid, long executiontime) throws StreamSpinnerException {
		extractTupleSet();
		return innerqueue.getDeltaTupleSet(qid, executiontime);
	}

	public TupleSet getDeltaTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException {
		extractTupleSet();
		return innerqueue.getDeltaTupleSet(baseid, windowid, executiontime);
	}

	private void extractTupleSet() throws StreamSpinnerException {
		if(extracted == false){
			TupleSet ts = is.getTupleSet(node);
			innerqueue.append(ts);
			ts.close();
			extracted = true;
		}
	}

	public TupleSet getWindowTupleSet(OperatorGroup qid, long executiontime) throws StreamSpinnerException {
		return innerqueue.getWindowTupleSet(qid, executiontime);
	}

	public TupleSet getWindowTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException {
		return innerqueue.getWindowTupleSet(baseid, windowid, executiontime);
	}

	public void moveToWindow(OperatorGroup id, long executiontime) throws StreamSpinnerException {
		innerqueue.moveToWindow(id, executiontime);
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof HybridQueueInternalState;
	}

	public QueueInternalState getInternalState(){
		HybridQueueInternalState rval = new HybridQueueInternalState();
		rval.setInnerQueueInternalState(innerqueue.getInternalState());
		rval.setExtracted(extracted);
		rval.setORNode(node);
		return rval;
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(isAcceptable(state)){
			HybridQueueInternalState hstate = (HybridQueueInternalState)state;
			if(! (node.equals(hstate.getORNode())))
				throw new StreamSpinnerException("ORNode mismatch is detected");
			innerqueue.setInternalState(hstate.getInnerQueueInternalState());
			extracted = hstate.isExtracted();
		}
		else
			throw new StreamSpinnerException("Different type of QueueInternalState is specified");
	}

}

class HybridQueueInternalState extends QueueInternalState implements Serializable {

	private QueueInternalState stateOfInnerQueue = null;
	private ORNode stateOfNode = null;
	private boolean stateOfExtracted = false;

	public HybridQueueInternalState(){
		;
	}

	public QueueInternalState getInnerQueueInternalState(){
		return stateOfInnerQueue;
	}

	public ORNode getORNode(){
		return stateOfNode;
	}

	public boolean isExtracted(){
		return stateOfExtracted;
	}

	public void setInnerQueueInternalState(QueueInternalState qis){
		stateOfInnerQueue = qis;
	}

	public void setExtracted(boolean f){
		stateOfExtracted = f;
	}

	public void setORNode(ORNode n){
		stateOfNode = n;
	}

}
