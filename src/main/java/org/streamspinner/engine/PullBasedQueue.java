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


public class PullBasedQueue implements Queue {

	private ORNode node;
	private InformationSource is;
	private boolean extracted;
	private HashMap<OperatorGroup, SourceSet> entry;
	private Schema schema;

	public PullBasedQueue(ORNode gid, Schema s, InformationSource source){
		node = gid;
		is = source;
		extracted = false;
		schema = s;
		entry = new HashMap<OperatorGroup, SourceSet>();
	}

	public void createEntry(OperatorGroup id, SourceSet windows) throws StreamSpinnerException {
		entry.put(id, windows);
	}

	public boolean hasEntry(OperatorGroup id){
		return entry.containsKey(id);
	}

	public void removeEntry(OperatorGroup id) throws StreamSpinnerException {
		entry.remove(id);
	}

	public int getEntryCount(){
		return entry.size();
	}

	public Schema getSchema(){
		return schema;
	}

	public void append(Tuple t) throws StreamSpinnerException {
		//throw new StreamSpinnerException("this queue is not writable");
	}

	public void append(TupleSet ts) throws StreamSpinnerException {
		//throw new StreamSpinnerException("this queue is not writable");
	}

	public TupleSet collectGarbage(long executiontime) throws StreamSpinnerException {
		return new OnMemoryTupleSet(null);
	}

	public TupleSet getDeltaTupleSet(OperatorGroup qid, long executiontime) throws StreamSpinnerException {
		if(extracted == false)
			return is.getTupleSet(node);
		else
			return new OnMemoryTupleSet(null);
	}

	public TupleSet getDeltaTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException {
		return getDeltaTupleSet(baseid, executiontime);
	}


	public TupleSet getWindowTupleSet(OperatorGroup qid, long executiontime) throws StreamSpinnerException {
		if(extracted == true)
			return is.getTupleSet(node);
		else
			return new OnMemoryTupleSet(null);
	}

	public TupleSet getWindowTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException {
		return getWindowTupleSet(baseid, executiontime);
	}

	public void moveToWindow(OperatorGroup id, long executiontime) throws StreamSpinnerException {
		extracted = true;
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof PullBasedQueueInternalState;
	}

	public QueueInternalState getInternalState(){
		PullBasedQueueInternalState rval = new PullBasedQueueInternalState();
		rval.setORNode(node);
		rval.setExtracted(extracted);
		rval.setEntryHashMap(entry);
		rval.setSchema(schema);
		return rval;
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(isAcceptable(state)){
			PullBasedQueueInternalState pstate = (PullBasedQueueInternalState)state;
			if(! node.equals(pstate.getORNode()))
				throw new StreamSpinnerException("Impossible queue substitution is detected");
			if(! schema.equals(pstate.getSchema()))
				throw new StreamSpinnerException("Schema mismatch is detected");
			extracted = pstate.isExtracted();
			entry = pstate.getEntryHashMap();
		}
		else
			throw new StreamSpinnerException("Different type of QueueInternalState is specified");
	}

}

class PullBasedQueueInternalState extends QueueInternalState implements Serializable {

	private ORNode ornode = null;
	private boolean extracted = false;
	private HashMap<OperatorGroup, SourceSet> entry = null;
	private Schema schema = null;

	public PullBasedQueueInternalState(){
		super();
	}

	public ORNode getORNode(){
		return ornode;
	}

	public boolean isExtracted(){
		return extracted;
	}

	public HashMap<OperatorGroup, SourceSet> getEntryHashMap(){
		return entry;
	}

	public Schema getSchema(){
		return schema;
	}

	public void setORNode(ORNode node){
		ornode = node;
	}

	public void setExtracted(boolean f){
		extracted = f;
	}

	public void setEntryHashMap(HashMap<OperatorGroup,SourceSet> hm){
		entry = hm;
	}

	public void setSchema(Schema s){
		schema = s;
	}

}
