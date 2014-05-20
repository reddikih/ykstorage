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
import org.streamspinner.InternalState;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.OperatorGroup;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.HashMap;


public class PushBasedQueue implements Queue, Serializable {

	private class TableEntry implements Serializable {
		public OperatorGroup id;
		public SourceSet windows;
		public ListEntry lastexec;
		public long lastexecutiontime;

		public TableEntry(OperatorGroup o, SourceSet w){
			id = o;
			windows = w;
			lastexec = null;
			lastexecutiontime = Long.MIN_VALUE;
		}

		public int hashCode(){
			return id.hashCode();
		}

		public boolean equals(Object o){
			if(! (o instanceof TableEntry))
				return false;
			TableEntry target = (TableEntry)(o);
			return id.equals(target.id) && windows.equals(target.windows);
		}
	}

	private class ListEntry implements Serializable {
		public Tuple tuple;
		public ListEntry previous;
		public ListEntry next;

		public ListEntry(Tuple t, ListEntry p, ListEntry n){
			tuple = t;
			previous = p;
			next = n;
		}
	}

	private class PushBasedQueueInternalState extends QueueInternalState implements Serializable {
		private HashMap<OperatorGroup, TableEntry> statetable;
		private ListEntry statefirst;
		private ListEntry statelast;
		private int statelistcounter;
		private Schema stateschema;
		private SourceSet statemaxwindow;

		public PushBasedQueueInternalState(){
			statetable = table;
			statefirst = first;
			statelast = last;
			statelistcounter = listcounter;
			stateschema = schema;
			statemaxwindow = maxwindow;
		}
	}

	private HashMap<OperatorGroup,TableEntry> table;
	private ListEntry first;
	private ListEntry last;
	private int listcounter;
	private Schema schema;

	private SourceSet maxwindow;

	public PushBasedQueue(Schema s){
		this.table = new HashMap<OperatorGroup,TableEntry>();
		this.first = null;
		this.last = null;
		this.listcounter = 0;
		this.maxwindow = new SourceSet();
		this.schema = s;
	}

	public void createEntry(OperatorGroup id, SourceSet windows) throws StreamSpinnerException {
		TableEntry te = new TableEntry(id, windows);
		te.lastexec = last;
		table.put(te.id, te);
		maxwindow = maxwindow.concat(windows);
	}

	public boolean hasEntry(OperatorGroup id){
		return table.containsKey(id);
	}

	public int getEntryCount(){
		return table.size();
	}

	public void removeEntry(OperatorGroup id) throws StreamSpinnerException {
		if(table.containsKey(id)){
			TableEntry te = table.get(id);
			te.lastexec = null;
			te.windows = null;
			te.id = null;
			table.remove(id);
		}
	}

	public Schema getSchema(){
		return schema;
	}

	public void append(Tuple t) throws StreamSpinnerException {
		ListEntry le = new ListEntry(t, last, null);
		if(first == null)
			first = le;
		if(last != null)
			last.next = le;
		last = le;
		listcounter++;
	}

	public void append(TupleSet ts) throws StreamSpinnerException {
		ts.beforeFirst();
		while(ts.next() == true)
			append(ts.getTuple());
	}

	public TupleSet collectGarbage(long executiontime) throws StreamSpinnerException {
		ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
		ListEntry le = first;
		while(le != null){
			ListEntry target = le;
			le = le.next;
			if(! target.tuple.willBeIncludedWindow(executiontime, maxwindow)){
				if(target.previous != null)
					target.previous.next = target.next;
				else
					first = target.next;

				if(target.next != null)
					target.next.previous = target.previous;
				else
					last = target.previous;

				tuplelist.add(target.tuple);
				target.tuple = null;
				target.previous = null;
				target.next = null;
				listcounter--;
			}
		}
		return new OnMemoryTupleSet(schema, tuplelist);
	}

	public TupleSet getDeltaTupleSet(OperatorGroup id, long executiontime) throws StreamSpinnerException {
		return getDeltaTupleSet(id, id, executiontime);
	}

	public TupleSet getDeltaTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException {
		if(! table.containsKey(baseid))
			throw new StreamSpinnerException("no entry is prepared for " + baseid.toString());
		if(! table.containsKey(windowid))
			throw new StreamSpinnerException("no entry is prepared for " + windowid.toString());

		TableEntry basete = table.get(baseid);
		TableEntry windowte = table.get(windowid);
		SourceSet windows = windowte.windows;
		ListEntry start = first;

		if(basete.lastexec != null && basete.lastexec.tuple != null){
			Tuple t = basete.lastexec.tuple;
			if(t.isIncludedWindow(executiontime, windows))
				start = basete.lastexec.next;
			else
				basete.lastexec = null;
		}

		ArrayList<Tuple> tuplelist = new ArrayList<Tuple>(listcounter);
		for(ListEntry le = start; le != null ; le = le.next){
			Tuple t = le.tuple;
			if(t.isIncludedWindow(executiontime, windows))
				tuplelist.add(le.tuple);
		}
		return new OnMemoryTupleSet(schema, tuplelist);
	}

	public TupleSet getWindowTupleSet(OperatorGroup id, long executiontime) throws StreamSpinnerException {
		return getWindowTupleSet(id, id, executiontime);
	}

	public TupleSet getWindowTupleSet(OperatorGroup baseid, OperatorGroup windowid, long executiontime) throws StreamSpinnerException {
		if(! table.containsKey(baseid))
			throw new StreamSpinnerException("no entry is prepared for " + baseid.toString());
		if(! table.containsKey(windowid))
			throw new StreamSpinnerException("no entry is prepared for " + windowid.toString());

		TableEntry basete = table.get(baseid);
		TableEntry windowte = table.get(windowid);
		SourceSet windows = windowte.windows;
		ListEntry start = null;

		if(basete.lastexec != null && basete.lastexec.tuple != null){
			Tuple t = basete.lastexec.tuple;
			if(t.isIncludedWindow(executiontime, windows))
				start = basete.lastexec;
			else
				basete.lastexec = null;
		}

		LinkedList<Tuple> tuplelist = new LinkedList<Tuple>();

		for(ListEntry le = start; le != null; le = le.previous){
			Tuple t = le.tuple;
			if(t.isIncludedWindow(executiontime, windows) )
				tuplelist.addFirst(t);
		}

		return new OnMemoryTupleSet(schema, tuplelist);
	}


	public int size(){
		return listcounter;
	}

	public void moveToWindow(OperatorGroup id, long executiontime) throws StreamSpinnerException {
		if(! table.containsKey(id))
			throw new StreamSpinnerException("no entry is prepared for " + id.toString());
		TableEntry te = table.get(id);
		SourceSet windows = te.windows;
		ListEntry start = first;

		if(te.lastexec != null && te.lastexec.tuple != null){
			Tuple t = te.lastexec.tuple;
			if(t.isIncludedWindow(executiontime, windows))
				start = te.lastexec;
			else
				te.lastexec = null;
		}
		for(ListEntry le = start; le != null; le = le.next){
			Tuple t = le.tuple;
			if(t.isIncludedWindow(executiontime, windows))
				te.lastexec = le;
		}
		te.lastexecutiontime = executiontime;
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof PushBasedQueueInternalState;
	}

	public QueueInternalState getInternalState(){
		return new PushBasedQueueInternalState();
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(isAcceptable(state)){
			PushBasedQueueInternalState pstate = (PushBasedQueueInternalState)state;
			if(! (schema.equals(pstate.stateschema)))
				throw new StreamSpinnerException("Schema mismatch is detected");
			table = pstate.statetable;
			first = pstate.statefirst;
			last = pstate.statelast;
			listcounter = pstate.statelistcounter;
			maxwindow = pstate.statemaxwindow;
		}
		else
			throw new StreamSpinnerException("Different type of QueueInternalState is specified");
	}
}

