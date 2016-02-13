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
import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.ArchiveManager;
import org.streamspinner.SystemManager;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.ORNode;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Set;
import java.util.Iterator;

public class Distributor {

	private Hashtable listenertable;
	private SystemManager manager;

	public Distributor(){
		listenertable = new Hashtable();
		manager = null;
	}

	public void addArrivalTupleListener(Object qid, ArrivalTupleListener listener){
		if(listenertable.containsKey(qid)){
			Vector llist = (Vector)(listenertable.get(qid));
			llist.add(listener);
		}
		else {
			Vector llist = new Vector();
			llist.add(listener);
			listenertable.put(qid, llist);
		}
	}

	public void removeArrivalTupleListener(Object qid, ArrivalTupleListener listener){
		if(! listenertable.containsKey(qid))
			return;

		Vector llist = (Vector)(listenertable.get(qid));
		for(int i=0; i < llist.size(); i++){
			if(listener.equals(llist.get(i))){
				llist.remove(i);
				if(llist.size() == 0)
					listenertable.remove(qid);
			}
		}
	}

	public void setSystemManager(SystemManager sm){
		manager = sm; 
	}

	public void deliver(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, ArchiveManager am) throws StreamSpinnerException {
		ANDNode[] node = op.getANDNodes();

		ORNode inputn = op.getInputORNodes()[0];
		Queue inputq = qm.getQueue(plan, op.getMasterSet(), inputn);

		TupleSet tset = inputq.getDeltaTupleSet(op, executiontime);
		if(tset.first() == false)
			return;
		for(int i=0; i < node.length; i++){
			Set qids = node[i].getQueryIDSet();
			DeliverThread dt = new DeliverThread(qids, executiontime, tset);
			dt.start();
		}
		inputq.moveToWindow(op, executiontime);
	}

	private class DeliverThread extends Thread {
		private Set qids;
		private long executiontime;
		private TupleSet ts;

		private DeliverThread(Set qids, long executiontime, TupleSet ts){
			this.qids = qids;
			this.executiontime = executiontime;
			this.ts = ts;
		}

		public void run(){
			for(Iterator it = qids.iterator(); it.hasNext(); ){
				Object qid = it.next();
				if(! listenertable.containsKey(qid))
					continue;
				Vector llist = (Vector)(listenertable.get(qid));
				for(int i=0; i < llist.size(); i++){
					ArrivalTupleListener atl = (ArrivalTupleListener)llist.get(i);
					try {
						ts.beforeFirst();
						atl.receiveTupleSet(executiontime, qid.toString(), ts);
					} catch(StreamSpinnerException sse){
						sse.printStackTrace();
					}
				}
			}
			if(manager != null)
				manager.dataDistributedTo(executiontime, qids, ts);
		}
	}
}

