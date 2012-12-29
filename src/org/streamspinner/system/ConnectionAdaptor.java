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
package org.streamspinner.system;

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.ExceptionListener;
import org.streamspinner.Recoverable;
import org.streamspinner.InternalState;
import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.Query;
import org.streamspinner.query.DAG;
import org.streamspinner.connection.Connection;
import org.streamspinner.connection.CQException;
import org.streamspinner.connection.DeliveryUnit;
import java.io.Serializable;
import java.util.Vector;
import java.util.LinkedList;
import java.rmi.RemoteException;


public class ConnectionAdaptor implements ArrivalTupleListener, ExceptionListener, Recoverable {

	public static final int MAX_SIZE = 90;
	private static long counter = 0;
	private long id;
	private Query query;
	private DAG dag;
	private Connection conn;
	private long seqno = -1;
	private LinkedList<DeliveryUnit> buffer;

	public ConnectionAdaptor(Query q, Connection c, DAG d){
		id = counter;
		query = q;
		conn = c;
		dag = d;
		buffer = new LinkedList<DeliveryUnit>();

		counter++;
	}

	public long getID(){
		return id;
	}

	public Query getQuery(){
		return query;
	}

	public Connection getConnection(){
		return conn;
	}

	public DAG getDAG(){
		return dag;
	}

	public void setQuery(Query q){
		query = q;
	}

	public void setConnection(Connection c){
		conn = c;
	}

	public void setDAG(DAG d){
		dag = d;
	}

	public void receiveTupleSet(long executiontime, String source, TupleSet ts) throws StreamSpinnerException {
		DeliveryUnit du = null;
		synchronized(buffer){
			seqno++;
			du = new DeliveryUnit(executiontime, seqno, ts);
			buffer.add(du);
			if(buffer.size() > MAX_SIZE)
				buffer.remove(0);
		}
		try {
			sendDeliveryUnit(du, ts.getSchema());
		} catch(RemoteException re){
			throw new StreamSpinnerException(re);
		}
	}

	private void sendDeliveryUnit(DeliveryUnit du, Schema s) throws RemoteException, StreamSpinnerException {
		conn.receiveSchema(s);
		long status = conn.receiveDeliveryUnit(du);
		if(status < du.getSequenceNumber())
			resendDeliveryUnits(status);
	}

	public void receiveException(long timestamp, Exception e) {
		try {
			conn.receiveCQException(new CQException(e));
		} catch(RemoteException re){
			re.printStackTrace();
		}
	}

	public void clearBuffer(long seqno){
		synchronized(buffer){
			for(int i = buffer.size() -1; i >= 0; i--){
				DeliveryUnit du = buffer.get(i);
				if(du.getSequenceNumber() <= seqno)
					buffer.remove(i);
				else
					break;
			}
		}
	}

	public void resendDeliveryUnits(long no) throws StreamSpinnerException {
		Vector<DeliveryUnit> ulist = new Vector<DeliveryUnit>();
		synchronized(buffer){
			for(DeliveryUnit du : buffer){
				if(du.getSequenceNumber() > no)
					ulist.add(du);
			}
		}
		DeliveryUnit[] uarray = (DeliveryUnit[])(ulist.toArray(new DeliveryUnit[0]));
		try {
			conn.receiveRecoveredUnits(seqno, uarray);
		} catch(RemoteException re){
			throw new StreamSpinnerException(re);
		}
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof ConnectionAdaptorInternalState;
	}

	public ConnectionAdaptorInternalState getInternalState(){
		synchronized(buffer){
			ConnectionAdaptorInternalState rval = new ConnectionAdaptorInternalState();
			rval.setConnectionID(id);
			rval.setSequenceNumber(seqno);
			rval.setBuffer(buffer);
			return rval;
		}
	}


	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(! isAcceptable(state))
			throw new StreamSpinnerException("Unknown state object is given");

		synchronized(buffer){
			ConnectionAdaptorInternalState cstate = (ConnectionAdaptorInternalState)state;
			if(id != cstate.getConnectionID())
				throw new StreamSpinnerException("ID is different. expected = " + id + ", but given = " + cstate.getConnectionID());
			seqno = cstate.getSequenceNumber();
			buffer = cstate.getBuffer();
		}
	}
}
