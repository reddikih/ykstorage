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
package org.streamspinner.distributed;

import org.streamspinner.*;
import org.streamspinner.engine.*;
import org.streamspinner.wrapper.*;
import org.streamspinner.query.*;
import org.streamspinner.connection.*;
import java.io.Serializable;
import java.util.Vector;
import java.rmi.*;
import java.rmi.server.*;

/*
 * A durable version of Remote StreamServerWrapper for Sustainable StreamSpinner
 */
public class DurableRemoteStreamServerWrapper extends Wrapper implements Recoverable {

	public static final String PARAMETER_ATTRIBUTES="attributes";
	public static final String PARAMETER_QUERY="query";
	public static final String PARAMETER_TABLENAME="tablename";
	public static final String PARAMETER_TYPES="types";
	public static final String PARAMETER_SOURCEURL="sourceurl";   /* RMI location of source streamspinner */
	public static final String PARAMETER_PRIMARYURL="primaryurl"; /* RMI location of primary wrapper. Use the same url as 'myurl' if the wrapper is the primary */
	public static final String PARAMETER_MYURL="myurl";           /* RMI location of this wrapper */
	public static final String PARAMETER_SLEEP="sleep";           /* sleep time for monitoring primary */

	private String attributes;
	private String query;
	private String tablename;
	private String types;
	private String sourceurl;
	private String primaryurl;
	private String myurl;
	private long sleep;
	private Schema schema;
	private MonitorableConnectionImpl conn;
	private Long lastCommitSeq;

	public DurableRemoteStreamServerWrapper(String name) throws StreamSpinnerException {
		super(name);
		attributes = null;
		query = null;
		tablename = null;
		types = null;
		sourceurl = null;
		primaryurl = null;
		myurl = null;
		sleep = 5000;
		schema = null;
		conn = null;
		lastCommitSeq = new Long(-1);
	}

	public String[] getAllTableNames(){
		if(tablename != null){
			String[] rval = { tablename };
			return rval;
		}
		else
			return new String[0];
	}

	public Schema getSchema(String name){
		if(tablename.equals(name))
			return schema.copy();
		else
			return null;
	}

	public TupleSet getTupleSet(ORNode on){
		return null;
	}

	public void init() throws StreamSpinnerException {
		query = checkParameter(PARAMETER_QUERY);
		attributes = checkParameter(PARAMETER_ATTRIBUTES);
		types = checkParameter(PARAMETER_TYPES);
		tablename = checkParameter(PARAMETER_TABLENAME);
		sourceurl = checkParameter(PARAMETER_SOURCEURL);
		primaryurl = checkParameter(PARAMETER_PRIMARYURL);
		myurl = checkParameter(PARAMETER_MYURL);
		sleep = Long.parseLong(checkParameter(PARAMETER_SLEEP));

		schema = createSchema();
	}

	private String checkParameter(String parametername) throws StreamSpinnerException {
		String value = getParameter(parametername);
		if(value == null || value.equals(""))
			throw new StreamSpinnerException("Parameter '" + parametername + "' is not specified.");
		return value;
	}

	private Schema createSchema() throws StreamSpinnerException {
		String[] a = attributes.split("\\s*,\\s*");
		String[] t = types.split("\\s*,\\s*");

		if(a == null || a.length == 0 || t == null || t.length == 0 || a.length != t.length)
			throw new StreamSpinnerException("Can not create a schema from given parameters: \"" + attributes + "\", \"" + types + "\"");
		Schema rval = new Schema(tablename, a, t);
		rval.setTableType(Schema.STREAM);
		return rval;
	}

	public void start() throws StreamSpinnerException {
		try {
			conn = new MonitorableConnectionImpl();
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}


	public void stop() throws StreamSpinnerException {
		try {
			if(conn != null){
				conn.stop();
				conn = null;
			}
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof DurableRemoteStreamServerWrapperInternalState;
	}

	public InternalState getInternalState(){
		DurableRemoteStreamServerWrapperInternalState rval = new DurableRemoteStreamServerWrapperInternalState();
		if(conn != null)
			rval.setSequenceNumber(conn.seq);
		return rval;
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(isAcceptable(state)){
			DurableRemoteStreamServerWrapperInternalState dstate = (DurableRemoteStreamServerWrapperInternalState)state;
			if(conn != null){
				conn.seq = dstate.getSequenceNumber();
				try {
					conn.sendAcknowledge(dstate.getSequenceNumber());
				} catch(RemoteException re){
					throw new StreamSpinnerException(re);
				}
			}
		}
		else
			throw new StreamSpinnerException("Unknown state object is given");
	}


	public void snapshotCreated() throws StreamSpinnerException {
		synchronized(lastCommitSeq){
			if(conn == null || conn.mconn != null)
				return;
			else
				lastCommitSeq = conn.seq;
		}
	}

	public void snapshotCopied() throws StreamSpinnerException {
		synchronized(lastCommitSeq){
			if(conn == null)
				return;
			else {
				try {
					conn.sendAcknowledge(lastCommitSeq);
				} catch(RemoteException re){
					throw new StreamSpinnerException(re);
				}
			}
		}
	}


	private class MonitorableConnectionImpl extends UnicastRemoteObject implements Connection, MonitorableConnection, Runnable {

		private MonitorableConnection mconn = null;
		private RemoteStreamServer rss = null;
		private Long cid = new Long(-1);
		private Long seq = new Long(-1);
		private Thread monitor = null;

		private MonitorableConnectionImpl() throws RemoteException {
			super();

			if (System.getSecurityManager() == null)
				System.setSecurityManager(new RMISecurityManager());

			try {
				Naming.rebind(myurl, this);

				if(myurl.equals(primaryurl))
					start();
				else {
					mconn = (MonitorableConnection)(Naming.lookup(primaryurl));
					cid = mconn.getConnectionID();
					monitor = new Thread(this);
					monitor.start();
				}
			} catch(Exception e){
				throw new RemoteException("", e);
			}
		}

		public void run(){
			try {
				Thread.sleep(sleep);
				while(monitor != null && monitor.equals(Thread.currentThread()) && mconn != null){
					try {
						mconn.receivePing();
					} catch(RemoteException re){
						monitor = null;
						mconn = null;
						start();
						return;
					}
					Thread.sleep(sleep);
				}
			} catch(Exception e){
				System.err.println(e.getMessage());
			}
		}

		public void start() throws RemoteException {
			try {
				rss = (RemoteStreamServer)(Naming.lookup(sourceurl));
				if(cid < 0)
					cid = rss.startQuery(query, this);
				else 
					rss.updateConnection(cid, this);
			} catch(Exception e){
				throw new RemoteException("", e);
			}
		}

		public void stop() throws RemoteException {
			if(rss != null){
				rss.stopQuery(cid);
			}
			rss = null;
			monitor = null;
			mconn = null;
		}


		public long receiveDeliveryUnit(DeliveryUnit du) throws RemoteException {
			synchronized(seq){
				long tmpno = du.getSequenceNumber();
				if(tmpno <= seq)     // Duplicate message is delivered. It must be ignored.
					return seq;
				if(tmpno > seq + 1)    // Some messages are dropped. They must be recovered.
					return seq;
				else {
					seq = tmpno;
					long now = System.currentTimeMillis();
					try {
						for(Tuple t : du.getTuples())
							t.setTimestamp(tablename, now);
						TupleSet ts = new OnMemoryTupleSet(schema, du.getTuples());
						ts.beforeFirst();
						deliverTupleSet(now, tablename, ts);
					} catch(StreamSpinnerException sse){
						throw new RemoteException("", sse);
					}
					return seq;
				}
			}
		}

		public void receiveSchema(Schema s) throws RemoteException { ; }

		public void receiveCQException(CQException e) throws RemoteException {
			System.err.println(getName() + ": " + e.getMessage());
		}

		public void receiveRecoveredUnits(long newseqno, DeliveryUnit[] units) throws RemoteException {
			synchronized(seq){
				for(DeliveryUnit du : units){
					if(du.getSequenceNumber() > seq){
						long time = du.getTimestamp();
						for(Tuple t : du.getTuples())
							t.setTimestamp(tablename, time);
						TupleSet ts = new OnMemoryTupleSet(schema, du.getTuples());
						deliverTupleSet(time, tablename, ts);
					}
				}
				if(newseqno > seq)
					seq = newseqno;
			}
		}

		public void reconnect(RemoteStreamServer r) throws RemoteException {
			rss = r;
		}

		public long getConnectionID() throws RemoteException {
			return cid;
		}

		public long receivePing() throws RemoteException {
			synchronized(seq){
				return seq;
			}
		}

		public void sendAcknowledge(long seqno) throws RemoteException {
			if(seqno <= seq){
				if(mconn != null)
					mconn.sendAcknowledge(seqno);
				else if(rss != null)
					rss.receiveAcknowledge(cid, seqno);
			}
		}

	}

}

class DurableRemoteStreamServerWrapperInternalState implements InternalState, Serializable {

	private long stateOfSeqno;

	public DurableRemoteStreamServerWrapperInternalState(){
		stateOfSeqno = -1;
	}

	public long getSequenceNumber(){
		return stateOfSeqno;
	}

	public void setSequenceNumber(long n){
		stateOfSeqno = n;
	}

}
