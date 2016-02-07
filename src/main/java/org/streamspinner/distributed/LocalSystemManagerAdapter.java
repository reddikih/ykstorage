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
import org.streamspinner.query.*;
import org.streamspinner.engine.*;
import org.streamspinner.wrapper.RemoteStreamServerWrapper;
import java.rmi.RemoteException;
import java.util.Set;

public class LocalSystemManagerAdapter implements SystemManager {

	private NodeManagerImpl nmi;
	private SystemManager sm;

	public LocalSystemManagerAdapter(NodeManagerImpl nmi, SystemManager sm){
		this.nmi = nmi;
		this.sm = sm;
	}

	private final int QUERY_REGISTERED = 0;
	private final int QUERY_DELETED = 1;
	private final int DATA_DISTRIBUTED_TO = 2;
	private final int DATA_RECEIVED = 3;
	private final int INFORMATIONSOURCE_ADDED = 4;
	private final int INFORMATIONSOURCE_DELETED = 5;
	private final int CONNECTION_ESTABLISHED = 6;
	private final int CONNECTION_CLOSED = 7;
	private final int TABLE_CREATED = 8;
	private final int TABLE_DROPPED = 9;

	private class NotificationThread extends Thread {
		private int command = -1;
		private Object[] args;

		public NotificationThread(int command, Object... args){
			this.command = command;
			this.args = args;
		}

		public void run(){
			try {
				switch(command){
					case QUERY_REGISTERED:
						nmi.notifyQueryRegistered((Query)(args[0]));
						break;
					case QUERY_DELETED:
						nmi.notifyQueryDeleted((Query)(args[0]));
						break;
					case DATA_DISTRIBUTED_TO:
						nmi.notifyDataDistributedTo((Long)(args[0]), (Set)(args[1]));
						break;
					case DATA_RECEIVED:
						nmi.notifyDataReceived((Long)(args[0]), (String)(args[1]));
						break;
					case INFORMATIONSOURCE_ADDED:
						nmi.notifyInformationSourceAdded((String)(args[0]), (String[])(args[1]));
						break;
					case INFORMATIONSOURCE_DELETED:
						nmi.notifyInformationSourceDeleted((String)(args[0]));
						break;
					case CONNECTION_ESTABLISHED:
						nmi.notifyConnectionEstablished((ConnectionInfo)(args[0]));
						break;
					case CONNECTION_CLOSED:
						nmi.notifyConnectionClosed((ConnectionInfo)(args[0]));
						break;
					case TABLE_CREATED:
						nmi.notifyTableCreated((String)(args[0]), (String)(args[1]));
						break;
					case TABLE_DROPPED:
						nmi.notifyTableDropped((String)(args[0]), (String)(args[1]));
						break;
					default:
						break;
				}
			} catch(RemoteException re){
				re.printStackTrace();
			}
		}
	}

	public void queryRegistered(Query q){
		sm.queryRegistered(q);
		Thread t = new NotificationThread(QUERY_REGISTERED, q);
		t.start();
	}

	public void queryDeleted(Query q){
		sm.queryDeleted(q);
		Thread t = new NotificationThread(QUERY_DELETED, q);
		t.start();
	}

	public void dataDistributedTo(long timestamp, Set queryids, TupleSet ts){
		sm.dataDistributedTo(timestamp, queryids, ts);
		Thread t = new NotificationThread(DATA_DISTRIBUTED_TO, timestamp, queryids);
		t.start();
	}

	public void dataReceived(long timestamp, String source, TupleSet ts){
		sm.dataReceived(timestamp, source, ts);
		Thread t = new NotificationThread(DATA_RECEIVED, timestamp, source);
		t.start();
	}

	public void executionPerformed(long timestamp, String master, long duration, long delay){
		sm.executionPerformed(timestamp, master, duration, delay);
	}

	public void startCacheConsumer(long timestamp, OperatorGroup og, double ratio){
		sm.startCacheConsumer(timestamp, og, ratio);
	}

	public void endCacheConsumer(long timestamp, OperatorGroup og, double ratio){
		sm.endCacheConsumer(timestamp, og, ratio);
	}

	public void informationSourceAdded(InformationSource is){
		sm.informationSourceAdded(is);
		String wrappername = is.getName();
		String[] tablenames = is.getAllTableNames();
		Thread t = new NotificationThread(INFORMATIONSOURCE_ADDED, wrappername, tablenames);
		t.start();

		if(is instanceof RemoteStreamServerWrapper){
			RemoteStreamServerWrapper rssw = (RemoteStreamServerWrapper)is;
			ConnectionInfo conn = new ConnectionInfo(rssw);
			Thread tc = new NotificationThread(CONNECTION_ESTABLISHED, conn);
			tc.start();
		}
	}

	public void informationSourceDeleted(InformationSource is){
		sm.informationSourceDeleted(is);
		String wrappername = is.getName();
		Thread t = new NotificationThread(INFORMATIONSOURCE_DELETED, wrappername);
		t.start();

		if(is instanceof RemoteStreamServerWrapper){
			RemoteStreamServerWrapper rssw = (RemoteStreamServerWrapper)is;
			ConnectionInfo conn = new ConnectionInfo(rssw);
			Thread tc = new NotificationThread(CONNECTION_CLOSED, conn);
			tc.start();
		}
	}

	public void tableCreated(String wrappername, String tablename, Schema schema){
		sm.tableCreated(wrappername, tablename, schema);
		Thread t = new NotificationThread(TABLE_CREATED, wrappername, tablename);
		t.start();
	}

	public void tableDropped(String wrappername, String tablename){
		sm.tableDropped(wrappername, tablename);
		Thread t = new NotificationThread(TABLE_DROPPED, wrappername, tablename);
		t.start();
	}

}
