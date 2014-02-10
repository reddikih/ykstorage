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

import java.rmi.*;
import java.rmi.server.*;
import org.streamspinner.*;
import org.streamspinner.system.*;
import org.streamspinner.gui.*;
import org.streamspinner.engine.*;
import org.streamspinner.query.*;
import org.streamspinner.wrapper.*;
import java.util.*;
import java.net.*;


public class NodeManagerImpl extends UnicastRemoteObject implements NodeManager {

	private StreamSpinnerMainSystem system;
	private LocalSystemManagerAdapter adapter;
	private Vector<DistributedSystemManager> dsmlist;
	private Vector<Query> qlist;
	private String nodename;

	public NodeManagerImpl(StreamSpinnerMainSystem ssms, SystemManager sm) throws RemoteException {
		super();

		system = ssms;
		adapter = new LocalSystemManagerAdapter(this, sm);
		system.setSystemManager(adapter);

		dsmlist = new Vector<DistributedSystemManager>();
		qlist = new Vector<Query>();

		nodename = updateNodeName();

		try {
			if(System.getSecurityManager() == null)
				System.setSecurityManager(new RMISecurityManager());
			Naming.rebind("rmi://localhost/NodeManager", this);
		} catch(MalformedURLException me){
			throw new RemoteException("", me);
		}
	}

	public void addDistributedSystemManager(DistributedSystemManager dsm) throws RemoteException{
		dsmlist.add(dsm);
	}

	public void removeDistributedSystemManager(DistributedSystemManager dsm) throws RemoteException{
		dsmlist.remove(dsm);
	}

	public String getNodeName() throws RemoteException{
		return nodename;
	}

	private String updateNodeName() throws RemoteException {
		try {
			InetAddress addr = InetAddress.getLocalHost();
			return addr.getCanonicalHostName();
		} catch(java.net.UnknownHostException uhe){
			throw new RemoteException("", uhe);
		}
	}

	public String[] getInformationSourceNames() throws RemoteException {
		InformationSourceManager ism = system.getInformationSourceManager();
		InformationSource[] is = ism.getAllInformationSources();
		String[] names = new String[is.length];
		for(int i=0; i < is.length; i++)
			names[i] = is[i].getName();
		return names;
	}

	public String[] getTableNames(String infosource) throws RemoteException {
		try {
			InformationSourceManager ism = system.getInformationSourceManager();
			InformationSource is = ism.getInformationSource(infosource);

			return is.getAllTableNames();
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public Schema getSchema(String tablename) throws RemoteException{
		try {
			InformationSourceManager ism = system.getInformationSourceManager();
			InformationSource is = ism.getSourceFromTableName(tablename);
			return is.getSchema(tablename);
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public String[] getAllTableNames() throws RemoteException {
		try {
			InformationSourceManager ism = system.getInformationSourceManager();
			return ism.getAllTableNames();
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public Schema[] getAllSchemata() throws RemoteException {
		try {
			InformationSourceManager ism = system.getInformationSourceManager();
			return ism.getAllSchemas();
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public Query[] getQueries() throws RemoteException {
		return (Query[])(qlist.toArray(new Query[0]));
	}

	public ConnectionInfo[] getAllConnectionInfo() throws RemoteException {
		InformationSourceManager ism = system.getInformationSourceManager();
		InformationSource[] is = ism.getAllInformationSources();
		ArrayList<ConnectionInfo> clist = new ArrayList<ConnectionInfo>();
		for(int i=0; i < is.length; i++){
			if(is[i] instanceof RemoteStreamServerWrapper)
				clist.add(new ConnectionInfo((RemoteStreamServerWrapper)is[i]));
		}
		return (ConnectionInfo[])(clist.toArray(new ConnectionInfo[0]));
	}

	public void notifyQueryRegistered(Query q) throws RemoteException {
		qlist.add(q);
		for(DistributedSystemManager dsm : dsmlist)
			dsm.queryRegistered(nodename, q);
	}

	public void notifyQueryDeleted(Query q) throws RemoteException {
		qlist.remove(q);
		for(DistributedSystemManager dsm : dsmlist)
			dsm.queryDeleted(nodename, q);
	}

	public void notifyDataDistributedTo(long timestamp, Set queryids) throws RemoteException {
		for(DistributedSystemManager dsm : dsmlist)
			dsm.dataDistributedTo(nodename, timestamp, queryids);
	}

	public void notifyDataReceived(long timestamp, String master) throws RemoteException {
		for(DistributedSystemManager dsm : dsmlist)
			dsm.dataReceived(nodename, timestamp, master);
	}

	public void notifyInformationSourceAdded(String wrappername, String[] tablenames) throws RemoteException {
		for(DistributedSystemManager dsm : dsmlist)
			dsm.informationSourceAdded(nodename, wrappername, tablenames);
	}

	public void notifyInformationSourceDeleted(String wrappername) throws RemoteException{
		for(DistributedSystemManager dsm : dsmlist)
			dsm.informationSourceDeleted(nodename, wrappername);
	}

	public void notifyConnectionEstablished(ConnectionInfo conn) throws RemoteException {
		for(DistributedSystemManager dsm : dsmlist)
			dsm.connectionEstablished(nodename, conn);
	}

	public void notifyConnectionClosed(ConnectionInfo conn) throws RemoteException {
		for(DistributedSystemManager dsm : dsmlist)
			dsm.connectionClosed(nodename, conn);
	}

	public void notifyTableCreated(String wrappername, String tablename) throws RemoteException{
		for(DistributedSystemManager dsm : dsmlist)
			dsm.tableCreated(nodename, wrappername, tablename);
	}

	public void notifyTableDropped(String wrappername, String tablename) throws RemoteException {
		for(DistributedSystemManager dsm : dsmlist)
			dsm.tableDropped(nodename, wrappername, tablename);
	}

	public void snapshotCreated() throws RemoteException {
		InformationSourceManager ism = system.getInformationSourceManager();
		for(InformationSource is : ism.getAllInformationSources()){
			if(is instanceof RemoteStreamServerWrapper){
				RemoteStreamServerWrapper rssw = (RemoteStreamServerWrapper)is;
				try {
					rssw.sendAcknowledge();
				} catch(StreamSpinnerException sse){
					sse.printStackTrace();
				}
			}
			else if(is instanceof DurableRemoteStreamServerWrapper){
				DurableRemoteStreamServerWrapper drssw = (DurableRemoteStreamServerWrapper)is;
				try {
					drssw.snapshotCreated();
				} catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	}

	public void snapshotCopied() throws RemoteException {
		InformationSourceManager ism = system.getInformationSourceManager();
		for(InformationSource is : ism.getAllInformationSources()){
			if(is instanceof DurableRemoteStreamServerWrapper){
				DurableRemoteStreamServerWrapper drssw = (DurableRemoteStreamServerWrapper)is;
				try {
					drssw.snapshotCopied();
				} catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	}

	public void snapshotRestored() throws RemoteException {
		;
	}
}
