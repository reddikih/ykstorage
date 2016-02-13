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

import org.streamspinner.*;
import org.streamspinner.connection.*;
import org.streamspinner.wrapper.*;
import org.streamspinner.optimizer.*;
import org.streamspinner.query.*;
import org.streamspinner.engine.*;
import java.net.*;
import java.rmi.server.*;
import java.rmi.*;
import java.util.*;
import java.io.*;


public class StreamSpinnerMainSystemImpl extends UnicastRemoteObject implements StreamSpinnerMainSystem, RemoteStreamServer {

	private InformationSourceManager ism;
	private Mediator mediator;
	private SystemManager manager;
	private ArchiveManager archive;
	private ResourceBundle resource;
	private QueryOptimizer optimizer;
	private LogManager log;
	private Hashtable<Long, ConnectionAdaptor> connections;
	private Vector<RemoteStreamServer> backups;
	private RemoteStreamServer primary;

	private String rmilocation;
	private String wrapperdir;
	private String functiondir;
	private String optimizationmode;
	private String primarylocation;
	private boolean useoptimizer;
	private double cacheconsumerthreshold;

	public StreamSpinnerMainSystemImpl() throws RemoteException {
		super();
		try {
			loadProperties();

			// init InformationsourceManager
			ism = new InformationSourceManagerImpl(wrapperdir);
			ism.initAllInformationSources();

			// init LogManager
			log = new LogManagerImpl();
			log.init();

			// init Function
			Function.loadFunctions(functiondir);

			// init Mediator
			mediator = new MediatorImpl(ism);
			mediator.setLogManager(log);

			// init Optimizer
			if(useoptimizer == true){
				optimizer = new QueryOptimizerImpl(optimizationmode);
				optimizer.setCacheConsumerThreshold(cacheconsumerthreshold);
			}
			else
				optimizer = new DummyQueryOptimizer();
			optimizer.setLogManager(log);
			optimizer.setMediator(mediator);
			optimizer.init();

			// init other members
			manager = null;
			archive = null;
			backups = new Vector<RemoteStreamServer>();
			primary = null;
			connections = new Hashtable<Long, ConnectionAdaptor>();

			// if 'primarylocation' is given, the server become a backup of the server running on 'primarylocation'.
			if(primarylocation != null && (! primarylocation.equals("")) && (! primarylocation.equals(rmilocation))){
				try {
					primary = (RemoteStreamServer)(Naming.lookup(primarylocation));
					primary.addBackupServer(this);
					Thread t = new BackupThread(this);
					t.start();
				} catch(Exception e){
					throw new RemoteException("", e);
				}
			}

		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}


	private void loadProperties() throws StreamSpinnerException {
		resource = ResourceBundle.getBundle(PROPERTY_FILENAME);
		if(resource == null)
			throw new StreamSpinnerException("Cannot load properties");

		wrapperdir = resource.getString(PROPERTY_WRAPPERDIR);
		if(wrapperdir == null)
			throw new StreamSpinnerException("'"+PROPERTY_WRAPPERDIR+"' is not set");

		functiondir = resource.getString(PROPERTY_FUNCTIONDIR);
		if(functiondir == null)
			throw new StreamSpinnerException("'"+PROPERTY_FUNCTIONDIR+"' is not set");

		useoptimizer = (new Boolean(resource.getString(PROPERTY_USEOPTIMIZER))).booleanValue();
		if(useoptimizer == true){
			optimizationmode = resource.getString(PROPERTY_OPTIMIZATIONMODE);
			if(optimizationmode == null)
				throw new StreamSpinnerException("'"+PROPERTY_OPTIMIZATIONMODE+ "' is not set");

			String strcthreshold = resource.getString(PROPERTY_CACHECONSUMERTHRESHOLD);
			if(strcthreshold == null)
				throw new StreamSpinnerException("'"+PROPERTY_CACHECONSUMERTHRESHOLD + "' is not set");
			try {
				cacheconsumerthreshold = Double.parseDouble(strcthreshold);
			} catch(NumberFormatException nfe){
				throw new StreamSpinnerException(nfe);
			}
		}

		rmilocation = resource.getString(PROPERTY_RMILOCATION);
		if(rmilocation == null)
			throw new StreamSpinnerException("'"+PROPERTY_RMILOCATION+"' is not set");

		primarylocation = resource.getString(PROPERTY_PRIMARYLOCATION);
	}


	public ResourceBundle getCurrentResourceBundle(){
		return resource;
	}

	public InformationSourceManager getInformationSourceManager(){
		return ism;
	}

	public ArchiveManager getArchiveManager(){
		return archive;
	}

	public SystemManager getSystemManager(){
		return manager;
	}

	public void setArchiveManager(ArchiveManager amgr){
		archive = amgr;
		mediator.setArchiveManager(archive);
	}

	public void setSystemManager(SystemManager mgr){
		manager = mgr;
		mediator.setSystemManager(mgr);
		optimizer.setSystemManager(mgr);
		ism.setSystemManager(mgr);
	}

	public void setResourceBundle(ResourceBundle rb){
		resource = rb;
	}

	public void start() throws StreamSpinnerException {
		ism.startAllInformationSources();
		log.start();
		optimizer.start();
		mediator.start();
		if(archive != null){
			archive.init();
			archive.start();
		}
		try {
			if(System.getSecurityManager() == null)
				System.setSecurityManager(new RMISecurityManager());
			Naming.rebind(rmilocation, this);
		} catch(MalformedURLException me){
			throw new StreamSpinnerException(me);
		} catch(RemoteException re){
			throw new StreamSpinnerException(re);
		}
	}

	public void shutdown() throws StreamSpinnerException {
		try {
			ism.stopAllInformationSources();
			optimizer.stop();
			mediator.stop();
			log.stop();
			if(archive != null)
				archive.stop();
			Naming.unbind(rmilocation);

			if(primary != null){
				primary.removeBackupServer(this);
				primary = null;
			}
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}

	public long startQuery(String cq, Connection conn) throws RemoteException {
		try {
			DAG d = parseQuery(cq);
			d.clearQueryID();
			Query q = d.toQueries()[0];
			d.addQueryID(q);
			q.setOwner(conn);
			d.addQueryID(q);

			ConnectionAdaptor ca = new ConnectionAdaptor(q, conn, d);
			connections.put(ca.getID(), ca);

			optimizer.add(q, d, ca);

			if(manager != null)
				manager.queryRegistered(q);

			for(RemoteStreamServer rss : backups){
				try {
					long cid = rss.startQuery(cq, conn);
					if(ca.getID() != cid)
						throw new RemoteException("ID is not synchronized among servers");
				} catch(RemoteException re){
					System.err.println(re.getMessage());
				}
			}

			return ca.getID();
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	protected DAG parseQuery(String cq) throws StreamSpinnerException {
		if(cq.startsWith("<?xml"))
			return (new XMLDAGBuilder()).createDAG(cq);
		else 
			return DAGBuilder.getInstance().createDAG(cq);
	}

	public void stopQuery(long cid) throws RemoteException {
		try {
			if(! connections.containsKey(cid))
				return;
			ConnectionAdaptor ca = (ConnectionAdaptor)(connections.get(cid));
			optimizer.remove(ca.getQuery(), ca);

			connections.remove(cid);

			if(manager != null)
				manager.queryDeleted(ca.getQuery());

			for(RemoteStreamServer rss : backups){
				try {
					rss.stopQuery(cid);
				} catch(RemoteException re){
					System.err.println(re.getMessage());
				}
			}
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public void updateConnection(long cid, Connection conn) throws RemoteException {
		if(! connections.containsKey(cid))
			throw new RemoteException("Unknown connection id:" + cid);
		ConnectionAdaptor ca = connections.get(cid);
		ca.setConnection(conn);

		for(RemoteStreamServer rss :backups){
			try {
				rss.updateConnection(cid, conn);
			} catch(RemoteException re){
				System.err.println(re.getMessage());
			}
		}
	}

	public void receiveAcknowledge(long cid, long seqno) throws RemoteException {
		if(! connections.containsKey(cid))
			throw new RemoteException("Unknown connection id:" + cid);
		ConnectionAdaptor ca = connections.get(cid);
		ca.clearBuffer(seqno);
	}

	public Schema[] getAllSchemas() throws RemoteException {
		try {
			return ism.getAllSchemas();
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public void addInformationSource(String confstr) throws RemoteException {
		try {
			ism.addInformationSource(confstr);
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public void addInformationSource(InformationSource is) throws StreamSpinnerException {
		ism.addInformationSource(is);
	}

	public void removeInformationSource(String name) throws RemoteException {
		try {
			ism.removeInformationSource(name);
		} catch(StreamSpinnerException sse){
			throw new RemoteException("", sse);
		}
	}

	public void addBackupServer(RemoteStreamServer rss) throws RemoteException {
		BackupServerConfigurator.copyWrapperConfigToBackupServer(this, rss);
		backups.add(rss);
	}

	public void removeBackupServer(RemoteStreamServer rss) throws RemoteException {
		backups.remove(rss);
	}

	public long receivePing() throws RemoteException {
		return System.currentTimeMillis();
	}


	private class BackupThread extends Thread {
		private RemoteStreamServer parent;

		private BackupThread(RemoteStreamServer rss){
			parent = rss;
		}

		public void run(){
			try {
				Thread.sleep(5000);
				while(primary != null){
					try {
						//primary.receivePing();
						InternalState sis = primary.getRemoteStreamServerInternalState();
						setInternalState(sis);
					} catch(RemoteException re){
						primary = null;
						for(ConnectionAdaptor ca : connections.values())
							ca.getConnection().reconnect(parent);
						return;
					}
					Thread.sleep(5000);
				}
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof SystemInternalState;
	}

	public InternalState getInternalState() {
		SystemInternalState rval = new SystemInternalState();
		rval.setMediatorInternalState(mediator.getInternalState());
		rval.setInformationSourceManagerInternalState(ism.getInternalState());
		for(Long id : connections.keySet()){
			ConnectionAdaptor ca = connections.get(id);
			rval.addConnectionAdaptorInternalState(ca.getInternalState());
		}
		return rval;
	}

	public InternalState getRemoteStreamServerInternalState() throws RemoteException {
		return getInternalState();
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(! (isAcceptable(state)))
			throw new StreamSpinnerException("Unknown state object is given");
		SystemInternalState sstate = (SystemInternalState)state;
		mediator.setInternalState(sstate.getMediatorInternalState());
		ism.setInternalState(sstate.getInformationSourceManagerInternalState());
		for(ConnectionAdaptorInternalState cais : sstate.getConnectionAdaptorInternalStates()){
			if(connections.containsKey(cais.getConnectionID())){
				ConnectionAdaptor ca = connections.get(cais.getConnectionID());
				ca.setInternalState(cais);
			}
		}
	}

}
