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
package org.streamspinner.connection;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Iterator;
import java.util.Vector;
import java.util.Enumeration;
import java.util.HashSet;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.DataTypes;
import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.Schema;


public class DefaultCQRowSet implements CQRowSet {

	private class NotificationThread extends Thread {
		private Vector tuples;
		private NotificationThread(Vector v){
			tuples = v;
		}
		public void run(){
			notifyDataDistributed(tuples);
		}
	}

	private class ControlEventNotificationThread extends Thread {
		private CQException cqe;
		private ControlEventNotificationThread(CQException e){
			cqe = e;
		}
		public void run(){
			notifyControlEvent(cqe);
		}
	}

	private class AddressMonitoringThread extends Thread {
		private boolean running = false;
		private long sleep=5000;

		public void setSleepTime(long s){
			sleep = s;
		}

		public void run(){
			running = true;
			try {
				HashSet<InetAddress> base = getCurrentAddresses();
				while(running == true){
					HashSet<InetAddress> current = getCurrentAddresses();
					if(! base.equals(current) && conn != null){
						DefaultConnector tmp = conn;
						conn = conn.updateConnection();
						tmp.dispose();
						base = current;
					}
					Thread.sleep(sleep);
				}
			} catch(Exception e){
				e.printStackTrace();
			}
		}

		private HashSet<InetAddress> getCurrentAddresses() throws Exception {
			HashSet<InetAddress> rval = new HashSet<InetAddress>();
			Enumeration<NetworkInterface> eni = NetworkInterface.getNetworkInterfaces();
			while(eni.hasMoreElements()){
				NetworkInterface ni = eni.nextElement();
				Enumeration<InetAddress> eia = ni.getInetAddresses();
				while(eia.hasMoreElements())
					rval.add(eia.nextElement());
			}
			return rval;
		}

		public void exit(){
			running = false;
		}
	}

	private class DefaultConnector extends UnicastRemoteObject implements Connection {
		private RemoteStreamServer server = null;
		private Schema scm = null;
		private String url = null;
		private String command = null;
		private long cid = -1;
		private Long seq = new Long(-1);

		DefaultConnector(String url, String command) throws RemoteException {
			this.url = url;
			this.command = command;
			if (System.getSecurityManager() == null)
				System.setSecurityManager(new RMISecurityManager());
			try {
				server = (RemoteStreamServer) Naming.lookup(url);
			} catch (Exception e) {
				throw new RemoteException("", e);
			}
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
					Thread t = new NotificationThread(du.getTuples());
					t.start();
					return seq;
				}
			}
		}

		public void receiveRecoveredUnits(long newseq, DeliveryUnit[] units){
			synchronized(seq){
				for(DeliveryUnit du : units){
					if(du.getSequenceNumber() > seq){
						Thread t = new NotificationThread(du.getTuples());
						t.start();
					}
				}
				if(newseq > seq)
					seq = newseq;
			}
		}

		public void receiveSchema(Schema s) throws RemoteException {
			scm = s;
		}

		public void receiveCQException(CQException cqe) throws RemoteException {
			Thread t = new ControlEventNotificationThread(cqe);
			t.start();
		}

		public Schema getSchema(){
			return scm;
		}

		public void start() throws RemoteException {
			cid = server.startQuery(command, this);
		}

		public void stop() throws RemoteException {
			server.stopQuery(cid);
		}

		protected void dispose(){
			server = null;
			scm = null;
			command = null;
			url = null;
		}

		public DefaultConnector updateConnection() throws RemoteException {
			DefaultConnector rval = new DefaultConnector(url, command);
			rval.cid = cid;
			rval.scm = scm;
			rval.server.updateConnection(cid, rval);
			return rval;
		}

		public void sendAcknowledge() throws RemoteException {
			synchronized(seq){
				if(autoacknowledge == true)
					server.receiveAcknowledge(cid, seq);
				else {
					long margin = 30;
					server.receiveAcknowledge(cid, seq - margin);
				}
			}
		}

		public void reconnect(RemoteStreamServer rss) throws RemoteException {
			server = rss;
		}
	}

	class DefaultCQRowSetMetaData implements CQRowSetMetaData {

		private Schema s = null;

		DefaultCQRowSetMetaData(Schema s) {
			this.s = s;
		}

		public int getColumnCount() throws CQException {
			return s.size();
		}

		public String getColumnName(int column) throws CQException {
			return s.getAttributeName(column - 1);
		}

		public String getColumnTypeName(int column) throws CQException {
			return s.getType(column - 1);
		}

	}

	private String command = null;

	private DefaultConnector conn = null;
	private int current = 0;
	private Tuple current_row = null;

	private Vector listeners = null;
	private Vector controllisteners = null;
	private String password = null;

	private Vector rows = null;
	private String url = null;
	private String username = null;

	private AddressMonitoringThread amthread = null;

	private boolean autoacknowledge = true;

	public DefaultCQRowSet() {
		listeners = new Vector();
		controllisteners = new Vector();

		useVariableInetAddress(true);
		setAutoAcknowledge(true);
	}

	public boolean absolute(int row) throws CQException {
		int abs = Math.abs(row);
		if (row > 0) {
			if (abs > rows.size()) {
				afterLast();
				return false;
			} else {
				current = row;
			}
		} else {
			if (abs == 0) {
				beforeFirst();
				return false;
			}
			if (abs > rows.size()) {
				beforeFirst();
				return false;
			} else {
				current = rows.size() - abs + 1;
			}
		}
		current_row = (Tuple) rows.elementAt(current - 1);
		return true;
	}

	public void addCQRowSetListener(CQRowSetListener listener) {
		listeners.add(listener);
	}

	public void addCQControlEventListener(CQControlEventListener listener){
		controllisteners.add(listener);
	}

	public void afterLast() throws CQException {
		current = rows.size() + 1;
		current_row = null;
	}

	public void beforeFirst() throws CQException {
		current = 0;
		current_row = null;
	}

	public void execute() throws CQException {
		start();
		stop();
	}

	public int findColumn(String columnName) throws CQException {
		Schema s = conn.getSchema();
		return s.getIndex(columnName) + 1;
	}

	public boolean first() throws CQException {
		return absolute(1);
	}

	public String getCommand() {
		return command;
	}

	public double getDouble(int columnIndex) throws CQException {
		if (current_row == null)
			throw new CQException("There is no cursor on resultset.");
		try {
			return current_row.getDouble(columnIndex - 1);
		} catch (StreamSpinnerException e) {
			throw new CQException(e);
		}
	}

	public double getDouble(String columnName) throws CQException {
		return getDouble(findColumn(columnName));
	}


	public long getLong(int columnIndex) throws CQException {
		if (current_row == null)
			throw new CQException("There is no cursor on resultset.");
		try {
			return current_row.getLong(columnIndex - 1);
		} catch (StreamSpinnerException e) {
			throw new CQException(e);
		}
	}

	public long getLong(String columnName) throws CQException {
		return getLong(findColumn(columnName));
	}

	public CQRowSetMetaData getMetaData() throws CQException {
		return new DefaultCQRowSetMetaData(conn.getSchema());
	}

	public Object getObject(int columnIndex) throws CQException {
		if (current_row == null)
			throw new CQException("There is no cursor on resultset.");
		String type = conn.getSchema().getType(columnIndex - 1);
		try {
			if (type.equals(DataTypes.STRING))
				return current_row.getString(columnIndex - 1);
			if (type.equals(DataTypes.LONG))
				return new Long(current_row.getLong(columnIndex - 1));
			if (type.equals(DataTypes.DOUBLE))
				return new Double(current_row.getDouble(columnIndex - 1));
			if (type.equals(DataTypes.OBJECT))
				return current_row.getObject(columnIndex - 1);
			if (DataTypes.isArray(type))
				return current_row.getObject(columnIndex - 1);
			throw new StreamSpinnerException("Unknown data type: " + type);
		} catch (StreamSpinnerException e) {
			throw new CQException(e);
		}
	}

	public Object getObject(String columnName) throws CQException {
		return getObject(findColumn(columnName));
	}

	public String getString(int columnIndex) throws CQException {
		if (current_row == null)
			throw new CQException("There is no cursor on resultset.");
		try {
			return current_row.getString(columnIndex - 1);
		} catch (StreamSpinnerException e) {
			throw new CQException(e);
		}
	}

	public String getString(String columnName) throws CQException {
		return getString(findColumn(columnName));
	}

	public String getUrl() {
		return url;
	}

	public boolean isAfterLast() throws CQException {
		return judgeCursorPosition(rows.size() + 1);
	}

	public boolean isBeforeFirst() throws CQException {
		return judgeCursorPosition(0);
	}

	public boolean isEmpty() throws CQException {
		if (rows != null) {
			return true;
		}
		return false;
	}

	public boolean isFirst() throws CQException {
		return judgeCursorPosition(1);
	}

	public boolean isLast() throws CQException {
		return judgeCursorPosition(rows.size());
	}

	private boolean judgeCursorPosition(int position) throws CQException {
		if (current == position) {
			return true;
		} else {
			return false;
		}
	}

	public boolean last() throws CQException {
		return absolute(-1);
	}

	public boolean next() throws CQException {
		return relative(1);
	}

	synchronized private void notifyDataDistributed(Vector tuples) {
		rows = tuples;
		current = 0;
		if (!listeners.isEmpty()) {
			CQRowSetEvent event = new CQRowSetEvent(this);
			Iterator iterator = listeners.iterator();
			while (iterator.hasNext()) {
				((CQRowSetListener) iterator.next()).dataDistributed(event);
			}
		}
		if(getAutoAcknowledge() == true){
			try {
				acknowledge();
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	synchronized private void notifyControlEvent(CQException cqe){
		CQControlEvent event = new CQControlEvent(cqe);
		Iterator iterator = controllisteners.iterator();
		while(iterator.hasNext())
			((CQControlEventListener)(iterator.next())).errorOccurred(event);
	}

	public boolean previous() throws CQException {
		return relative(-1);
	}

	public boolean relative(int rows) throws CQException {
		return absolute(current + rows);
	}

	public void removeCQRowSetListener(CQRowSetListener listener) {
		listeners.remove(listener);
	}

	public void removeCQControlEventListener(CQControlEventListener listener){
		controllisteners.remove(listener);
	}

	public void setCommand(String cmd) {
		command = cmd;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void start() throws CQException {
		if (url != null && command != null) {
			try {
				conn = new DefaultConnector(url, command);
				conn.start();
				if(amthread != null)
					amthread.start();
			} catch (RemoteException e) {
				throw new CQException(e);
			}
		} else {
			throw new CQException("Some parameters are not given, url="+url+", command="+command+"");
		}
	}

	public void stop() throws CQException {
		if (conn != null) {
			try {
				conn.stop();
				conn.dispose();
				conn = null;
				if(amthread != null){
					amthread.exit();
					amthread = new AddressMonitoringThread();
				}
			} catch (RemoteException e) {
				throw new CQException(e);
			}
		} else {
			throw new CQException("connection is not established");
		}
	}

	public void useVariableInetAddress(boolean flag){
		if(flag == true){
			if(amthread == null)
				amthread = new AddressMonitoringThread();
		}
		else {
			if(amthread != null){
				amthread.exit();
				amthread = null;
			}
		}
	}

	public void setAutoAcknowledge(boolean flag){
		autoacknowledge = flag;
	}

	public boolean getAutoAcknowledge(){
		return autoacknowledge;
	}

	public void acknowledge() throws CQException {
		if(conn == null)
			return;
		try {
			conn.sendAcknowledge();
		} catch(RemoteException re){
			throw new CQException(re);
		}
	}

}
