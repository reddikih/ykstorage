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
package org.streamspinner.distributed.gui;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import org.streamspinner.*;
import org.streamspinner.distributed.*;
import org.streamspinner.engine.*;
import org.streamspinner.query.*;


public class DistributedSystemMonitor extends JFrame implements ActionListener {

	private CircleLayoutPanel panel;
	private InternalDistributedSystemManager idsm;
	private Vector<NodeManager> nmlist;

	private class InternalWindowAdapter extends WindowAdapter {
		public void windowClosing(WindowEvent we){
			try {
				stop();
			} catch(StreamSpinnerException e){
				e.printStackTrace();
			} finally {
				System.exit(0);
			}
		}
	}

	private class InternalDistributedSystemManager extends UnicastRemoteObject implements DistributedSystemManager {
		public InternalDistributedSystemManager() throws RemoteException {
			super();
		}

		public void queryRegistered(String nodename, Query q) throws RemoteException {
			panel.addQueryLabel(nodename, q);
		}

		public void queryDeleted(String nodename, Query q) throws RemoteException {
			panel.deleteQueryLabel(nodename, q);
		}

		public void dataDistributedTo(String nodename, long timestamp, Set<Object> queryids) throws RemoteException {
			panel.showQueryExecution(nodename, timestamp, queryids);
		}

		public void dataReceived(String nodename, long timestamp, String master) throws RemoteException {
			panel.showDataArrival(nodename, timestamp, master);
		}

		public void informationSourceAdded(String nodename, String wrappername, String[] tablenames) throws RemoteException {
			panel.addInformationSourceLabel(nodename, wrappername, tablenames);
		}

		public void informationSourceDeleted(String nodename, String wrappername) throws RemoteException {
			panel.deleteInformationSourceLabel(nodename, wrappername);
		}

		public void connectionEstablished(String nodename, ConnectionInfo conn) throws RemoteException {
			panel.addConnectionLine(nodename, conn);
		}

		public void connectionClosed(String nodename, ConnectionInfo conn) throws RemoteException {
			panel.deleteConnectionLine(nodename, conn);
		}

		public void tableCreated(String nodename, String wrappername, String tablename) throws RemoteException {
			panel.addTableLabel(nodename, wrappername, tablename);
		}

		public void tableDropped(String nodename, String wrappername, String tablename) throws RemoteException {
			panel.deleteTableLabel(nodename, wrappername, tablename);
		}

	}

	public DistributedSystemMonitor(){
		super();
		try {
			idsm = new InternalDistributedSystemManager();
		} catch(RemoteException re){
			re.printStackTrace();
			idsm = null;
		}
		nmlist = new Vector<NodeManager>();

		JMenuBar bar = new JMenuBar();
		JMenu menu = new JMenu("Node");
		JMenuItem item = new JMenuItem("Add node");
		item.addActionListener(this);
		menu.add(item);
		bar.add(menu);
		setJMenuBar(bar);

		Container c = getContentPane();
		panel = new CircleLayoutPanel();
		c.add(panel);

		addWindowListener(new InternalWindowAdapter());
		setTitle("Monitoring system for distributed StreamSpinner");
		setExtendedState(JFrame.MAXIMIZED_BOTH);
		setSize(800, 600);
	}

	public void actionPerformed(ActionEvent ae){
		String command = ae.getActionCommand();
		if(command.equals("Add node")){
			NodeCreatorDialog ncd = new NodeCreatorDialog(this);
			ncd.setVisible(true);
		}
	}

	public void addNodeManager(NodeManager nm){
		try {
			nmlist.add(nm);
			nm.addDistributedSystemManager(idsm);
			panel.addNode(nm);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public void stop() throws StreamSpinnerException {
		try {
			for(NodeManager nm : nmlist){
				nm.removeDistributedSystemManager(idsm);
			}
		} catch(RemoteException re){
			throw new StreamSpinnerException(re);
		}
	}

	public static void main(String[] args){
		DistributedSystemMonitor dsm = new DistributedSystemMonitor();
		dsm.setVisible(true);
	}

}
