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
package org.streamspinner.gui;

import org.streamspinner.StreamSpinnerMainSystem;
import org.streamspinner.SystemManager;
import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import org.streamspinner.query.Query;
import org.streamspinner.query.CQFileFilter;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.Schema;
import java.util.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.filechooser.*;
import java.io.*;

public class SystemManagerSwing extends JFrame implements SystemManager, ActionListener {

	private StreamSpinnerMainSystem system;
	private JDesktopPane workspace;
	private TableTree ttree;
	private QueryInfoPanel qpanel;

	private class ManagerWindowAdapter extends WindowAdapter {
		public void windowClosing(WindowEvent we){
			doShutdown();
		}
	}

	public SystemManagerSwing(StreamSpinnerMainSystem ssms){
		super();
		addWindowListener(new ManagerWindowAdapter());

		setTitle("StreamSpinner");
		setExtendedState(JFrame.MAXIMIZED_BOTH);
		setSize(800, 600);

		setJMenuBar(new ManagerMenuBar(this));

		Container c = getContentPane();
		c.setLayout(new BorderLayout());

		workspace = new JDesktopPane();
		c.add(workspace, BorderLayout.CENTER);

		qpanel = new QueryInfoPanel();
		qpanel.setJDesktopPane(workspace);
		c.add(qpanel, BorderLayout.NORTH);

		system = ssms;
		try {
			system.setSystemManager(this);
			InformationSourceManager ism = system.getInformationSourceManager();
			ttree = new TableTree(ism);
			ttree.setSize(new Dimension((int)(0.2 * c.getWidth()), c.getHeight()));
			c.add(ttree, BorderLayout.WEST);
			ttree.setJDesktopPane(workspace);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public void actionPerformed(ActionEvent ae){
		String command = ae.getActionCommand();
		if(command.equals(ManagerMenuBar.LABEL_SHUTDOWN)){
			doShutdown();
		}
		else if(command.equals(ManagerMenuBar.LABEL_CQTERMINAL)){
			createCQTerminal();
		}
		else if(command.equals(ManagerMenuBar.LABEL_OPEN)){
			createCQTerminalFromFile();
		}
		else if(command.equals(ManagerMenuBar.LABEL_ADDWRAPPER)){
			String wrapperdir = system.getCurrentResourceBundle().getString(StreamSpinnerMainSystem.PROPERTY_WRAPPERDIR);
			WrapperCreator wc = new WrapperCreator(system.getInformationSourceManager(), wrapperdir);
			addToWorkspace(wc, new Dimension(500, 400));
		}
		else if(command.equals(ManagerMenuBar.LABEL_DELETEWRAPPER)){
			WrapperRemover wr = new WrapperRemover(system.getInformationSourceManager());
			addToWorkspace(wr, new Dimension(500, 400));
		}
	}

	private void doShutdown(){
		try {
			if(system != null)
				system.shutdown();
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			System.exit(0);
		}
	}

	private CQTerminal createCQTerminal(){
		String url = system.getCurrentResourceBundle().getString(StreamSpinnerMainSystem.PROPERTY_RMILOCATION);
		CQTerminal term = new CQTerminal(url);
		addToWorkspace(term, new Dimension(500, 400));
		return term;
	}

	private void createCQTerminalFromFile(){
		ResourceBundle rb = system.getCurrentResourceBundle();
		JFileChooser ch = new JFileChooser(rb.getString(StreamSpinnerMainSystem.PROPERTY_QUERYDIR));
		ch.setFileFilter(new CQFileFilter());

		int status = ch.showOpenDialog(this);

		if(status == JFileChooser.APPROVE_OPTION){
			CQTerminal term = createCQTerminal();
			term.openFile(ch.getSelectedFile().getPath());
		}
	}

	private void addToWorkspace(JInternalFrame f, Dimension size){
		workspace.add(f);
		if(size != null)
			f.setSize(size);
		f.setVisible(true);
		f.setLocation(new Point(10 * (workspace.getAllFrames().length -1), 10 * (workspace.getAllFrames().length -1 )));
		f.toFront();
	}


	public void queryRegistered(Query q){
		qpanel.queryRegistered(q);
	}

	public void queryDeleted(Query q){
		qpanel.queryDeleted(q);
	}

	public void dataDistributedTo(long timestamp, Set queryids, TupleSet ts){
		;
	}

	public void dataReceived(long timestamp, String source, TupleSet ts){
		ttree.showAnimation(source);
	}

	public void executionPerformed(long executiontime, String master, long duration, long delay){
		;
	}

	public void startCacheConsumer(long timestamp, OperatorGroup og, double ratio) {
		;
	}

	public void endCacheConsumer(long timestamp, OperatorGroup og, double ratio) {
		;
	}

	public void informationSourceAdded(InformationSource is){
		ttree.addInformationSource(is);
	}

	public void informationSourceDeleted(InformationSource is){
		ttree.deleteInformationSource(is);
	}

	public void tableCreated(String wrappername, String tablename, Schema schema){
		ttree.addTable(wrappername, tablename);
	}

	public void tableDropped(String wrappername, String tablename){
		ttree.deleteTable(wrappername, tablename);
	}
}
