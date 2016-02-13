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

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.DataTypes;
import org.streamspinner.query.Query;
import org.streamspinner.connection.DefaultCQRowSet;
import org.streamspinner.connection.CQRowSet;
import org.streamspinner.connection.CQRowSetMetaData;
import org.streamspinner.connection.CQRowSetEvent;
import org.streamspinner.connection.CQRowSetListener;
import org.streamspinner.connection.CQControlEvent;
import org.streamspinner.connection.CQControlEventListener;
import org.streamspinner.connection.CQException;
import javax.swing.*;
import javax.swing.event.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.io.*;


public class CQTerminal extends JInternalFrame implements ActionListener, CQRowSetListener, CQControlEventListener {

	private CQRowSet rs;
	private DefaultTableModel model;
	private JTable table;
	private JTextArea text;
	private JButton regist;
	private JScrollBar scroll;
	private boolean dummyschema;
	private boolean running;

	public CQTerminal(String url){
		super("CQTerminal", true, true, true, true);
		Container c = getContentPane();
		c.setLayout(new BoxLayout(c, BoxLayout.Y_AXIS));

		rs = new DefaultCQRowSet();
		rs.setUrl(url);
		rs.addCQRowSetListener(this);
		rs.addCQControlEventListener(this);
		running = false;

		JPanel queryfield = new JPanel();
		queryfield.setLayout(new BoxLayout(queryfield, BoxLayout.X_AXIS));

		text = new JTextArea(8, 30);
		JScrollPane textscroll = new JScrollPane(text);
		queryfield.add(textscroll);

		regist = new JButton("Regist");
		regist.addActionListener(this);
		queryfield.add(regist);

		c.add(queryfield);

		dummyschema = true;
		Object[] defaultcol = { " ", " ", " " };
		model = new DefaultTableModel(defaultcol, 0);
		table = new JTable(model);
		JScrollPane tablescroll = new JScrollPane(table, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		tablescroll.setWheelScrollingEnabled(true);
		scroll = tablescroll.getVerticalScrollBar();

		c.add(tablescroll);

		addInternalFrameListener(new CQTerminalAdapter());
	}

	private class CQTerminalAdapter extends InternalFrameAdapter {
		public void internalFrameClosing(InternalFrameEvent we){
			doClose();
		}
	}

	private void doStart(){
		String qstr = text.getText();
		regist.setEnabled(false);
		try {
			rs.setCommand(qstr);
			rs.start();
			running = true;
		} catch (CQException ce){
			showException(ce);
			doStop();
			regist.setEnabled(true);
		}
	}

	private void doStop(){
		try {
			rs.stop();
		} catch(CQException ce){
			ce.printStackTrace();
		}
		running = false;
	}

	private void doClose(){
		if(running == true)
			doStop();
		table = null;
		model = null;
		text = null;
	}

	public void actionPerformed(ActionEvent ae){
		String command = ae.getActionCommand();
		if(command.equals("Regist") && running == false){
			doStart();
		}
	}

	public void dataDistributed(CQRowSetEvent re){
		try {
			CQRowSet rs = (CQRowSet)(re.getSource());
			while(rs.next()){
				CQRowSetMetaData rsmd = rs.getMetaData();
				if(dummyschema == true){
					Vector column = new Vector();
					for(int i=0; i < rsmd.getColumnCount(); i++)
						column.add(rsmd.getColumnName(i+1));
					model = new DefaultTableModel(column, 0);
					table.setModel(model);
					dummyschema = false;
				}
				Vector tuple = new Vector();
				for(int i=1; i <= rsmd.getColumnCount(); i++){
					tuple.add(rs.getString(i));
				}
				model.addRow(tuple);
			}
			if(model.getRowCount() > 0){
				table.changeSelection(model.getRowCount() -1, 1, false, false);
				repaint();
			}
		} catch (CQException ce){
			showException(ce);
			doStop();
		}
	}

	public void errorOccurred(CQControlEvent event) {
		showException(event.getCQException());
		doStop();
	}

	public void showException(Exception e){
		ExceptionDialog message = new ExceptionDialog(e);

		message.setLocationRelativeTo(this);
		message.setVisible(true);
	}

	public void openFile(String filename){
		try {
			FileReader fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);

			StringBuffer sb = new StringBuffer();
			for(String line=br.readLine(); line != null ; line=br.readLine()){
				sb.append(line);
				sb.append("\n");
			}
			text.setText(sb.toString());
		} catch(IOException e){
			e.printStackTrace();
		}
	}

}
