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

import org.streamspinner.query.Query;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;

public class QueryInfoPanel extends JPanel implements ActionListener {

	private JLabel numOfQuery;
	private DefaultComboBoxModel queryids;
	private JComboBox box;
	private Hashtable qtable;
	private JDesktopPane workspace;
	private JButton view;

	public QueryInfoPanel(){
		qtable = new Hashtable();

		setLayout(new FlowLayout());
		numOfQuery = new JLabel("0");
		numOfQuery.setForeground(Color.blue);
		add(numOfQuery);
		add(new JLabel(" queries are registered."));
		add(new JLabel("   "));

		queryids = new DefaultComboBoxModel();
		box = new JComboBox(queryids);
		add(box);

		view = new JButton("View Query");
		view.addActionListener(this);
		view.setEnabled(false);
		add(view);

	}

	public void queryRegistered(Query q){
		qtable.put(q.getID(), q);
		numOfQuery.setText("" + qtable.size());
		queryids.addElement(q.getID());
		view.setEnabled(true);
	}

	public void queryDeleted(Query q){
		if(qtable.containsKey(q.getID())){
			qtable.remove(q.getID());
			numOfQuery.setText("" + qtable.size());
			queryids.removeElement(q.getID());
			if(qtable.size() == 0){
				view.setEnabled(false);
			}
		}
	}

	public void setJDesktopPane(JDesktopPane d){
		workspace = d;
	}

	public void actionPerformed(ActionEvent ae){
		String command = ae.getActionCommand();
		Object qid = box.getSelectedItem();
		if(qid == null || qtable.containsKey(qid) == false || workspace == null)
			return;

		Query q = (Query)(qtable.get(qid));

		if(command.equals(view.getActionCommand())){
			JInternalFrame jf = new JInternalFrame("Query " + qid.toString(), true, true, false, false);
			Container c = jf.getContentPane();
			JTextArea ta = new JTextArea(q.toString());
			ta.setEditable(false);
			JScrollPane scroll = new JScrollPane(ta);
			c.add(scroll);
			jf.setSize(new Dimension(500, 150));
			jf.setVisible(true);
			workspace.add(jf);
			jf.setLocation(new Point(10 * (workspace.getAllFrames().length -1), 10 * (workspace.getAllFrames().length -1)));
			jf.moveToFront();
		}
	}

}
