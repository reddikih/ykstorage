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

import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import org.streamspinner.StreamSpinnerException;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;

public class TableListPanel extends JPanel implements ActionListener {

	private InformationSourceManager manager;
	private ArrayList tables;
	private JDesktopPane workspace;
	private Hashtable boxes;

	public TableListPanel(InformationSourceManager ism) throws StreamSpinnerException {
		super();
		manager = ism;
		init();
	}

	private void init() throws StreamSpinnerException {
		boxes = new Hashtable();

		GridBagLayout gb = new GridBagLayout();
		GridBagConstraints gc = new GridBagConstraints();
		gc.fill = GridBagConstraints.BOTH;
		gc.gridwidth = 1;
		gc.gridheight = 1;

		setLayout(gb);

		String[] header = { "Wrapper", "Table" };
		for(int i=0; i < header.length; i++){
			JLabel l = new JLabel(header[i]);
			l.setFont(l.getFont().deriveFont(Font.BOLD));
			l.setForeground(Color.BLUE);
			l.setBackground(Color.RED);
			l.setHorizontalAlignment(SwingConstants.CENTER);
			l.setBorder(BorderFactory.createLineBorder(Color.LIGHT_GRAY, 1));
			gc.gridx = i % 2;
			gb.setConstraints(l, gc);
			add(l);
		}

		InformationSource[] sources = manager.getAllInformationSources();
		for(int i=0; sources != null && i < sources.length; i++){
			String[] tnames = sources[i].getAllTableNames();

			JLabel wname = new JLabel(sources[i].getName());
			wname.setFont(wname.getFont().deriveFont(Font.BOLD));
			wname.setBorder(BorderFactory.createLineBorder(Color.LIGHT_GRAY, 1));
			wname.setHorizontalAlignment(SwingConstants.CENTER);

			gc.gridx = 0;
			gc.gridheight = Math.max(tnames.length, 1);
			gc.gridwidth = 1;
			gb.setConstraints(wname, gc);
			add(wname);

			if(tnames == null || tnames.length == 0){
				tnames = new String[1];
				tnames[0] = "";
			}

			for(int j=0; tnames != null && j < tnames.length; j++){
				SourceBox b = new SourceBox(tnames[j]);
				b.addActionListener(this);
				b.setPreferredSize(new Dimension(150, 20));

				boxes.put(tnames[j], b);

				gc.gridx = 1;
				gc.gridwidth = GridBagConstraints.REMAINDER;
				gc.gridheight = 1;
				gb.setConstraints(b, gc);
				add(b);
			}
		}
	}

	public void setJDesktopPane(JDesktopPane d){
		workspace = d;
	}

	public void activateBox(String tablename){
		if(boxes.containsKey(tablename)){
			SourceBox b = (SourceBox)(boxes.get(tablename));
			b.activate();
		}
	}

	public void actionPerformed(ActionEvent ae){
		String table = ae.getActionCommand();
		if(table.equals(""))
			return;

		try {
			InformationSource is = manager.getSourceFromTableName(table);
			if(is != null && workspace != null){
				TableViewer tv = new TableViewer(is, table);
				//tv.setSize(500, 85);
				tv.setVisible(true);
				workspace.add(tv);
				tv.setLocation(new Point(10 * (workspace.getAllFrames().length -1), 10 * (workspace.getAllFrames().length -1)));
				tv.toFront();
			}
		} catch(StreamSpinnerException e){
			e.printStackTrace();
		}
	}

}
