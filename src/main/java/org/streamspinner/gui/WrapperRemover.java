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
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import java.awt.Container;
import java.awt.BorderLayout;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JInternalFrame;
import javax.swing.JButton;
import javax.swing.JList;
import javax.swing.JPanel;

public class WrapperRemover extends JInternalFrame implements ActionListener {

	private static final String LABEL_DELETE = "Delete";
	private static final String LABEL_CLOSE= "close";

	private InformationSourceManager manager;
	private JList list;

	public WrapperRemover(InformationSourceManager ism){
		super("Wrapper Remover", true, true, true, true);
		manager = ism;

		Container c = getContentPane();
		c.setLayout(new BorderLayout());

		InformationSource[] sources = manager.getAllInformationSources();
		String[] names = new String[sources.length];
		for(int i=0; i < sources.length; i++)
			names[i] = sources[i].getName();

		list = new JList(names);
		c.add(list, BorderLayout.CENTER);

		JPanel buttonarea = new JPanel();

		JButton bdelete = new JButton(LABEL_DELETE);
		bdelete.addActionListener(this);
		buttonarea.add(bdelete);

		JButton bclose = new JButton(LABEL_CLOSE);
		bclose.addActionListener(this);
		buttonarea.add(bclose);

		c.add(buttonarea, BorderLayout.SOUTH);
	}

	public void actionPerformed(ActionEvent ae){
		String command = ae.getActionCommand();
		if(command.equals(LABEL_DELETE)){
			deleteWrapper();
		}
		else if(command.equals(LABEL_CLOSE)){
			doClose();
		}
	}

	private void doClose(){
		manager = null;
		dispose();
	}

	private void deleteWrapper(){
		Object[] items = list.getSelectedValues();
		if(items != null && items.length > 0){
			try {
				for(int i=0; i < items.length; i++)
					manager.removeInformationSource(items[i].toString());
			} catch(StreamSpinnerException e){
				showException(e);
			}
		}
		doClose();
	}

	public void showException(Exception e){
		ExceptionDialog message = new ExceptionDialog(e);
		message.setLocationRelativeTo(this);
		message.setVisible(true);
	}

}
