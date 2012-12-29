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
import org.streamspinner.InformationSourceManager;
import java.awt.Container;
import java.awt.BorderLayout;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JInternalFrame;
import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JPanel;
import javax.swing.JFileChooser;

public class WrapperCreator extends JInternalFrame implements ActionListener {

	private static final String LABEL_CREATE = "Create";
	private static final String LABEL_LOAD = "Load";
	private static final String LABEL_CLEAR= "Clear";
	private static final String LABEL_CANCEL= "Cancel";

	private static final String DEFAULT_TEXT=
		"<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n" +
		"<wrapper name=\"\" class=\"\">\n" +
		"    <parameter name=\"\" value=\"\" />\n"+
		"</wrapper>";

	private JEditorPane edit;
	private InformationSourceManager manager;
	private String wrapperdir;

	public WrapperCreator(InformationSourceManager ism, String dir){
		super("Wrapper Creator", true, true, true, true);
		manager = ism;
		wrapperdir = dir;

		Container c = getContentPane();
		c.setLayout(new BorderLayout());

		edit = new JEditorPane("text/xml", DEFAULT_TEXT);
		c.add(edit, BorderLayout.CENTER);

		JPanel buttonarea = new JPanel();

		JButton bcreate = new JButton(LABEL_CREATE);
		bcreate.addActionListener(this);
		buttonarea.add(bcreate);

		JButton bload = new JButton(LABEL_LOAD);
		bload.addActionListener(this);
		buttonarea.add(bload);

		JButton bclear = new JButton(LABEL_CLEAR);
		bclear.addActionListener(this);
		buttonarea.add(bclear);

		JButton bcancel = new JButton(LABEL_CANCEL);
		bcancel.addActionListener(this);
		buttonarea.add(bcancel);

		c.add(buttonarea, BorderLayout.SOUTH);
	}

	public void actionPerformed(ActionEvent ae){
		String command = ae.getActionCommand();
		if(command.equals(LABEL_CREATE)){
			createWrapper();
		}
		else if(command.equals(LABEL_LOAD)){
			loadFile();
		}
		else if(command.equals(LABEL_CLEAR)){
			edit.setText(DEFAULT_TEXT);
		}
		else if(command.equals(LABEL_CANCEL)){
			doClose();
		}
	}

	private void createWrapper(){
		try {
			manager.addInformationSource(edit.getText());;
			doClose();
		} catch(Exception e) {
			showException(e);
		}
	}

	private void loadFile(){
		JFileChooser ch = new JFileChooser(wrapperdir);
		int status = ch.showOpenDialog(this);
		if(status == JFileChooser.APPROVE_OPTION){
			try{
				edit.setPage(ch.getSelectedFile().toURI().toURL());
			} catch(Exception e){
				showException(e);
			}
		}
	}

	private void doClose(){
		manager = null;
		edit = null;
		dispose();
	}

	public void showException(Exception e){
		ExceptionDialog message = new ExceptionDialog(e);
		message.setLocationRelativeTo(this);
		message.setVisible(true);
	}
}
