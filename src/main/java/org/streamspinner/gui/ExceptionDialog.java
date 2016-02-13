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

import java.awt.Container;
import javax.swing.JDialog;
import javax.swing.JTextArea;
import javax.swing.JScrollPane;

public class ExceptionDialog extends JDialog {

	public static final String TITLE="Exception!";

	public ExceptionDialog(Exception e){
		super();
		setTitle("Exception!");
		setSize(600, 400);

		JTextArea text = new JTextArea();

		StringBuffer stackinfo = new StringBuffer();
		stackinfo.append(e.getMessage());
		stackinfo.append("\n");
		StackTraceElement[] stack = e.getStackTrace();
		for(int i=0; stack != null && i < stack.length; i++){
			stackinfo.append("    ");
			stackinfo.append(stack[i].toString());
			stackinfo.append("\n");
		}
		text.setText(stackinfo.toString());
		text.setCaretPosition(0);
		text.setEditable(false);

		Container c = getContentPane();
		JScrollPane scroll = new JScrollPane(text, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		scroll.setWheelScrollingEnabled(true);
		c.add(scroll);
	}

}
