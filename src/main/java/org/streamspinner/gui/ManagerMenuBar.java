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

import javax.swing.*;

public class ManagerMenuBar extends JMenuBar {

	public static final String LABEL_SYSTEM = "System";
	public static final String LABEL_SHUTDOWN = "Shutdown";
	public static final String LABEL_CQTERMINAL= "CQ Terminal";
	public static final String LABEL_OPEN= "Open Query";
	public static final String LABEL_WRAPPER= "Wrapper";
	public static final String LABEL_ADDWRAPPER= "Add a new wrapper";
	public static final String LABEL_DELETEWRAPPER= "Delete a wrapper";

	public ManagerMenuBar(SystemManagerSwing manager){
		super();

		JMenu msystem = new JMenu(LABEL_SYSTEM);

		JMenuItem mopen = new JMenuItem(LABEL_OPEN);
		mopen.addActionListener(manager);
		msystem.add(mopen);

		JMenuItem mterminal = new JMenuItem(LABEL_CQTERMINAL);
		mterminal.addActionListener(manager);
		msystem.add(mterminal);

		msystem.addSeparator();

		JMenuItem mshutdown = new JMenuItem(LABEL_SHUTDOWN);
		mshutdown.addActionListener(manager);
		msystem.add(mshutdown);

		this.add(msystem);


		JMenu mwrapper = new JMenu(LABEL_WRAPPER);

		JMenuItem maddwrapper = new JMenuItem(LABEL_ADDWRAPPER);
		maddwrapper.addActionListener(manager);
		mwrapper.add(maddwrapper);

		JMenuItem mdeletewrapper = new JMenuItem(LABEL_DELETEWRAPPER);
		mdeletewrapper.addActionListener(manager);
		mwrapper.add(mdeletewrapper);

		this.add(mwrapper);
	}

}
