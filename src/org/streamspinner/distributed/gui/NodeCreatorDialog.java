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
import java.rmi.*;
import javax.swing.*;
import org.streamspinner.distributed.*;

public class NodeCreatorDialog extends JDialog implements ActionListener {

	private JTextField text;
	private DistributedSystemMonitor dsm;

	public NodeCreatorDialog(DistributedSystemMonitor dsm){
		super(dsm, "NodeCreatorDialog", false);
		this.dsm = dsm;

		JPanel p = new JPanel();

		p.add(new JLabel("Input nodename or IP:"), BorderLayout.WEST);

		this.text = new JTextField(20);
		p.add(text, BorderLayout.EAST);

		JButton b = new JButton("OK");
		b.addActionListener(this);
		p.add(b);

		Container c = getContentPane();
		c.add(p);

		setSize(400, 100);
		setVisible(true);
	}

	public void actionPerformed(ActionEvent ae){
		String hostname = text.getText();
		String url = "rmi://" + hostname + "/NodeManager";
		try {
			NodeManager nm = (NodeManager)(Naming.lookup(url));
			dsm.addNodeManager(nm);
			setVisible(false);
			dispose();
		} catch(Exception e){
			e.printStackTrace();
		}
	}
}
