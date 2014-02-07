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
import java.awt.*;
import java.awt.event.*;
import java.util.*;

public class SourceBox extends JMenuItem implements Runnable {

	private Thread monitor;

	public SourceBox(String tablename){
		super(tablename);
		setPreferredSize(new Dimension(100, 20));
		setBackground(Color.WHITE);
		setForeground(Color.BLACK);
		//setBorderPainted(true);
	}

	public void activate() {
		if(monitor == null){
			monitor = new Thread(this);
			monitor.start();
		}
	}

	public void run(){
		try {
			setBackground(Color.RED);
			Thread.sleep(200);
			setBackground(Color.WHITE);
			monitor = null;
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public void paint(Graphics g){
		super.paint(g);
		g.setColor(Color.LIGHT_GRAY);
		g.drawRect(0, 0, getWidth(), getHeight());
	}
}
