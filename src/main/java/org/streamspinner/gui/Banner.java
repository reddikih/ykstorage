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
import java.util.ResourceBundle;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

public class Banner extends JDialog {

	public static final int WIDTH=400;
	public static final int HEIGHT=250;
	public static final int STREAMSPINNER=0;
	public static final int HARMONICA=1;

	private int mode;

	private JPanel panel;

	public Banner(){
		this(STREAMSPINNER);
	}

	public Banner(int mode){
		super();
		JRootPane r = getRootPane();
		r.setWindowDecorationStyle(JRootPane.NONE);

		Container c = getContentPane();
		c.setBackground(Color.WHITE);

		Dimension dm = Toolkit.getDefaultToolkit().getScreenSize();
		setLocation(dm.width/2 - WIDTH/2, dm.height / 2 - HEIGHT/2);

		setSize(WIDTH, HEIGHT);
		setResizable(false);

		repaint();
	}

	public void paint(Graphics g){
		super.paint(g);
		Container c = getContentPane();
		int width = c.getWidth();
		int height = c.getHeight();
		int w, h;
		FontMetrics fm;

		g.setColor(new Color(255, 255, 144));
		g.fillOval(50, 10, 300, 250);
		g.setColor(Color.BLUE);
		if(mode == HARMONICA)
			g.setColor(new Color(0, 245, 140));
		g.fillRect(10, 60, 380, 100);

		String projectname = "StreamSpinner";
		if(mode == HARMONICA)
			projectname = "Harmonica";
		g.setFont(new Font(g.getFont().getName(), Font.BOLD, 50));
		fm = g.getFontMetrics();
		w = SwingUtilities.computeStringWidth(fm, projectname);
		h = fm.getHeight();
		g.setColor(Color.WHITE);
		g.drawString(projectname, (width - w)/2, (int)(height * 6.0 /10.0));

		g.setFont(new Font(g.getFont().getName(), Font.BOLD, 12));
		fm = g.getFontMetrics();
		h = fm.getHeight();
		g.setColor(Color.BLACK);

		String lab = "Kitagawa Data Engineering Laboratory";
		w = SwingUtilities.computeStringWidth(fm, lab);
		g.drawString(lab, (width - w) / 2, (int)(height * 8.5/10.0));

		String cs = "Graduate School of Systems and Information Engineering";
		w = SwingUtilities.computeStringWidth(fm, cs);
		g.drawString(cs, (width - w) / 2, (int)(height * 9.25/10.0));

		String univ = "University of Tsukuba";
		w = SwingUtilities.computeStringWidth(fm, univ);
		g.drawString(univ, (width - w) / 2, (int)(height * 10.0/10.0));
	}

}

