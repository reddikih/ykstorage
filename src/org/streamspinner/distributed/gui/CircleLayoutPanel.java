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
import javax.swing.*;
import java.awt.event.*;
import java.util.*;
import org.streamspinner.*;
import org.streamspinner.query.*;
import org.streamspinner.engine.*;
import org.streamspinner.distributed.*;

public class CircleLayoutPanel extends JPanel {

	private Hashtable<String, NodeInfoPanel> panels;
	private Hashtable<String, Vector<ConnectionInfo>> connections;

	public CircleLayoutPanel(){
		super();
		setLayout(new CircleLayoutManager());
		setBackground(new Color(0, 0, 127));
		panels = new Hashtable<String, NodeInfoPanel>();
		connections = new Hashtable<String, Vector<ConnectionInfo>>();
	}

	public void addNode(NodeManager nm){
		try {
			NodeInfoPanel nip = new NodeInfoPanel(nm);
			String nodename = nip.getNodeName();
			panels.put(nodename, nip);
			add(nip);
			validate();
			doLayout();
			repaint();

			if(! connections.containsKey(nodename))
				connections.put(nodename, new Vector<ConnectionInfo>());
			Vector<ConnectionInfo> clist = connections.get(nodename);
			for(ConnectionInfo c : nm.getAllConnectionInfo())
				clist.add(c);

		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public void addQueryLabel(String nodename, Query q){
		if(! panels.containsKey(nodename))
			return;
		NodeInfoPanel nip = panels.get(nodename);
		nip.addQueryLabel(q);
	}

	public void deleteQueryLabel(String nodename, Query q){
		if(! panels.containsKey(nodename))
			return;
		NodeInfoPanel nip = panels.get(nodename);
		nip.deleteQueryLabel(q);
	}

	public void addInformationSourceLabel(String nodename, String wrappername, String[] tablenames){
		if(! panels.containsKey(nodename))
			return;
		NodeInfoPanel nip = panels.get(nodename);
		nip.addInformationSourceLabel(wrappername, tablenames);
	}

	public void deleteInformationSourceLabel(String nodename, String wrappername){
		if(! panels.containsKey(nodename))
			return;
		NodeInfoPanel nip = panels.get(nodename);
		nip.deleteInformationSourceLabel(wrappername);
	}

	public void addTableLabel(String nodename, String wrappername, String tablename){
		if(! panels.containsKey(nodename))
			return;
		NodeInfoPanel nip = panels.get(nodename);
		nip.addTableLabel(wrappername, tablename);
	}

	public void deleteTableLabel(String nodename, String wrappername, String tablename){
		if(! panels.containsKey(nodename))
			return;
		NodeInfoPanel nip = panels.get(nodename);
		nip.deleteTableLabel(wrappername, tablename);
	}

	public void addConnectionLine(String nodename, ConnectionInfo conn) {
		if(! connections.containsKey(nodename))
			return;
		Vector<ConnectionInfo> clist = connections.get(nodename);
		clist.add(conn);
		repaint();
	}

	public void deleteConnectionLine(String nodename, ConnectionInfo conn) {
		if(! connections.containsKey(nodename))
			return;
		Vector<ConnectionInfo> clist = connections.get(nodename);
		clist.remove(conn);
		repaint();
	}

	public void showDataArrival(String nodename, long timestamp, String master){
		if(panels.containsKey(nodename)){
			NodeInfoPanel nip = panels.get(nodename);
			nip.showDataArrival(master);
		}
		if(connections.containsKey(nodename)){
			Vector<ConnectionInfo> clist = connections.get(nodename);
			for(ConnectionInfo c : clist){
				if(! c.getTableName().equals(master))
					continue;
				if(! panels.containsKey(c.getSource()) || ! panels.containsKey(c.getDistination()))
					continue;
				NodeInfoPanel from = panels.get(c.getSource());
				NodeInfoPanel dist = panels.get(c.getDistination());
				Thread t = new ReceiveAnimationThread(from, dist);
				t.start();
			}
		}
	}

	public void showQueryExecution(String nodename, long timestamp, Set queryids){
		if(! panels.containsKey(nodename))
			return;
		NodeInfoPanel nip = panels.get(nodename);
		nip.showQueryExecution(queryids);
	}

	public void paint(Graphics g){
		super.paint(g);
		for(Vector<ConnectionInfo> clist : connections.values()){
			for(ConnectionInfo c : clist){
				if(! panels.containsKey(c.getSource()) || ! panels.containsKey(c.getDistination()))
					continue;
				NodeInfoPanel from = panels.get(c.getSource());
				NodeInfoPanel dist = panels.get(c.getDistination());
				drawConnection(g, from, dist);
			}
		}
	}

	public void drawConnection(Graphics g, Component from, Component to){
		Graphics2D g2 = (Graphics2D)g;
		g2.setColor(Color.YELLOW);
		g2.setStroke(new BasicStroke(5));
		Point pf = computeVertex(from);
		Point pt = computeVertex(to);
		g2.drawLine((int)(pf.getX()), (int)(pf.getY()), (int)(pt.getX()), (int)(pt.getY()));
	}

	private Point computeVertex(Component c){
		double centerx = getWidth() / 2.0;
		double centery = getHeight() / 2.0;

		double w = c.getWidth() / 2.0;
		double h = c.getHeight() / 2.0;
		double x = c.getLocation().getX() + w;
		double y = c.getLocation().getY() + h;
		double diag = Math.sqrt(w * w + h * h);
		double cos = w / diag;
		double sin = h / diag;

		double diffx = centerx - x;
		double diffy = centery - y;
		double dist = Math.sqrt(diffx * diffx + diffy * diffy);

		double rx, ry;

		if(Math.abs(diffx / dist) > 0.7)
			rx = x + w * Math.signum(diffx/dist);
		else
			rx = x + w * (diffx/dist);
		if(Math.abs(diffy / dist) > 0.7)
			ry = y + h * Math.signum(diffy / dist);
		else
			ry = y + h * (diffy / dist);
		Point rval = new Point();
		rval.setLocation(rx, ry);
		return rval;
	}

	private class ReceiveAnimationThread extends Thread {
		private static final int numofframe = 7;
		private Component from;
		private Component to;

		public ReceiveAnimationThread(Component from, Component to){
			this.from = from;
			this.to = to;
		}

		public void run(){
			try {
				Thread.sleep(100);
				for(int i=0; i < numofframe; i++){
					Point pf = computeVertex(from);
					Point pt = computeVertex(to);
					int beginx = (int)(pf.getX() + i * (pt.getX() - pf.getX()) / numofframe);
					int beginy = (int)(pf.getY() + i * (pt.getY() - pf.getY()) / numofframe);
					int endx = (int)(pf.getX() + (i+1) * (pt.getX() - pf.getX()) / numofframe);
					int endy = (int)(pf.getY() + (i+1) * (pt.getY() - pf.getY()) / numofframe);
					Graphics2D g2 = (Graphics2D)(getGraphics());
					g2.setStroke(new BasicStroke(5));
					g2.setColor(Color.RED);
					g2.drawLine(beginx, beginy, endx, endy);
					Thread.sleep(50);
					g2.setColor(Color.YELLOW);
					g2.drawLine(beginx, beginy, endx, endy);
				}
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
