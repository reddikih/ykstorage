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
import java.rmi.*;
import java.util.Hashtable;
import java.util.Set;
import javax.swing.*;
import javax.swing.tree.*;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.Query;
import org.streamspinner.distributed.*;

public class NodeInfoPanel extends JPanel {

	private class InternalTreeCellRenderer extends DefaultTreeCellRenderer {
		private int threashold;
		public InternalTreeCellRenderer(int threashold){
			super();
			this.threashold = threashold;
		}
		public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected, boolean expanded, boolean leaf, int row, boolean hasFocus){
			super.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);
			if(value instanceof DefaultMutableTreeNode){
				DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
				if(node.getLevel() < threashold && leaf == true)
					setIcon(getClosedIcon());
			}
			return this;
		}
	}

	private class HighlightAnimationThread extends Thread {
		private JTree target;
		private TreePath path;
		private Color color;
		public HighlightAnimationThread(JTree target, TreePath path, Color color){
			this.target = target;
			this.path = path;
			this.color = color;
		}
		public void run(){
			try {
				Rectangle rect = target.getPathBounds(path);
				Graphics2D g = (Graphics2D)(target.getGraphics());
				g.setColor(color);
				g.draw(rect);
				Thread.sleep(700);
				rect = target.getPathBounds(path);
				g = (Graphics2D)(target.getGraphics());
				g.setColor(target.getBackground());
				g.draw(rect);
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	private String nodename;
	private Hashtable<String, DefaultMutableTreeNode> wrappers;
	private Hashtable<String, DefaultMutableTreeNode> tables;
	private Hashtable<String, DefaultMutableTreeNode> queries;
	private JTree ttree;
	private JTree qtree;

	public NodeInfoPanel(NodeManager nm) throws StreamSpinnerException {
		super();
		wrappers = new Hashtable<String, DefaultMutableTreeNode>();
		tables = new Hashtable<String, DefaultMutableTreeNode>();
		queries = new Hashtable<String, DefaultMutableTreeNode>();

		try {
			nodename = nm.getNodeName();
			setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.BLACK), nodename));

			constructTableTree(nm);
			ttree.setCellRenderer(new InternalTreeCellRenderer(2));
			JScrollPane tscroll = new JScrollPane(ttree, ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
			tscroll.setPreferredSize(new Dimension(150, 200));
			add(tscroll, BorderLayout.WEST);

			constructQueryTree(nm);
			qtree.setCellRenderer(new InternalTreeCellRenderer(1));
			JScrollPane qscroll = new JScrollPane(qtree, ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
			qscroll.setPreferredSize(new Dimension(200, 200));
			add(qscroll, BorderLayout.EAST);
		} catch(RemoteException re){
			throw new StreamSpinnerException(re);
		}
	}

	private void constructTableTree(NodeManager nm) throws RemoteException {
		DefaultMutableTreeNode root = new DefaultMutableTreeNode("Sources");
		DefaultTreeModel model = new DefaultTreeModel(root);
		ttree = new JTree(model);

		String[] isnames = nm.getInformationSourceNames();
		for(String is : isnames)
			addInformationSourceLabel(is, nm.getTableNames(is));
		for(int i=0; i < ttree.getRowCount(); i++)
			ttree.expandRow(i);
	}

	private void constructQueryTree(NodeManager nm) throws RemoteException {
		DefaultMutableTreeNode root = new DefaultMutableTreeNode("Queries");
		DefaultTreeModel model = new DefaultTreeModel(root);
		qtree = new JTree(model);
		Query[] qarray = nm.getQueries();
		for(Query q : qarray)
			addQueryLabel(q);
		for(int i=0; i < qtree.getRowCount(); i++)
			qtree.expandRow(i);
	}

	public String getNodeName(){
		return nodename;
	}

	public void addInformationSourceLabel(String wrappername, String[] tablenames){
		DefaultTreeModel model = (DefaultTreeModel)(ttree.getModel());
		DefaultMutableTreeNode root = (DefaultMutableTreeNode)(model.getRoot());
		DefaultMutableTreeNode wnode = new DefaultMutableTreeNode(wrappername);
		model.insertNodeInto(wnode, root, root.getChildCount());

		wrappers.put(wrappername, wnode);
		for(String table : tablenames)
			addTableLabel(wrappername, table);
	}

	public void addTableLabel(String wrappername, String tablename){
		if(! wrappers.containsKey(wrappername))
			return;
		DefaultTreeModel model = (DefaultTreeModel)(ttree.getModel());
		DefaultMutableTreeNode wnode = wrappers.get(wrappername);
		DefaultMutableTreeNode tnode = new DefaultMutableTreeNode(tablename);
		model.insertNodeInto(tnode, wnode, wnode.getChildCount());
		tables.put(tablename, tnode);
	}

	public void deleteInformationSourceLabel(String wrappername){
		if(! wrappers.containsKey(wrappername))
			return;
		DefaultTreeModel model = (DefaultTreeModel)(ttree.getModel());
		DefaultMutableTreeNode wnode = wrappers.get(wrappername);
		for(int i=wnode.getChildCount() -1 ; i >= 0; i--){
			DefaultMutableTreeNode tnode = (DefaultMutableTreeNode)(wnode.getChildAt(i));
			deleteTableLabel(wrappername, (String)(tnode.getUserObject()));
		}
		model.removeNodeFromParent(wnode);
		wrappers.remove(wrappername);
	}

	public void deleteTableLabel(String wrappername, String tablename){
		if(! tables.containsKey(tablename))
			return;
		DefaultTreeModel model = (DefaultTreeModel)(ttree.getModel());
		DefaultMutableTreeNode tnode = tables.get(tablename);
		model.removeNodeFromParent(tnode);
		tables.remove(tablename);
	}

	public void addQueryLabel(Query q){
		DefaultTreeModel model = (DefaultTreeModel)(qtree.getModel());
		DefaultMutableTreeNode root = (DefaultMutableTreeNode)(model.getRoot());
		DefaultMutableTreeNode qnode = new DefaultMutableTreeNode(q.toString());
		model.insertNodeInto(qnode, root, root.getChildCount());
		String id = q.getID().toString();
		queries.put(id, qnode);
	}

	public void deleteQueryLabel(Query q){
		String key = q.getID().toString();
		if(! queries.containsKey(key))
			return;
		DefaultTreeModel model = (DefaultTreeModel)(qtree.getModel());
		DefaultMutableTreeNode qnode = queries.get(key);
		model.removeNodeFromParent(qnode);
		queries.remove(key);
	}

	public void showDataArrival(String tablename){
		if(! tables.containsKey(tablename))
			return;
		DefaultMutableTreeNode tnode = tables.get(tablename);
		TreePath path = new TreePath(tnode.getPath());
		if(! ttree.isVisible(path))
			return;
		Thread t = new HighlightAnimationThread(ttree, path, Color.RED);
		t.start();
	}

	public void showQueryExecution(Set queryids){
		for(Object o : queryids){
			String key = o.toString();
			if(! queries.containsKey(key))
				continue;
			DefaultMutableTreeNode qnode = queries.get(key);
			TreePath path = new TreePath(qnode.getPath());
			if(! qtree.isVisible(path))
				continue;
			Thread t = new HighlightAnimationThread(qtree, path, Color.BLUE);
			t.start();
		}
	}
}
