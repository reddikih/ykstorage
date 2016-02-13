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

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import org.streamspinner.engine.Schema;
import java.util.Hashtable;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.event.*;
import javax.swing.tree.*;


public class TableTree extends JPanel implements MouseListener {

	private class TableTreeCellRenderer extends DefaultTreeCellRenderer {
		public TableTreeCellRenderer(){
			super();
		}
		public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected, boolean expanded, boolean leaf, int row, boolean hasFocus){
			super.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);
			if(value instanceof DefaultMutableTreeNode){
				DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
				if(node.getLevel() < 2 && leaf == true)
					setIcon(getClosedIcon());
			}
			return this;
		}
	}

	private class ArrivalAnimation extends Thread {

		private TreePath path;

		public ArrivalAnimation(ThreadGroup g, String name, JTree tree, TreePath p){
			super(g, name);
			path = p;
		}

		public void run(){
			if(tree == null || path == null)
				return;
			Rectangle rect = tree.getPathBounds(path);
			if(rect == null)
				return;
			Graphics g = tree.getGraphics();
			Color fc = new Color(255,0,0), bc = tree.getBackground();
			int width = tree.getWidth(), height = tree.getHeight();
			int numofstep = 8;
			int w = (width - (rect.x + rect.width)) / numofstep;

			g.setColor(fc);
			g.drawRect(rect.x, rect.y, rect.width, rect.height);
			try {
				for(int i=numofstep; i >= 0; i--){
					int x = rect.x + rect.width + w * i;
					g.setColor(fc);
					g.fillRect(x, rect.y, 20, rect.height);
					Thread.sleep(50);
					g.setColor(bc);
					g.fillRect(x, rect.y, 20, rect.height);
				}
			} catch(Exception e){
				e.printStackTrace();
			}
			g.setColor(bc);
			g.drawRect(rect.x, rect.y, rect.width, rect.height);
		}
	}

	private ThreadGroup threads;
	private Hashtable<String, TreePath> paths;
	private JDesktopPane workspace;
	private JTree tree;
	private InformationSourceManager manager;

	public TableTree(InformationSourceManager ism){
		super();
		manager = ism;
		workspace = null;
		paths = new Hashtable<String, TreePath>();
		tree = new JTree();

		DefaultMutableTreeNode root = new DefaultMutableTreeNode("localhost");
		InformationSource[] sources = manager.getAllInformationSources();
		for(int i=0; sources != null && i < sources.length; i++){
			DefaultMutableTreeNode wnode = createWrapperNode(root, sources[i]);
			root.add(wnode);
		}

		DefaultTreeModel mod = new DefaultTreeModel(root);
		tree.setModel(mod);
		for(int i=0; i < tree.getRowCount(); i++){
			tree.expandRow(i);
		}

		JScrollPane scroll = new JScrollPane(tree, ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		scroll.setPreferredSize(new Dimension(220, 550));
		add(scroll);

		threads = new ThreadGroup("Animation Threads");

		tree.setCellRenderer(new TableTreeCellRenderer());

		tree.addMouseListener(this);
	}


	private DefaultMutableTreeNode createWrapperNode(DefaultMutableTreeNode root, InformationSource is){
		DefaultMutableTreeNode wnode = new DefaultMutableTreeNode(is.getName());
		TreePath rpath = new TreePath(root);
		TreePath wpath = rpath.pathByAddingChild(wnode);
		String[] tablenames = is.getAllTableNames();

		for(int i=0; tablenames != null && i < tablenames.length; i++){
			DefaultMutableTreeNode tnode = new DefaultMutableTreeNode(tablenames[i]);
			TreePath tpath = wpath.pathByAddingChild(tnode);
			wnode.add(tnode);
			paths.put(tablenames[i], tpath);
		}
		return wnode;
	}


	public void setJDesktopPane(JDesktopPane d){
		workspace = d;
	}

	public void mouseClicked(MouseEvent me){
		TreePath path = tree.getPathForLocation(me.getX(), me.getY());
		if(path == null)
			return;
		TreeNode node = (TreeNode)(path.getLastPathComponent());
		String tablename = node.toString();
		if(node.isLeaf() == false || (! paths.containsKey(tablename)))
			return;
		try {
			InformationSource is = manager.getSourceFromTableName(tablename);
			if(is != null && workspace != null){
				TableViewer tv = new TableViewer(is, tablename);
				tv.setVisible(true);
				workspace.add(tv);
				tv.setLocation(new Point(10 * (workspace.getAllFrames().length -1), 10 * (workspace.getAllFrames().length -1)));
				tv.toFront();
			}
		} catch(StreamSpinnerException sse){
			sse.printStackTrace();
		}
	}

	public void mouseEntered(MouseEvent me){ ; }

	public void mouseExited(MouseEvent me){ ; }

	public void mousePressed(MouseEvent me){ ; }

	public void mouseReleased(MouseEvent me){ ; }

	public void showAnimation(String tablename){
		if(! paths.containsKey(tablename))
			return;
		TreePath p = paths.get(tablename);
		if(tree.isVisible(p)){
			Thread t = new ArrivalAnimation(threads, p.toString(), tree, p);
			t.start();
		}
	}

	public void addInformationSource(InformationSource is){
		DefaultTreeModel model = (DefaultTreeModel)(tree.getModel());
		DefaultMutableTreeNode root = (DefaultMutableTreeNode)(model.getRoot());
		int index = root.getChildCount();

		DefaultMutableTreeNode wnode = createWrapperNode(root, is);

		model.insertNodeInto(wnode, root, index);
		tree.expandRow(tree.getRowCount()-1);
	}

	public void deleteInformationSource(InformationSource is){
		DefaultTreeModel model = (DefaultTreeModel)(tree.getModel());
		DefaultMutableTreeNode root = (DefaultMutableTreeNode)(model.getRoot());
		for(int i=0; i < root.getChildCount(); i++){
			DefaultMutableTreeNode wnode = (DefaultMutableTreeNode)(root.getChildAt(i));
			if(is.getName().equals(wnode.toString())){
				String[] tablenames = is.getAllTableNames();
				for(int j=0; tablenames != null && j < tablenames.length; j++)
					paths.remove(tablenames[j]);
				model.removeNodeFromParent(wnode);
				break;
			}
		}
	}

	public void addTable(String sourcename, String tablename){
		DefaultTreeModel model = (DefaultTreeModel)(tree.getModel());
		DefaultMutableTreeNode root = (DefaultMutableTreeNode)(model.getRoot());
		TreePath rpath = new TreePath(root);
		for(int i=0; i < root.getChildCount(); i++){
			DefaultMutableTreeNode wnode = (DefaultMutableTreeNode)(root.getChildAt(i));
			TreePath wpath = rpath.pathByAddingChild(wnode);

			if(sourcename.equals(wnode.toString())){
				DefaultMutableTreeNode tnode = new DefaultMutableTreeNode(tablename);
				model.insertNodeInto(tnode, wnode, wnode.getChildCount());
				TreePath tpath = wpath.pathByAddingChild(tnode);
				paths.put(tablename, tpath);
				if(wnode.getChildCount() == 1)
					tree.expandPath(wpath);
				break;
			}
		}
	}

	public void deleteTable(String sourcename, String tablename){
		DefaultTreeModel model = (DefaultTreeModel)(tree.getModel());
		DefaultMutableTreeNode root = (DefaultMutableTreeNode)(model.getRoot());
		for(int i=0; i < root.getChildCount(); i++){
			DefaultMutableTreeNode wnode = (DefaultMutableTreeNode)(root.getChildAt(i));
			if(sourcename.equals(wnode.toString())){
				for(int j=0; j < wnode.getChildCount(); j++){
					DefaultMutableTreeNode tnode = (DefaultMutableTreeNode)(wnode.getChildAt(j));
					if(tablename.equals(tnode.toString())){
						paths.remove(tablename);
						model.removeNodeFromParent(tnode);
						break;
					}
				}
				break;
			}
		}
	}

}
