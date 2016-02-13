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
import org.streamspinner.InformationSource;
import org.streamspinner.DataTypes;
import org.streamspinner.engine.Schema;
import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.query.AttributeList;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.PredicateSet;
import org.streamspinner.query.ORNode;
import javax.swing.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;

public class TableViewer extends JInternalFrame {

	public TableViewer(InformationSource is, String tablename){
		super(tablename, true, true, false, true);
		setTitle(tablename);

		Schema s = is.getSchema(tablename);

		DefaultTableModel model = new DefaultTableModel(s.getAttributeNames(), 0);
		model.addRow(s.getTypes());

		Container c = getContentPane();
		c.setLayout(new BoxLayout(c, BoxLayout.Y_AXIS));
		c.add(new JLabel(tablename));

		JTable table = new JTable(model);
		JScrollPane scroll = new JScrollPane(table);
		scroll.setPreferredSize(new Dimension(400, 45));
		c.add(scroll);
		setSize(500, 85);

		if((! s.getTableType().equals(Schema.RDB)) && (! s.getTableType().equals(Schema.HISTORY)))
			return;

		AttributeList attr = new AttributeList("*");
		SourceSet source = new SourceSet(tablename, Long.MAX_VALUE);
		PredicateSet cond = new PredicateSet();

		ORNode node = new ORNode(attr, source, cond);

		try {
			DefaultTableModel datamodel = new DefaultTableModel(s.getAttributeNames(), 0);
			TupleSet tset = is.getTupleSet(node);
			if(tset != null){
				tset.beforeFirst();
				while(tset.next()){
					Vector row = new Vector();
					Tuple tuple = tset.getTuple();
					for(int i=0; i < s.size(); i++)
						row.add(tuple.getString(i));
					datamodel.addRow(row);
				}
				tset.close();
			}
			JTable datatable = new JTable(datamodel);
			JScrollPane datascroll = new JScrollPane(datatable);
			c.add(datascroll);
			setSize(500, 300);
		} catch(StreamSpinnerException sse){
			sse.printStackTrace();
		}

	}
}
