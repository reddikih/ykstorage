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
package org.streamspinner.query;

import org.streamspinner.StreamSpinnerException;
import javax.xml.parsers.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;
import org.w3c.dom.*;
import java.io.*;
import java.util.*;


public class XMLDAGBuilder {

	private class SourceTag {
		private String id;
		private String name;
		private long window;
		private long origin;

		public SourceTag(Element s) throws StreamSpinnerException {
			id = s.getAttribute("id");
			if(id == null || id.equals(""))
				throw new StreamSpinnerException("Attribute 'id' is required");
			name = s.getAttribute("name");
			if(name == null || name.equals(""))
				throw new StreamSpinnerException("Attribute 'name' is required");
			String w = s.getAttribute("window");
			if(w == null || w.equals(""))
				window = Long.MAX_VALUE;
			else
				window = Long.parseLong(w);
			String a = s.getAttribute("window_at");
			if(a == null || a.equals(""))
				origin = 0;
			else
				origin = Long.parseLong(a);
		}

		public String getID() { return id; }
		public String getName() { return name; }
		public long getWindow() { return window; }
		public long getOrigin() { return origin; }
	}


	private class OperatorTag {
		private String id;
		private String type;
		private Map parameters;
		private String[] inputids;
		private String outputid;

		public OperatorTag(Element o) throws StreamSpinnerException{
			id = o.getAttribute("id");
			if(id == null || id.equals(""))
				throw new StreamSpinnerException("Attribute 'id' is required");

			type = o.getAttribute("type");
			if(type == null || type.equals(""))
				throw new StreamSpinnerException("Attribute 'type' is required");
			parameters = new HashMap();
			ArrayList inputlist = new ArrayList();
			NodeList children = o.getChildNodes();

			for(int i=0; i < children.getLength(); i++){
				if(! (children.item(i) instanceof Element))
					continue;
				Element c = (Element)(children.item(i));
				if(c.getNodeName().equals("parameter"))
					parameters.put(c.getAttribute("name"), c.getAttribute("value"));
				else if(c.getNodeName().equals("input"))
					inputlist.add(c.getAttribute("refid"));
				else if(c.getNodeName().equals("output"))
					outputid = c.getAttribute("refid");
			}
			inputids = (String[])(inputlist.toArray(new String[0]));
		}

		public String getID(){ return id; }
		public String getType(){ return type; }
		public String getParameter(String key){ return (String)(parameters.get(key)); }
		public String[] getInputIDs(){ return inputids; }
		public String getOutputID(){ return outputid; }
	}

	private class ParseResult {
		private MasterSet master;
		private List sources;
		private List operators;
		private Map idmap;

		public ParseResult(){
			master = new MasterSet();
			sources = new ArrayList();
			operators = new ArrayList();
			idmap = new HashMap();
		}

		public void setMaster(String m){
			master = new MasterSet(m.split("\\s*,\\s*"));
		}

		public void add(SourceTag s){
			sources.add(s);
			idmap.put(s.getID(), s);
		}

		public void add(OperatorTag o){
			operators.add(o);
			idmap.put(o.getID(), o);
		}

		public MasterSet getMaster(){
			return master;
		}

		public SourceTag[] getSourceTags(){
			return (SourceTag[])(sources.toArray(new SourceTag[0]));
		}

		public OperatorTag[] getOperatorTags(){
			return (OperatorTag[])(operators.toArray(new OperatorTag[0]));
		}

		public Object getTag(String id){
			return idmap.get(id);
		}
	}

	private DocumentBuilder db;
	private ParseResult result;
	private ArrayList ornode;
	private ArrayList andnode;
	private LinkTable ilink;
	private LinkTable olink;
	private SourceSet sources;


	public XMLDAGBuilder() throws StreamSpinnerException {
		try{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			db = dbf.newDocumentBuilder();
		} catch(ParserConfigurationException pce){
			throw new StreamSpinnerException(pce);
		}
	}

	public DAG createDAG(String xml) throws StreamSpinnerException {
		parse(xml);
		return initDAG();
	}

	public DAG createDAG(Document xml) throws StreamSpinnerException{
		parse(xml);
		return initDAG();
	}

	private DAG initDAG() throws StreamSpinnerException {
		ornode = new ArrayList();
		andnode = new ArrayList();
		ilink = new LinkTable();
		olink = new LinkTable();

		analyze();

		ORNode[] on = (ORNode[])(ornode.toArray(new ORNode[0]));
		ANDNode[] an = (ANDNode[])(andnode.toArray(new ANDNode[0]));
		return new DAG(an, on, ilink, olink);
	}

	private void parse(String xml) throws StreamSpinnerException {
		StringReader sr = new StringReader(xml);
		InputSource is = new InputSource(sr);
		try {
			Document d = db.parse(is);
			parse(d);
		} catch(SAXException se){
			throw new StreamSpinnerException(se);
		} catch(IOException se){
			throw new StreamSpinnerException(se);
		}
	}

	private void parse(Document d) throws StreamSpinnerException {
		result = new ParseResult();

		Element root = d.getDocumentElement();

		String master = root.getAttribute("master");
		if(master != null && (! master.equals("")))
			result.setMaster(master);

		NodeList children = root.getChildNodes();
		for(int i=0; i < children.getLength(); i++){
			if(! (children.item(i) instanceof Element))
				continue;
			Element e = (Element)(children.item(i));
			if(e.getTagName().equals("source"))
				result.add(new SourceTag(e));
			else if(e.getTagName().equals("operator"))
				result.add(new OperatorTag(e));
		}
	}

	private void analyze() throws StreamSpinnerException {
		sources = new SourceSet();
		SourceTag[] s = result.getSourceTags();
		for(int i=0; i < s.length; i++)
			sources.add(s[i].getName(), s[i].getWindow(), s[i].getOrigin());

		OperatorTag[] ot = result.getOperatorTags();
		boolean found = false;
		for(int i=0; i < ot.length; i++){
			if(ot[i].getType().equalsIgnoreCase("root")){
				found = true;
				createNode(ot[i].getID());
			}
		}
		if(found == false)
			throw new StreamSpinnerException("root is not found");
	}

	private ORNode createNode(String refid) throws StreamSpinnerException {
		Object o = result.getTag(refid);
		if(o instanceof SourceTag){
			SourceTag st = (SourceTag)o;
			SourceSet ss = new NestedSourceSet(st.getName(), st.getWindow(), st.getOrigin());
			ORNode rval = new ORNode(new AttributeList("*"), ss, new PredicateSet());
			ornode.add(rval);
			return rval;
		}

		OperatorTag ot = (OperatorTag)o;

		String[] ids = ot.getInputIDs();
		ORNode[] inputs = new ORNode[ids.length];
		SourceSet ss = new NestedSourceSet();
		for(int i=0; i < ids.length; i++){
			inputs[i] = createNode(ids[i]);
			ss = ss.concat(inputs[i].getSources());
		}

		String type = ot.getType();
		ANDNode node = null;
		if(type.equalsIgnoreCase("root"))
			node = root(ot, ss, inputs);
		else if(type.equalsIgnoreCase("selection"))
			node = selection(ot, ss, inputs);
		else if(type.equalsIgnoreCase("projection"))
			node = projection(ot, ss, inputs);
		else if(type.equalsIgnoreCase("join"))
			node = join(ot, ss, inputs);
		else if(type.equalsIgnoreCase("eval"))
			node = eval(ot, ss, inputs);
		else if(type.equalsIgnoreCase("group"))
			node = group(ot, ss, inputs);
		else if(type.equalsIgnoreCase("rename"))
			node = rename(ot, ss, inputs);
		else if(type.equalsIgnoreCase("store"))
			node = store(ot, ss, inputs);
		else if(type.equalsIgnoreCase("create"))
			node = create(ot, ss, inputs);
		else if(type.equalsIgnoreCase("drop"))
			node = drop(ot, ss, inputs);
		else
			throw new StreamSpinnerException("unknown type: " + type);

		if(! andnode.contains(node))
			andnode.add(node);
		for(int i=0; i < inputs.length; i++)
			ilink.add(inputs[i], node);
		ORNode output = node.getOutputORNode();
		if(output != null){
			ornode.add(output);
			olink.add(output, node);
		}
		return node.getOutputORNode();
	}

	private ANDNode root(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		return ANDNode.createRoot(result.getMaster(), ss, inputs[0]);
	}

	private ANDNode selection(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		PredicateSet cond = new PredicateSet(ot.getParameter("predicate"));
		ORNode output = new ORNode(inputs[0].getAttributeList(), ss, inputs[0].getConditions().concat(cond));
		return ANDNode.createSelection(result.getMaster(), ss, cond, inputs[0], output);
	}

	private ANDNode projection(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		AttributeList attr = AttributeList.parse(ot.getParameter("attribute"));
		ORNode output = new ORNode(attr, ss, inputs[0].getConditions());
		return ANDNode.createProjection(result.getMaster(), ss, attr, inputs[0], output);
	}

	private ANDNode join(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		String scond = ot.getParameter("predicate");
		PredicateSet cond = new PredicateSet(scond);

		AttributeList oattr = new AttributeList();
		PredicateSet ocond = cond.copy();
		for(int i=0; i < inputs.length; i++){
			ocond = ocond.concat(inputs[i].getConditions());
			AttributeList ai = inputs[i].getAttributeList();
			for(int j=0; j < ai.size(); j++)
				if(oattr.size() == 0 || (! ai.getString(j).equals("*")))
					oattr.add(ai.getString(j));
		}

		ORNode output = new ORNode(oattr, ss, ocond);
		return ANDNode.createJoin(result.getMaster(), ss, cond, inputs, output);
	}

	private ANDNode eval(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		FunctionParameter fp = new FunctionParameter(ot.getParameter("function"));
		AttributeList oattr = inputs[0].getAttributeList().copy();
		oattr.add(fp);
		ORNode output = new ORNode(oattr, ss, inputs[0].getConditions());
		return ANDNode.createEval(result.getMaster(), ss, fp, inputs[0], output);
	}

	private ANDNode group(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		AttributeList key = AttributeList.parse(ot.getParameter("attribute"));
		ORNode output = new ORNode(inputs[0].getAttributeList(), ss, inputs[0].getConditions(), key);
		return ANDNode.createGroup(result.getMaster(), ss, key, inputs[0], output);
	}

	private ANDNode rename(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		AttributeList oattr = AttributeList.parse(ot.getParameter("attribute"));
		RenameParameter rp = new RenameParameter(oattr);
		ORNode output = null;
		if(rp.isTableRename()){
			ORNode nest = new ORNode(inputs[0].getAttributeList(), ss, inputs[0].getConditions(), inputs[0].getGroupingKeys(), rp);
			NestedSourceSet nss = new NestedSourceSet(nest);
			output = new ORNode(new AttributeList("*"), nss, new PredicateSet());
		}
		else 
			output = new ORNode(inputs[0].getAttributeList(), ss, inputs[0].getConditions(), inputs[0].getGroupingKeys(), rp);
		return ANDNode.createRename(result.getMaster(), ss, rp, inputs[0], output);
	}

	private ANDNode store(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		String tablename = ot.getParameter("table");
		TableManipulationParameter tp = TableManipulationParameter.store(tablename);
		ORNode output = new ORNode(inputs[0].getAttributeList(), ss, inputs[0].getConditions(), inputs[0].getGroupingKeys(), inputs[0].getRenameParameter(), tp);
		return ANDNode.createStore(result.getMaster(), ss, tp, inputs[0], output);
	}

	private ANDNode create(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		String schema = ot.getParameter("schema");
		TableManipulationParameter tp = TableManipulationParameter.create(schema);
		ORNode output = new ORNode(tp);
		return ANDNode.createTableCreate(result.getMaster(), ss, tp, output);
	}

	private ANDNode drop(OperatorTag ot, SourceSet ss, ORNode[] inputs) throws StreamSpinnerException {
		String tablename = ot.getParameter("table");
		TableManipulationParameter tp = TableManipulationParameter.drop(tablename);
		ORNode output = new ORNode(tp);
		return ANDNode.createTableDrop(result.getMaster(), ss, tp, output);
	}
}
