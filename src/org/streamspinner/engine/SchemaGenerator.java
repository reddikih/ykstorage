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
package org.streamspinner.engine;

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import org.streamspinner.Operators;
import org.streamspinner.DataTypes;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.AttributeList;
import org.streamspinner.query.FunctionParameter;
import org.streamspinner.query.RenameParameter;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.ANDNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Comparator;
import java.util.Arrays;

public class SchemaGenerator {

	private InformationSourceManager ism;
	private ExecutionPlan plan;
	private HashMap<ORNode, Schema> schemas;

	public SchemaGenerator(InformationSourceManager i, ExecutionPlan p) throws StreamSpinnerException {
		ism = i;
		plan = p;
		schemas = new HashMap<ORNode, Schema>();

		resolveSchema();
	}

	private void resolveSchema() throws StreamSpinnerException {
		OperatorGroup[] op = plan.getOperators();

		for(int i=0; op != null && i < op.length; i++){
			ANDNode an = op[i].getANDNodes()[0];
			ORNode output = an.getOutputORNode();
			ORNode[] inputs = op[i].getInputORNodes();
			Schema[] ischemas = new Schema[inputs.length];
			for(int j=0; j < inputs.length; j++)
				ischemas[j] = getSchema(inputs[j]);

			String type = op[i].getType();
			if(type.equals(Operators.ROOT))
				continue;
			else if(type.equals(Operators.SELECTION))
				selection(output, an, ischemas);
			else if(type.equals(Operators.PROJECTION))
				projection(output, an, ischemas);
			else if(type.equals(Operators.JOIN))
				join(output, an, ischemas);
			else if(type.equals(Operators.EVAL))
				eval(output, an, ischemas);
			else if(type.equals(Operators.GROUP))
				group(output, an, ischemas);
			else if(type.equals(Operators.RENAME))
				rename(output, an, ischemas);
			else if(type.equals(Operators.STORE))
				table(output, an, ischemas);
			else if(type.equals(Operators.CREATE))
				table(output, an, ischemas);
			else if(type.equals(Operators.DROP))
				table(output, an, ischemas);
			else
				throw new StreamSpinnerException("Unknown operator: " + type);
		}
	}

	public Schema getSchema(ORNode n) throws StreamSpinnerException {
		if(schemas.containsKey(n))
			return schemas.get(n);
		if(n.isBase())
			return ism.getSchema(n);
		throw new StreamSpinnerException("cannot guess schema: " + n.toString());
	}

	private void selection(ORNode output, ANDNode an, Schema[] ischemas) throws StreamSpinnerException {
		Schema oschema = ischemas[0];
		if(oschema.getTableType().equals(Schema.RDB))
			oschema.setTableType(Schema.RDB);
		else
			oschema.setTableType(Schema.STREAM);
		schemas.put(output, oschema);
	}

	private void projection(ORNode output, ANDNode an, Schema[] ischemas) throws StreamSpinnerException {
		Schema oschema = ischemas[0].subset(an.getAttributeList());
		if(oschema.getTableType().equals(Schema.RDB))
			oschema.setTableType(Schema.RDB);
		else
			oschema.setTableType(Schema.STREAM);
		schemas.put(output, oschema);
	}

	private void join(ORNode output, ANDNode an, Schema[] ischemas) throws StreamSpinnerException {
		ischemas = sortSchemas(ischemas, output);
		Schema oschema = new Schema();
		oschema.setTableType(Schema.UNKNOWN);
		for(int i=0; i < ischemas.length; i++){
			Schema tmp = oschema.concat(ischemas[i]);

			String otype = oschema.getTableType();
			String itype = ischemas[i].getTableType();
			if(otype.equals(Schema.UNKNOWN))
				tmp.setTableType(itype);
			else if(otype.equals(itype))
				tmp.setTableType(otype);
			else
				tmp.setTableType(Schema.STREAM);
			oschema = tmp;
		}

		if(! isSameSource(output))
			oschema.setTableType(Schema.STREAM);
		if(! oschema.getTableType().equals(Schema.RDB))
			oschema.setTableType(Schema.STREAM);

		schemas.put(output, oschema);
	}

	private void eval(ORNode output, ANDNode an, Schema[] ischemas) throws StreamSpinnerException {
		FunctionParameter fp = an.getFunctionParameter();
		Function f = Function.getInstance(fp, ischemas[0]);
		Schema oschema = ischemas[0].append(fp.toString(), f.getReturnType());
		oschema.setTableType(Schema.STREAM);
		schemas.put(output, oschema);
	}

	private void group(ORNode output, ANDNode an, Schema[] ischemas) throws StreamSpinnerException {
		AttributeList key = an.getAttributeList();
		Schema oschema = ischemas[0].group(key);
		oschema.setTableType(Schema.STREAM);
		schemas.put(output, oschema);
	}

	private void rename(ORNode output, ANDNode an, Schema[] ischemas) throws StreamSpinnerException {
		RenameParameter rp = an.getRenameParameter();
		Schema oschema = ischemas[0].rename(rp);
		oschema.setTableType(Schema.STREAM);
		schemas.put(output, oschema);
	}

	private void table(ORNode output, ANDNode an, Schema[] ischemas) throws StreamSpinnerException {
		Schema oschema = null;
		if(ischemas != null && ischemas.length == 1)
			oschema = ischemas[0].subset(new AttributeList());
		else
			oschema = new Schema();
		oschema = oschema.append("Status", DataTypes.STRING);
		oschema.setTableType(Schema.STREAM);
		schemas.put(output, oschema);
	}

	private boolean isSameSource(ORNode on) throws StreamSpinnerException {
		SourceSet ss = on.getSources();
		InformationSource is = null;
		boolean rval = true;
		for(Iterator sit = ss.iterator(); sit.hasNext(); ){
			InformationSource i = ism.getSourceFromTableName((String)(sit.next()));
			if(is == null)
				is = i;
			else if(! is.getName().equals(i.getName()))
				rval = false;
		}
		return rval;
	}

	private Schema[] sortSchemas(Schema[] inputs, ORNode o){
		Arrays.sort(inputs, new SchemaComparator(o));
		return inputs;
	}

	private class SchemaComparator implements Comparator<Schema> {
		private SourceSet ss;
		public SchemaComparator(ORNode o){
			ss = o.getSources();
		}
		public int compare(Schema s1, Schema s2){
			if(s1.equals(s2))
				return 0;
			String[] t1 = s1.getBaseTableNames();
			String[] t2 = s2.getBaseTableNames();
			String first = (String)(ss.iterator().next());
			for(int i=0; i < t1.length; i++)
				if(first.equals(t1[i]))
					return -1;
			for(int i=0; i < t2.length; i++)
				if(first.equals(t2[i]))
					return 1;
			return 0;
		}
	}

}
