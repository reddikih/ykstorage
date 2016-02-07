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

import java.util.*;
import java.io.Serializable;
import java.util.regex.*;
import org.streamspinner.StreamSpinnerException;

public class Predicate implements Serializable {

	public static final int RIGHT=1;
	public static final int LEFT=2;

	public static final String LE = "<=";
	public static final String GE = ">=";
	public static final String EQ = "=";
	public static final String NE = "!=";
	public static final String LT = "<";
	public static final String GT = ">";

	private static Pattern constant = Pattern.compile("((\\d+\\.\\d*)|('.*')|(\\d+))");

	private Object left;
	private String op;
	private Object right;

	private Predicate(){
		;
	}

	public Predicate(String l, String o, String r){
		op = new String(o);
		left = parse(l);
		right = parse(r);
	}

	public Predicate copy(){
		Predicate rval = new Predicate();
		rval.op = new String(op);

		if(left instanceof FunctionParameter)
			rval.left = ((FunctionParameter)left).copy();
		else if(left instanceof Attribute)
			rval.left = ((Attribute)left).copy();
		else
			rval.left = left.toString();

		if(right instanceof FunctionParameter)
			rval.right = ((FunctionParameter)right).copy();
		else if(right instanceof Attribute)
			rval.right = ((Attribute)right).copy();
		else
			rval.right = right.toString();

		return rval;
	}

	public Predicate reverse(){
		Predicate rval = copy();
		rval.left = right;
		rval.right = left;

		if(op.equals(LE))
			rval.op = GE;
		else if(op.equals(GE))
			rval.op = LE;
		else if(op.equals(LT))
			rval.op = GT;
		else if(op.equals(GT))
			rval.op = LT;
		else
			rval.op = op;

		return rval;
	}

	public boolean equals(Object o){
		if(! (o instanceof Predicate))
			return false;
		Predicate p = (Predicate)o;
		if(! left.equals(p.left))
			return false;
		if(! op.equals(p.op))
			return false;
		if(! right.equals(p.right))
			return false;
		return true;
	}

	public Object getLeft(){
		return left;
	}

	public String getLeftString(){
		return left.toString();
	}

	public String getOperator(){
		return op;
	}

	public Object getRight(){
		return right;
	}

	public String getRightString(){
		return right.toString();
	}

	public int hashCode(){
		return left.hashCode() + op.hashCode() + right.hashCode();
	}

	public String toString(){
		StringBuffer sb = new StringBuffer("");
		sb.append(left.toString());
		sb.append(op);
		sb.append(right.toString());
		return sb.toString();
	}

	public boolean isJoinCondition(){
		return ! ( (left instanceof java.lang.String) || (right instanceof java.lang.String));
	}

	public boolean isJoinConditionOf(SourceSet sources){
		if( (! isJoinCondition()) || sources.size() < 2 )
			return false;

		if(left instanceof Attribute){
			Attribute a = (Attribute)left;
			if(! sources.contains(a.getSourceName()))
				return false;
		}
		else if(left instanceof FunctionParameter){
			FunctionParameter f = (FunctionParameter)left;
			if(! f.isComputable(sources))
				return false;
		}
		if(right instanceof Attribute){
			Attribute a = (Attribute)right;
			if(! sources.contains(a.getSourceName()))
				return false;
		}
		else if(right instanceof FunctionParameter){
			FunctionParameter f = (FunctionParameter)right;
			if(! f.isComputable(sources))
				return false;
		}
		return true;
	}

	public boolean isSelectionConditionOf(SourceSet sources){
		if(! isSelectionCondition())
			return false;
		if(left instanceof FunctionParameter){
			FunctionParameter f = (FunctionParameter)left;
			if(f.isComputable(sources))
				return true;
		}
		if(left instanceof Attribute){
			Attribute a = (Attribute)left;
			if(sources.contains(a.getSourceName()))
				return true;
		}
		if(right instanceof FunctionParameter){
			FunctionParameter f = (FunctionParameter)right;
			if(f.isComputable(sources))
				return true;
		}
		if(right instanceof Attribute){
			Attribute a = (Attribute)right;
			if(sources.contains(a.getSourceName()))
				return true;
		}
		return false;
	}

	public boolean isSelectionCondition(){
		if( (left instanceof FunctionParameter || left instanceof Attribute ) && right instanceof java.lang.String)
			return true;
		else if( (right instanceof FunctionParameter || right instanceof Attribute ) && left instanceof java.lang.String)
			return true;
		else
			return false;
	}


	public boolean containsFunction(){
		return (left instanceof FunctionParameter) || (right instanceof FunctionParameter);
	}

	public FunctionParameter[] extractFunctions(){
		if(! containsFunction())
			return null;

		ArrayList flist = new ArrayList();
		if(left instanceof FunctionParameter)
			flist.add(left);
		if(right instanceof FunctionParameter)
			flist.add(right);

		FunctionParameter[] rval = new FunctionParameter[flist.size()];
		rval = (FunctionParameter[])(flist.toArray(rval));
		return rval;
	}

	public boolean isFunction(int leftorright){
		switch(leftorright){
			case RIGHT:
				return right instanceof FunctionParameter;
			case LEFT:
				return left instanceof FunctionParameter;
			default:
				return false;
		}
	}

	public boolean isConstant(int leftorright){
		switch(leftorright){
			case RIGHT:
				return right instanceof String;
			case LEFT:
				return left instanceof String;
			default:
				return false;
		}
	}

	public boolean isAttribute(int leftorright){
		switch(leftorright){
			case RIGHT:
				return right instanceof Attribute;
			case LEFT:
				return left instanceof Attribute;
			default:
				return false;
		}
	}

	public boolean evaluate(long vleft, long vright) throws StreamSpinnerException {
		return compare(new Long(vleft), new Long(vright));
	}

	public boolean evaluate(double vleft, double vright) throws StreamSpinnerException {
		return compare(new Double(vleft), new Double(vright));
	}

	public boolean evaluate(Comparable vleft, Comparable vright) throws StreamSpinnerException {
		return compare(vleft, vright);
	}

	private boolean compare(Comparable vleft, Comparable vright) throws StreamSpinnerException {
		int result = vleft.compareTo(vright);
		if(op.equals(EQ) )
			return result == 0;
		else if(op.equals(LE))
			return result <= 0;
		else if(op.equals(GE))
			return result >= 0;
		else if(op.equals(LT))
			return result < 0;
		else if(op.equals(GT))
			return result > 0;
		else if(op.equals(NE))
			return result != 0;
		else
			throw new StreamSpinnerException("Unknown operator: " + op);
	}
	
	public static boolean isFunction(String str){
		return FunctionParameter.isFunction(str);
	}

	public static boolean isConstant(String str){
		Matcher m = constant.matcher(str);
		return m.matches();
	}

	public static boolean isAttribute(String str){
		return Attribute.isAttribute(str);
	}


	private static Object parse(String str){
		if(isFunction(str))
			return new FunctionParameter(str);
		else if(isAttribute(str))
			return new Attribute(str);
		else
			return str;
	}

}
