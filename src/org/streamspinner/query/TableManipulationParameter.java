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

import org.streamspinner.Operators;

public class TableManipulationParameter {

	public static final String STORE = Operators.STORE;
	public static final String CREATE = Operators.CREATE;
	public static final String DROP = Operators.DROP;

	private String operation;
	private String parameter;

	public TableManipulationParameter(String op, String param){
		operation = op;
		parameter = param;
	}

	public String getOperationType(){
		return operation;
	}

	public String getParameter(){
		return parameter;
	}

	public boolean equals(Object obj){
		if(! (obj instanceof TableManipulationParameter))
			return false;
		TableManipulationParameter target = (TableManipulationParameter)obj;
		return operation.equals(target.operation) && parameter.equals(target.parameter);
	}

	public int hashCode(){
		return operation.hashCode() + parameter.hashCode();
	}

	public String toString(){
		return toString("");
	}

	public String toString(String basestr){
		if(operation.equals(STORE))
			return "INSERT INTO " + parameter + " " + basestr;
		else if(operation.equals(CREATE))
			return "CREATE TABLE " + parameter;
		else 
			return "DROP TABLE " + parameter;
	}

	public TableManipulationParameter copy(){
		return new TableManipulationParameter(new String(operation), new String(parameter));
	}

	public static TableManipulationParameter store(String parameter){
		return new TableManipulationParameter(STORE, parameter);
	}

	public static TableManipulationParameter create(String parameter){
		return new TableManipulationParameter(CREATE, parameter);
	}

	public static TableManipulationParameter drop(String parameter){
		return new TableManipulationParameter(DROP, parameter);
	}
}
