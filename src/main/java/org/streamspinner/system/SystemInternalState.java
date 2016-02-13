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
package org.streamspinner.system;

import org.streamspinner.*;
import java.util.*;
import java.io.*;

public class SystemInternalState implements InternalState, Serializable {

	private InternalState stateOfMediator;
	private ArrayList<ConnectionAdaptorInternalState> stateOfConn;
	private InternalState stateOfInformationSourceManager;

	public SystemInternalState(){
		stateOfMediator = null;
		stateOfConn = new ArrayList<ConnectionAdaptorInternalState>();
		stateOfInformationSourceManager = null;
	}

	public InternalState getMediatorInternalState(){
		return stateOfMediator;
	}

	public List<ConnectionAdaptorInternalState> getConnectionAdaptorInternalStates(){
		return stateOfConn;
	}

	public void setMediatorInternalState(InternalState is){
		stateOfMediator = is;
	}

	public void addConnectionAdaptorInternalState(ConnectionAdaptorInternalState cais){
		stateOfConn.add(cais);
	}

	public void removeConnectionAdaptorInternalState(ConnectionAdaptorInternalState cais){
		stateOfConn.remove(cais);
	}

	public void setInformationSourceManagerInternalState(InternalState is){
		stateOfInformationSourceManager = is;
	}

	public InternalState getInformationSourceManagerInternalState(){
		return stateOfInformationSourceManager;
	}

}
