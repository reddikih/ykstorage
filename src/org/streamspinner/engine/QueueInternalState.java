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

import java.io.*;
import org.streamspinner.*;
import org.streamspinner.query.*;

public abstract class QueueInternalState implements InternalState, Serializable {

	private String planid = null;
	private MasterSet master = null;
	private ORNode ornode = null;

	public QueueInternalState(){
		;
	}

	public QueueInternalState(String planid, MasterSet master, ORNode ornode){
		this.planid = planid;
		this.master = master;
		this.ornode = ornode;
	}


	public String getExecutionPlanID(){
		return planid;
	}

	public MasterSet getMasterSet(){
		return master;
	}

	public ORNode getORNode(){
		return ornode;
	}

	public void setExecutionPlanID(String planid){
		this.planid = planid;
	}

	public void setMasterSet(MasterSet master){
		this.master= master;
	}

	public void setORNode(ORNode ornode){
		this.ornode = ornode;
	}

}
