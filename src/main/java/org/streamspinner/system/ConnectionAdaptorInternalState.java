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

import org.streamspinner.InternalState;
import org.streamspinner.connection.DeliveryUnit;
import java.io.Serializable;
import java.util.LinkedList;

public class ConnectionAdaptorInternalState implements InternalState, Serializable {

	private long stateOfId;
	private long stateOfSeqno;
	private LinkedList<DeliveryUnit> stateOfBuffer;

	public ConnectionAdaptorInternalState(){
		stateOfId = -1;
		stateOfSeqno = -1;
		stateOfBuffer = null;
	}

	public long getConnectionID(){
		return stateOfId;
	}

	public long getSequenceNumber(){
		return stateOfSeqno;
	}

	public LinkedList<DeliveryUnit> getBuffer(){
		return stateOfBuffer;
	}

	public void setConnectionID(long cid){
		stateOfId = cid;
	}

	public void setSequenceNumber(long seqno){
		stateOfSeqno = seqno;
	}

	public void setBuffer(LinkedList<DeliveryUnit> buffer){
		stateOfBuffer = buffer;
	}
		
}
