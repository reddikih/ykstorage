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

import org.streamspinner.ExceptionListener;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Iterator;
import java.util.Set;

public class ExceptionNotifier {

	private Hashtable listenertable;

	public ExceptionNotifier(){
		listenertable = new Hashtable();
	}

	public void addExceptionListener(Object qid, ExceptionListener listener){
		if(! listenertable.containsKey(qid))
			listenertable.put(qid, new Vector());
		Vector listeners = (Vector)(listenertable.get(qid));
		listeners.add(listener);
	}

	public void removeExceptionListener(Object qid, ExceptionListener listener){
		if(! listenertable.containsKey(qid))
			return;
		Vector listeners = (Vector)(listenertable.get(qid));
		listeners.remove(listener);
		if(listeners.size() == 0)
			listenertable.remove(qid);
	}

	public void notifyException(long timestamp, Set ids, Exception e){
		Thread t = new NotificationThread(timestamp, ids, e);
		t.start();
	}


	private class NotificationThread extends Thread {
		private long timestamp;
		private Set idset;
		private Exception exception;

		private NotificationThread(long t, Set s, Exception e){
			timestamp = t;
			idset = s;
			exception = e;
		}

		public void run(){
			Iterator idit = idset.iterator();
			while(idit.hasNext()){
				Object key = idit.next();
				if(! listenertable.containsKey(key))
					continue;
				Vector listeners = (Vector)(listenertable.get(key));
				for(int i=0; i < listeners.size(); i++)
					((ExceptionListener)(listeners.get(i))).receiveException(timestamp, exception);
			}
		}
	}
}
