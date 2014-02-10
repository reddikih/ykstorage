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
package org.streamspinner.wrapper;

import org.streamspinner.MetaDataUpdateListener;
import org.streamspinner.engine.Schema;

public class MetaDataUpdateNotificationThread extends Thread {

	private static final int CREATE = 0;
	private static final int DROP = 1;

	private int mode;
	private MetaDataUpdateListener listener;
	private String wrappername;
	private String tablename;
	private Schema schema;

	public MetaDataUpdateNotificationThread(MetaDataUpdateListener mdul, String wname, String tname, Schema s){
		mode = CREATE;
		listener = mdul;
		wrappername = wname;
		tablename = tname;
		schema = s;
	}

	public MetaDataUpdateNotificationThread(MetaDataUpdateListener mdul, String wname, String tname){
		this(mdul, wname, tname, null);
		mode = DROP;
	}

	public void run(){
		if(mode == CREATE)
			listener.tableCreated(wrappername, tablename, schema);
		else if(mode == DROP)
			listener.tableDropped(wrappername, tablename);
	}

}
