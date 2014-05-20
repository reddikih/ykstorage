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
package org.streamspinner.connection;

import java.rmi.Remote;
import java.rmi.RemoteException;
import org.streamspinner.engine.Schema;
import org.streamspinner.InternalState;

public interface RemoteStreamServer extends Remote {

	public long startQuery(String cq, Connection conn) throws RemoteException;

	public void stopQuery(long cid) throws RemoteException;

	public Schema[] getAllSchemas() throws RemoteException;

	public void addInformationSource(String confstr) throws RemoteException;

	public void removeInformationSource(String name) throws RemoteException;

	public void updateConnection(long cid, Connection conn) throws RemoteException;

	public void receiveAcknowledge(long cid, long seqid) throws RemoteException;

	public void addBackupServer(RemoteStreamServer rss) throws RemoteException ;

	public void removeBackupServer(RemoteStreamServer rss) throws RemoteException ;

	public long receivePing() throws RemoteException ;

	public InternalState getRemoteStreamServerInternalState() throws RemoteException ;

}
