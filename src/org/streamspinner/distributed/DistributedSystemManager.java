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
package org.streamspinner.distributed;

import org.streamspinner.*;
import org.streamspinner.query.*;
import org.streamspinner.engine.*;
import java.rmi.*;
import java.util.*;

public interface DistributedSystemManager extends Remote {

	public void queryRegistered(String nodename, Query q) throws RemoteException;

	public void queryDeleted(String nodename, Query q) throws RemoteException;

	public void dataDistributedTo(String nodename, long timestamp, Set<Object> queryids) throws RemoteException;

	public void dataReceived(String nodename, long timestamp, String master) throws RemoteException;

	public void informationSourceAdded(String nodename, String wrappername, String[] tablenames) throws RemoteException;

	public void informationSourceDeleted(String nodename, String wrappername) throws RemoteException;

	public void connectionEstablished(String nodename, ConnectionInfo conn) throws RemoteException ;

	public void connectionClosed(String nodename, ConnectionInfo conn) throws RemoteException ;

	public void tableCreated(String nodename, String wrappername, String tablename) throws RemoteException;

	public void tableDropped(String nodename, String wrappername, String tablename) throws RemoteException;

}
