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
package org.streamspinner.optimizer;

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.StreamSpinnerMainSystem;
import org.streamspinner.SystemManager;
import org.streamspinner.InformationSourceManager;
import org.streamspinner.InformationSource;
import org.streamspinner.system.StreamSpinnerMainSystemImpl;
import org.streamspinner.query.CQFileFilter;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.Query;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.connection.DeliveryUnit;
import org.streamspinner.connection.Connection;
import org.streamspinner.connection.CQException;
import org.streamspinner.connection.RemoteStreamServer;
import java.util.ResourceBundle;
import java.util.Hashtable;
import java.util.Set;
import java.util.Iterator;
import java.util.Date;
import java.util.Vector;
import java.io.*;
import java.rmi.registry.*;
import java.rmi.*;
import java.text.SimpleDateFormat;

public class ExperimentRunner implements SystemManager {

	private StreamSpinnerMainSystemImpl system;
	private Hashtable connections;
	private String querydir;
	private BufferedWriter writer;
	private File outputfile;

	public ExperimentRunner(StreamSpinnerMainSystemImpl ssms) throws StreamSpinnerException {
		connections = new Hashtable();
		system = ssms;
		system.setSystemManager(this);

		ResourceBundle resource = system.getCurrentResourceBundle();

		querydir = resource.getString(StreamSpinnerMainSystem.PROPERTY_QUERYDIR);
		if(querydir == null || querydir.equals(""))
			throw new StreamSpinnerException("property '" + StreamSpinnerMainSystem.PROPERTY_QUERYDIR + "' is needed");

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		try {
			outputfile = new File("explog" + sdf.format(new Date()) + ".csv");
			FileWriter fw = new FileWriter(outputfile);
			writer = new BufferedWriter(fw);
			System.out.println("log file is " + outputfile.getPath());
		} catch(IOException ioe){
			throw new StreamSpinnerException(ioe);
		}

	}

	public void startExperiment() throws StreamSpinnerException {
		try {
			InformationSourceManager ism = system.getInformationSourceManager();
			ism.stopAllInformationSources();

			File dir = new File(querydir);
			if(! dir.isDirectory())
				throw new IOException(querydir + " is not directory");
			FilenameFilter ff = new CQFileFilter();

			File[] qfiles = dir.listFiles(ff);
			for(int i=0; i < qfiles.length; i++){
				String qstr = loadQuery(qfiles[i]);
				registQuery(qfiles[i].getName(), qstr);
			}

			system.start();
		} catch(StreamSpinnerException sse){
			throw sse;
		} catch(IOException ioe){
			throw new StreamSpinnerException(ioe);
		}
	}

	public void registQuery(String fname, String qstr) throws StreamSpinnerException {
		try {
			SimpleConnection conn = new SimpleConnection(fname);
			conn.setID(system.startQuery(qstr, conn));
			connections.put(conn, fname);
		} catch(RemoteException re){
			throw new StreamSpinnerException(re);
		}
	}

	public String loadQuery(File f) throws StreamSpinnerException {
		try {
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			StringBuffer sb = new StringBuffer();
			String line = null;
			while((line = br.readLine()) != null){
				sb.append(line);
				sb.append(" ");
			}
			br.close();
			fr.close();
			return sb.toString();
		} catch(IOException ioe){
			throw new StreamSpinnerException(ioe);
		}
	}


	public void stopExperiment() throws StreamSpinnerException {
		try {
			Set keys = connections.keySet();
			for(Iterator it = keys.iterator(); it.hasNext(); ){
				SimpleConnection conn = (SimpleConnection)(it.next());
				system.stopQuery(conn.getID());
			}

			if(writer != null){
				writer.flush();
				writer.close();
			}
		} catch(RemoteException re){
			throw new StreamSpinnerException(re);
		} catch(IOException ioe){
			throw new StreamSpinnerException(ioe);
		}
	}

	public void queryRegistered(Query q){
		try {
			writer.write("\"\",\"queryRegistered\",\"" + q.toString() + "\"\n");
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

	public void queryDeleted(Query q){
		try{
			writer.write("\"\",\"queryDeleted\",\"" + q.toString() + "\"\n");
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

	public void dataDistributedTo(long timestamp, Set queryids, TupleSet ts){
		try {
			writer.write("\"" + timestamp + "\",\"dataDistributedTo\",\"" +  queryids.toString() + "\"\n");
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

	public void dataReceived(long timestamp, String source, TupleSet ts){
		/*
		try {
			writer.write("\"" + timestamp + "\",\"dataReceived\",\"" + source + "\"\n");
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
		*/
	}

	public void informationSourceAdded(InformationSource is){
		;
	}

	public void informationSourceDeleted(InformationSource is){
		;
	}

	public void executionPerformed(long executiontime, String master, long duration, long delay) {
		try {
			writer.write("\"" + executiontime + "\",\"executionPerformed\",\"" + master + "\",\"" + duration + "\",\"" + delay + "\"\n");
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

	public class SimpleConnection implements Connection {

		private String filename;
		private long id = -1;

		public SimpleConnection(String filename){
			this.filename = filename;
		}

		public void start() throws RemoteException { ; }
		public void stop() throws RemoteException { ; }
		public void receiveSchema(Schema scm) throws RemoteException { ; }
		public void receiveCQException(CQException cqe) throws RemoteException { ; }
		public void receiveRecoveredUnits(long newseqno, DeliveryUnit[] units) throws RemoteException { ; }

		public long receiveDeliveryUnit(DeliveryUnit du) throws RemoteException {
			try {
				writer.write("\"\",\"receiveTuple\",\"" + filename + "\",\"" + du.getTuples().size() + "\"\n");
			} catch(IOException ioe){
				ioe.printStackTrace();
			}
			return du.getSequenceNumber();
		}

		public void setID(long id){
			this.id = id;
		}

		public long getID(){
			return id;
		}

		public void reconnect(RemoteStreamServer rss) throws RemoteException { ; }
	}

	public void startCacheConsumer(long timestamp, OperatorGroup og, double ratio) {
		try {
			writer.write("\""+timestamp+"\",\"startCacheConsumer\",\""+ og.toString()+"\",\""+ratio+"\"\n");
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

	public void endCacheConsumer(long timestamp, OperatorGroup og, double ratio) {
		try {
			writer.write("\""+timestamp+"\",\"endCacheConsumer\",\""+ og.toString()+"\",\""+ratio+"\"\n");
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

	public void tableCreated(String wrappername, String tablename, Schema schema){
		;
	}

	public void tableDropped(String wrappername, String tablename){
		;
	}

	public static void main(String[] args){
		StreamSpinnerMainSystemImpl sys = null;
		try {
			Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
			sys = new StreamSpinnerMainSystemImpl();
			ExperimentRunner er = new ExperimentRunner(sys);
			er.startExperiment();

			Thread.sleep(10 * 60 * 1000);

			er.stopExperiment();

			sys.shutdown();
			System.exit(0);

		} catch(Exception e1){
			e1.printStackTrace();
			if(sys != null){
				try {
					sys.shutdown();
				} catch(Exception e2){
					e2.printStackTrace();
				}
			}
			System.exit(0);
		}
	}
}

