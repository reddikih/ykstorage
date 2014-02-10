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
import org.streamspinner.query.*;
import org.streamspinner.engine.*;
import java.rmi.registry.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;

public class SystemManagerCUI implements SystemManager {

	public static final Pattern COMMAND = Pattern.compile("^\\s*(\\w+)(\\s+(.+))?\\s*$");
	public static final String DEFAULT_PROMPT = "Manager> ";

	private StreamSpinnerMainSystem system;
	private BufferedReader in;
	private BufferedWriter out;
	private String prompt = DEFAULT_PROMPT;
	private Hashtable<Object, Query> queries;

	public SystemManagerCUI(StreamSpinnerMainSystem ssms) {
		system = ssms;
		system.setSystemManager(this);
		in = new BufferedReader(new InputStreamReader(System.in));
		out = new BufferedWriter(new OutputStreamWriter(System.out));
		queries = new Hashtable<Object, Query>();
	}

	public void readInputs() throws StreamSpinnerException, IOException {
		String line = null;
		showWelcome();
		showPrompt();
		while((line = in.readLine()) != null){
			Matcher m = COMMAND.matcher(line);
			if(m.matches()){
				String command = m.group(1);
				String argument = m.group(3);
				if(command.equalsIgnoreCase("quit") || command.equalsIgnoreCase("exit")){
					doShutdown();
					break;
				}
				else if(command.equalsIgnoreCase("delete"))
					deleteWrapper(argument);
				else if(command.equalsIgnoreCase("load"))
					loadWrapper(argument);
				else if(command.equalsIgnoreCase("query"))
					showQueries();
				else if(command.equalsIgnoreCase("schema"))
					showSchemata();
				else if(command.equalsIgnoreCase("terminal")){
					CQTerminalCUI term = new CQTerminalCUI();
					term.readInputs();
				}
				else if(command.equalsIgnoreCase("wrapper"))
					showWrappers();
				else if(command.equalsIgnoreCase("help"))
					showHelp();
				else {
					out.write("Unknown command : " + command + "\n");
					showHelp();
				}
			}
			showPrompt();
		}
	}


	private void showPrompt() throws IOException {
		out.write(prompt);
		out.flush();
	}

	private void showWelcome() throws IOException {
		out.write("Welcome to StreamSpinner!!\n");
		out.write("Please type 'help' to show usage.\n");
		out.write("And, type 'quit' or 'exit' to exit streamspinner.\n");
	}

	private void showHelp() throws IOException {
		out.write("Usage: command [argument]\n");
		out.write("\n");
		out.write("Command:\n");
		out.write("  load file    load new wrapper from file\n");
		out.write("  delete name  delete wrapper\n");
		out.write("  query        display registered query\n");
		out.write("  schema       display available schema\n");
		out.write("  terminal     open CQTerminal\n");
		out.write("  wrapper      display available wrapper\n");
		out.write("  exit         shutdown streamspinner\n");
		out.write("  quit         shutdown streamspinner\n");
	}

	private void showWrappers() throws StreamSpinnerException, IOException {
		InformationSourceManager ism = system.getInformationSourceManager();
		InformationSource[] sources = ism.getAllInformationSources();

		out.write("Wrapper[tables]:\n");
		for(int i=0; sources != null && i < sources.length; i++){
			out.write("  " + sources[i].getName() + "\t[ ");
			String[] tablenames = sources[i].getAllTableNames();
			for(int j=0; tablenames != null && j < tablenames.length; j++){
				out.write(tablenames[j]);
				if(j+1 < tablenames.length)
					out.write(", ");
			}
			out.write(" ]\n");
		}
	}

	private void showSchemata() throws StreamSpinnerException, IOException {
		InformationSourceManager ism = system.getInformationSourceManager();
		Schema[] schemata = ism.getAllSchemas();
		out.write("Schema:\n");
		for(int i=0; schemata != null && i < schemata.length; i++)
			out.write("  " + schemata[i].toString() + "\n");
	}

	private void showQueries() throws IOException {
		out.write("Query:\n");
		for(Object key : queries.keySet()){
			Query q = queries.get(key);
			out.write("  " + q.toString() + "\n");
		}
	}

	private void loadWrapper(String argument) throws IOException {
		try {
			FileReader fr = new FileReader(argument);
			BufferedReader br = new BufferedReader(fr);
			StringBuilder data = new StringBuilder();
			String line = null;
			while((line = br.readLine()) != null)
				data.append(line);

			InformationSourceManager ism = system.getInformationSourceManager();
			ism.addInformationSource(data.toString());
			out.write("Succeeded\n");
		} catch(Exception e){
			out.write("Failed : " + e.getMessage() + "\n");
		}
	}

	private void deleteWrapper(String argument) throws IOException {
		try {
			InformationSourceManager ism = system.getInformationSourceManager();
			ism.removeInformationSource(argument);
			out.write("Succeeded\n");
		} catch(Exception e){
			out.write("Failed: " + e.getMessage() + "\n");
		}
	}

	private void doShutdown() throws StreamSpinnerException {
		system.shutdown();
	}

	public void queryRegistered(Query q){
		queries.put(q.getID(), q);
	}

	public void queryDeleted(Query q){
		queries.remove(q.getID());
	}

	public void dataDistributedTo(long timestamp, Set queryids, TupleSet ts){;}

	public void dataReceived(long timestamp, String source, TupleSet ts){;}

	public void executionPerformed(long executiontime, String master, long duration, long delay){;}


	public void startCacheConsumer(long timestamp, OperatorGroup og, double ratio){;}

	public void endCacheConsumer(long timestamp, OperatorGroup og, double ratio){;}

	public void informationSourceAdded(InformationSource is){;}

	public void informationSourceDeleted(InformationSource is){;}

	public void tableCreated(String wrappername, String tablename, Schema schema){;}

	public void tableDropped(String wrappername, String tablename){;}


	public static void main(String[] args){
		try {
			Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);

			StreamSpinnerMainSystemImpl ssms = new StreamSpinnerMainSystemImpl();
			SystemManagerCUI sm = new SystemManagerCUI(ssms);
			ssms.start();
			sm.readInputs();

		} catch(Exception e){
			e.printStackTrace();
		} finally {
			System.exit(0);
		}
	}
}
