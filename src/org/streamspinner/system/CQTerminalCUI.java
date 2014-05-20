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

import org.streamspinner.connection.*;
import java.io.*;

public class CQTerminalCUI implements CQRowSetListener, CQControlEventListener {

	public static final String DEFAULT_PROMPT = "CQTerminal> ";
	public static final String DEFAULT_URL = "rmi://localhost/StreamSpinnerServer";

	private CQRowSet rs;
	private BufferedReader in;
	private BufferedWriter out;
	private boolean running;
	private boolean schema;
	private String rmilocation;
	private String host;
	private String prompt;

	public CQTerminalCUI() throws IOException {
		this("localhost");
	}

	public CQTerminalCUI(String hostname) throws IOException {
		InputStreamReader isr = new InputStreamReader(System.in);
		in = new BufferedReader(isr);

		OutputStreamWriter osw = new OutputStreamWriter(System.out);
		out = new BufferedWriter(osw);

		init(hostname);
	}

	public CQTerminalCUI(String hostname, BufferedReader i, BufferedWriter o){
		in = i;
		out = o;
		init(hostname);
	}

	private void init(String hostname){
		rs = new DefaultCQRowSet();
		rs.addCQRowSetListener(this);
		rs.addCQControlEventListener(this);
		host = hostname;
		if(host != null && (! host.equals("")))
			rmilocation = "rmi://"+host+"/StreamSpinnerServer";
		else
			rmilocation = DEFAULT_URL;
		prompt = "CQTerminal@"+ host + "> ";
		rs.setUrl(rmilocation);
		running = false;
		schema = false;
	}


	public void readInputs() throws IOException {
		showWelcome();
		showPrompt();

		String line = null;
		while((line = in.readLine()) != null){
			if(line.matches("^\\s*[qQ][uU][iI][tT]\\s*$") || line.matches("^\\s*[eE][xX][iI][tT]\\s*$")){
				rs = null;
				break;
			}
			if(running == true)
				stop();
			else
				start(line);
		}
	}

	private void showWelcome() throws IOException {
		out.write("CQTerminal: please input query.\n");
		out.write("Type 'quit' or 'exit' to exit terminal.\n");
	}

	private void showPrompt() throws IOException {
		out.write(prompt);
		out.flush();
	}

	private void start(String query) throws IOException {
		if(query == null || query.equals("")){
			showPrompt();
			return;
		}
		try {
			rs.setCommand(query);
			rs.start();
			running = true;
			out.write("Query is registered. Press enter to stop the query.\n");
			out.flush();
		} catch(Exception e){
			out.write(e.getMessage() + "\n");
			running = false;
			showPrompt();
		}
	}

	private void stop() throws IOException {
		try {
			rs.stop();
		} catch(Exception e){
			out.write(e.getMessage());
		}
		init(host);
		out.write("Stopped\n");
		showPrompt();
	}

	public void errorOccurred(CQControlEvent event){
		try {
			out.write(event.getCQException().getMessage());
			out.flush();
			if(running == true)
				stop();
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

	public void dataDistributed(CQRowSetEvent re){
		try {
			CQRowSet rs = (CQRowSet)(re.getSource());
			if(rs.first() != true)
				return;
			rs.beforeFirst();
			CQRowSetMetaData rsmd = rs.getMetaData();
			if(schema == false){
				for(int i=1; i <= rsmd.getColumnCount(); i++){
					out.write(rsmd.getColumnName(i));
					if(i + 1 <= rsmd.getColumnCount())
						out.write("\t");
				}
				out.newLine();
				schema = true;
			}
			while(rs.next()){
				for(int i=1; i <= rsmd.getColumnCount(); i++){
					out.write(rs.getString(i));
					if(i + 1 <= rsmd.getColumnCount())
						out.write("\t");
				}
				out.newLine();
			}
			out.flush();
		} catch(Exception e){
			try {
				out.write(e.getMessage() + "\n");
				stop();
			} catch(IOException ioe){
				ioe.printStackTrace();
			}
		}
	}

	public static void main(String[] args){
		try {
			CQTerminalCUI term = null;
			if(args != null && args.length == 1)
				term = new CQTerminalCUI(args[0]);
			else
				term = new CQTerminalCUI();
			term.readInputs();
		} catch(Exception e){
			e.printStackTrace();
		}
		System.exit(0);
	}
}
