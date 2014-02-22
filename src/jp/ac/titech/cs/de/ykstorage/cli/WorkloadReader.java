package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class WorkloadReader {
	private BufferedReader reader;
	private ArrayList<Request> requests = new ArrayList<Request>();

    public WorkloadReader(String filePath) {
        init(filePath);
        createRequest();
    }

    private void init(String filePath) {
        try {
            reader = new BufferedReader(new FileReader(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    private void createRequest() {
		try {
			String line;
			while((line = reader.readLine()) != null) {
				String[] cmd = line.split(",");
				
				if(cmd.length == 3 && (cmd[0].equalsIgnoreCase("GET"))) {
					byte[] type = {0x00, 0x01};
					int arrival = Integer.parseInt(cmd[1]);
					long id = Long.parseLong(cmd[2]);
					requests.add(new Request(type, arrival, id));
				} else if(cmd.length == 4 && (cmd[0].equalsIgnoreCase("PUT"))) {
					byte[] type = {0x00, 0x10};
					int arrival = Integer.parseInt(cmd[1]);
					long id = Long.parseLong(cmd[2]);
					int size = Integer.parseInt(cmd[3]);
					requests.add(new Request(type, arrival, id, size));
				}			
			}
			
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
    }
    
    public Request getRequest() {
    	return requests.remove(0);
    }
    
    public int size() {
    	return requests.size();
    }
    
}