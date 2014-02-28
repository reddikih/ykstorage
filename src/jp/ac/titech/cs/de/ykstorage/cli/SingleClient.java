package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;

public class SingleClient {
	private final static Properties config = new Properties();
	private final boolean isConfigured = false;
	
	private static String path;
	
	private WorkloadReader workload;
	private int thread;
    private int port;
    private String hostName;
	
    public static SingleClient getInstance() {
    	return new SingleClient();
    }
    
    private SingleClient() {
    	startUp();
    	this.workload = new WorkloadReader(path);
    }
    
	private void startUp() {
        if (!isConfigured) try {
            config.load(new FileInputStream("./test/test/jp/ac/titech/cs/de/ykstorage/frontend/server_info.properties"));

            this.thread = Integer.parseInt(config.getProperty("server.info.threads"));
            this.hostName = config.getProperty("server.info.hostname");
            this.port = Integer.parseInt(config.getProperty("server.info.port"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
	
	public void start() {
		Socket conn;
		OutputStream out;
		
		while(workload.size() > 0) {
			try {
				conn = new Socket(hostName, port);
				out = conn.getOutputStream();
				
				Request req = workload.getRequest();
				byte[] request = req.getRequest();
				
				out.write(request);
				out.flush();
				
				ClientResponse response = new ClientResponse(conn);
				conn.close();
				
				long delay = (long) req.getArrivalTime();
				Thread.sleep(delay);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println("Usage: SingleClient <workload file>");
			System.exit(1);
		}
		path = args[0];
		
		SingleClient client = SingleClient.getInstance();
		client.start();
	}

}
