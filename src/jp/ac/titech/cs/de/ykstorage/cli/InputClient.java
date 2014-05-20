package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;

public class InputClient {
	private final static Properties config = new Properties();
	private final boolean isConfigured = false;
	
	private int thread;
    private int port;
    private String hostName;
	
    public static InputClient getInstance() {
    	return new InputClient();
    }
    
    private InputClient() {
    	startUp();
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
		
		while(true) {
			try {
                System.out.print("command: ");
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String line = br.readLine();

                if (line == null || line.length() == 0) {
                    continue;
                }

				if(line.equals("exit")) {
					break;
				}

				byte[] request = createRequest(line).getRequest();
				
				conn = new Socket(hostName, port);
				out = conn.getOutputStream();
				
				out.write(request);
				out.flush();
				
				ClientResponse response = new ClientResponse(conn);
				
				conn.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (InputClientException e) {
                System.out.println(e.getMessage());
			}
		}
	}
	
	private Request createRequest(String line) throws InputClientException {
		Request request;
		String[] cmd = line.split(" ");
		
		if(cmd.length == 2 && (cmd[0].equalsIgnoreCase("GET"))) {
			byte[] type = {0x00, 0x01};
			long delay = 0L;
			long id = Long.parseLong(cmd[1]);
			request = new Request(type, delay, id);
		} else if(cmd.length == 3 && (cmd[0].equalsIgnoreCase("PUT"))) {
			byte[] type = {0x00, 0x10};
			long delay = 0L;
			long id = Long.parseLong(cmd[1]);
			int size = Integer.parseInt(cmd[2]);
			request = new Request(type, delay, id, size);
        } else if (cmd[0].equalsIgnoreCase("SHUTDOWN")) {
            request = new Request(new byte[]{0x00, 0x11}, 0, 0, 0);
		} else {
			throw new InputClientException("'" + line + "' is invalid command");
		}
		
		return request;
    }
	
	public static void main(String[] args) {
		InputClient client = InputClient.getInstance();
		client.start();
	}

}
