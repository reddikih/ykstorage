package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Properties;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;
import jp.ac.titech.cs.de.ykstorage.frontend.RequestCommand;
import jp.ac.titech.cs.de.ykstorage.frontend.ResponseHeader;

public class SingleClient {

    private final static int INITIAL_COUNT = 0;

	private final static Properties config = new Properties();
	private final boolean isConfigured = false;
	
	private static String path;
	private static String configfile;
	
	private WorkloadReader workload;
	private int thread;
    private int port;
    private String hostName;

    private int requestCount;
    private long totalResponseTime;

    private int errorCount;
	
    public static SingleClient getInstance() {
    	return new SingleClient();
    }
    
    private SingleClient() {
    	startUp();
    	this.workload = new WorkloadReader(path);
    }
    
	private void startUp() {
        if (!isConfigured) try {
            config.load(new FileInputStream(configfile));

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

        System.out.println("Start SingleClient. " + Calendar.getInstance().getTime().toString());
        long execStartTime = System.currentTimeMillis();

		int reqCount = 0;
		while(workload.size() > 0) {
			try {
				conn = new Socket(hostName, port);
				out = conn.getOutputStream();
				
				Request req = workload.getRequest();
				byte[] request = req.getRequest();

                if (RequestCommand.READ.equals(req.getType())) {
                    System.out.printf("[%5d] [%s] key:%d --- ", reqCount, req.getType(), req.getKey());
                } else if (RequestCommand.WRITE.equals(req.getType())) {
                    System.out.printf("[%5d] [%s] key:%d size:%d --- ", reqCount, req.getType(), req.getKey(), req.getSize());
                }

                long start = System.nanoTime();
                reqCount++;

				out.write(request);
				out.flush();


                ClientResponse response = new ClientResponse(conn);

                long end = System.nanoTime();

                if (reqCount > INITIAL_COUNT) {
                    requestCount++;
                    totalResponseTime += (end - start);
                }

                ResponseHeader respHeader = response.getHeader();
                System.out.printf("%d ResponseTime: %.6f [s]\n", respHeader.getStatus(), (double)(end - start) / 1000000000);

				conn.close();

                long delay = req.getDelay();
				Thread.sleep(delay);
            } catch (SocketException e) {
                e.printStackTrace();
                errorCount++;
                System.err.println("Request count: " + reqCount);
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

        try {
            conn = new Socket(hostName, port);
            out = conn.getOutputStream();
            out.write(new byte[]{0x00, 0x11});
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long execEndTime = System.currentTimeMillis();

        System.out.println("----------------------------------------------------");
        System.out.printf("Total requests: %d\n", requestCount);
        System.out.printf("Error requests: %d\n", errorCount);
        System.out.printf("Average response time: %.6f [s]\n", (double) totalResponseTime / requestCount / 1000000000);
        System.out.printf("Execution time: %,.2f [s]\n", ((double)(execEndTime - execStartTime)) / 1000);
        System.out.println("----------------------------------------------------");
        System.out.println("End of the SingleClient. " + Calendar.getInstance().getTime().toString());
    }
	
	public static void main(String[] args) {
		if(args.length < 2) {
			System.out.println("Usage: SingleClient <config file> <workload file>");
			System.exit(1);
		}
		configfile = args[0];
		path = args[1];
		
		SingleClient client = SingleClient.getInstance();
		client.start();
	}

}
