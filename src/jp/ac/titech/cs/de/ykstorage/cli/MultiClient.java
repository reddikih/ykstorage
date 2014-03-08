package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;
import jp.ac.titech.cs.de.ykstorage.frontend.RequestCommand;
import jp.ac.titech.cs.de.ykstorage.frontend.ResponseHeader;

public class MultiClient {
	
	private final static Properties config = new Properties();
	private final boolean isConfigured = false;
	
	private static String path;
	
	private WorkloadReader workload;
	private int thread;
	private int port;
	private String hostName;
	
	private int requestCount;
	private long totalResponseTime;
	
	private int errorCount;
	
	private ScheduledExecutorService scheduler;
	
	public static MultiClient getInstance() {
		return new MultiClient();
	}
	
	private MultiClient() {
		startUp();
		this.workload = new WorkloadReader(path);
	}
	
	private void startUp() {
		if (!isConfigured) try {
			config.load(new FileInputStream("./test/test/jp/ac/titech/cs/de/ykstorage/frontend/server_info.properties"));
			
			this.thread = Integer.parseInt(config.getProperty("server.info.threads"));
			this.hostName = config.getProperty("server.info.hostname");
			this.port = Integer.parseInt(config.getProperty("server.info.port"));
			
			this.scheduler = Executors.newScheduledThreadPool(this.thread);
			
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void start() {
		ArrayList<ScheduledFuture> future = new ArrayList<ScheduledFuture>(workload.size());
		requestCount = 0;
		
		System.out.println("Start MultiClient. " + Calendar.getInstance().getTime().toString());
		long execStartTime = System.currentTimeMillis();
		
		while(workload.size() > 0) {
			Request req = workload.getRequest();
			requestCount++;
			
			future.add(scheduler.schedule(new ClientTask(req, requestCount, hostName, port), req.getDelay(), TimeUnit.MILLISECONDS));
		}
		
		boolean isDone = false;
		while(!isDone) {
			isDone = true;
			for(ScheduledFuture f : future) {
				isDone &= f.isDone();
			}
//			ResponseHeader respHeader = response.getHeader();
		}
		
		try {
			scheduler.shutdown();
			boolean await = scheduler.awaitTermination(10, TimeUnit.SECONDS);
			System.out.println("await: " + await);
			boolean terminate = scheduler.isTerminated();
			System.out.println("terminate: " + terminate);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		try {
			Socket conn = new Socket(hostName, port);
			OutputStream out = conn.getOutputStream();
			out.write(new byte[]{0x00, 0x11});
			out.flush();
			out.close();
			conn.close();
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
		System.out.println("End of the MultiClient. " + Calendar.getInstance().getTime().toString());
	}
	
	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println("Usage: MultiClient <workload file>");
			System.exit(1);
		}
		path = args[0];
		
		MultiClient client = MultiClient.getInstance();
		client.start();
	}

}
