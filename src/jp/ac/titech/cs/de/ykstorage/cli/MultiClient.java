package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MultiClient {
	
	private final static Properties config = new Properties();
	private final boolean isConfigured = false;
	
	private static String path;
	
	private WorkloadReader workload;
	private int thread;
	private int port;
	private String hostName;
	
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
		ArrayList<ScheduledFuture<Response>> future = new ArrayList<ScheduledFuture<Response>>(workload.size());
		int requestCount = 0;
		int errorCount = 0;
		long totalDelay = 0L;
		long totalResponseTime = 0L;
		
		System.out.println("Start MultiClient. " + Calendar.getInstance().getTime().toString());
		long execStartTime = System.currentTimeMillis();
		
		while(workload.size() > 0) {
			Request req = workload.getRequest();
			requestCount++;
			totalDelay += req.getDelay();
			
			future.add(scheduler.schedule(new ClientTask(req, requestCount, hostName, port), totalDelay, TimeUnit.MILLISECONDS));
		}
		
		boolean isDone = false;
		while(!isDone) {
			isDone = true;
			for(ScheduledFuture<Response> f : future) {
				isDone &= f.isDone();
			}
		}
		
		for(ScheduledFuture<Response> f : future) {
			try {
				Response res = (Response) f.get();
				
				if(res.isError()) {
					errorCount++;
				} else {
					totalResponseTime += res.getResponseTime();
				}
				
				System.out.println(res.getMessage());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		try {
			scheduler.shutdown();
			boolean await = scheduler.awaitTermination(30, TimeUnit.SECONDS);
			boolean terminate = scheduler.isTerminated();
			if(!(await && terminate)) {
				scheduler.shutdownNow();
			}
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
		System.out.printf("Number of threads: %d\n", thread);
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
