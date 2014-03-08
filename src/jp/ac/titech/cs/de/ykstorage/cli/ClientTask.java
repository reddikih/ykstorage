package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;
import jp.ac.titech.cs.de.ykstorage.frontend.RequestCommand;
import jp.ac.titech.cs.de.ykstorage.frontend.ResponseHeader;

public class ClientTask implements Runnable {
	private final Request req;
	private int port;
	private String hostName;
	
	public ClientTask(Request req, String hostName, int port) {
		this.req = req;
		this.hostName = hostName;
		this.port = port;
	}
	
	@Override
	public void run() {
		Socket conn;
		OutputStream out;
		
		try {
			conn = new Socket(hostName, port);
			out = conn.getOutputStream();
			
			byte[] request = req.getRequest();
			
			if (RequestCommand.READ.equals(req.getType())) {
				//System.out.printf("[%s] thread: %ld key:%d --- ", req.getType(), Thread.currentThread().getId(), req.getKey());
				System.out.printf("[%s] key:%d --- ", req.getType(), req.getKey());
			} else if (RequestCommand.WRITE.equals(req.getType())) {
				//System.out.printf("[%s] thread: %ld key:%d size:%d --- ", req.getType(), Thread.currentThread().getId(), req.getKey(), req.getSize());
				System.out.printf("[%s] key:%d size:%d --- ", req.getType(), req.getKey(), req.getSize());
			}
			
			long start = System.nanoTime();
			
			out.write(request);
			out.flush();
			
			ClientResponse response = new ClientResponse(conn);
			
			long end = System.nanoTime();
			
			ResponseHeader respHeader = response.getHeader();
			System.out.printf("%d ResponseTime: %.6f [s]\n", respHeader.getStatus(), (double)(end - start) / 1000000000);
			
			conn.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
