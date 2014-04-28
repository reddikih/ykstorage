package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;
import jp.ac.titech.cs.de.ykstorage.frontend.RequestCommand;
import jp.ac.titech.cs.de.ykstorage.frontend.ResponseHeader;

public class ClientTask implements Callable<Response> {
	private final Request req;
	private int requestCount;
	private int port;
	private String hostName;
	
	public ClientTask(Request req, int requestCount, String hostName, int port) {
		this.req = req;
		this.requestCount = requestCount;
		this.hostName = hostName;
		this.port = port;
	}
	
	@Override
	public Response call() {
		ClientResponse response = null;
		StringBuffer message = new StringBuffer();
		long responseTime = 0L;
		boolean error = false;
		
		try {
			Socket conn = new Socket(hostName, port);
			OutputStream out = conn.getOutputStream();
			
			byte[] request = req.getRequest();
			
			message.append(String.format("%5d [%s] key:%d size:%d --- ", requestCount, req.getType(), req.getKey(), req.getSize()));
			
			long start = System.nanoTime();
			
			out.write(request);
			out.flush();
			
			response = new ClientResponse(conn);
			
			long end = System.nanoTime();
			responseTime = end - start;
			
			ResponseHeader respHeader = response.getHeader();
			message.append(String.format("%d ResponseTime: %.6f [s]", respHeader.getStatus(), (double) responseTime / 1000000000));

			conn.close();
		} catch (SocketException e) {
			e.printStackTrace();
			error = true;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		System.out.println(message);
		return new Response(response, message.toString(), responseTime, error);
	}
}
