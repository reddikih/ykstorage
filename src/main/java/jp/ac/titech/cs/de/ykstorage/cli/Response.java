package jp.ac.titech.cs.de.ykstorage.cli;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;

public class Response {
	private ClientResponse response;
	private String message;
	/*
	 * NanoSeconds
	 */
	private long responseTime;
	private boolean error;
	
	public Response(ClientResponse response, String message, long responseTime, boolean error) {
		this.response = response;
		this.message = message;
		this.responseTime = responseTime;
		this.error = error;
	}
	
	public ClientResponse getResponse() {
		return response;
	}
	
	public String getMessage() {
		return message;
	}
	
	public long getResponseTime() {
		return responseTime;
	}
	
	public boolean isError() {
		return error;
	}
}
