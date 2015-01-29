/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.config;

public class HttpConnector {

	private String host;
	
	private Integer port;

	private String restletAppClass;
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getRestletAppClass() {
		return restletAppClass;
	}

	public void setRestletAppClass(String restletAppClass) {
		this.restletAppClass = restletAppClass;
	}

}
