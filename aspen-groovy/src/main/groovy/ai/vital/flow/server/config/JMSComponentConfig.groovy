/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.config;

public class JMSComponentConfig {

//	private String connectionFactoryNames = "QueueCF";
//
//	private String contextFactoryImpl = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
//	
//	private String securityPrincipal = "system";
//	
//	private String securityCredentials = "manager";
	
	private Boolean enabled = true;
	
	private String brokerURL = null;

	// How often to attempt a reconnect when JMS server seems to have gone away
	private Integer connectionRetry = 5;
	
	private String queueName = null;
	
	private String highPriorityQueueName = null;
	
//	public String getConnectionFactoryNames() {
//		return connectionFactoryNames;
//	}
//
//	public void setConnectionFactoryNames(String connectionFactoryNames) {
//		this.connectionFactoryNames = connectionFactoryNames;
//	}
//
//	public String getContextFactoryImpl() {
//		return contextFactoryImpl;
//	}
//
//	public void setContextFactoryImpl(String contextFactoryImpl) {
//		this.contextFactoryImpl = contextFactoryImpl;
//	}
//
//	public String getSecurityPrincipal() {
//		return securityPrincipal;
//	}
//
//	public void setSecurityPrincipal(String securityPrincipal) {
//		this.securityPrincipal = securityPrincipal;
//	}
//
//	public String getSecurityCredentials() {
//		return securityCredentials;
//	}
//
//	public void setSecurityCredentials(String securityCredentials) {
//		this.securityCredentials = securityCredentials;
//	}

	public String getBrokerURL() {
		return brokerURL;
	}


	public Boolean getEnabled() {
		return enabled;
	}

	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	public void setBrokerURL(String brokerURL) {
		this.brokerURL = brokerURL;
	}
	
	public Integer getConnectionRetry() {
		return connectionRetry;
	}
	
	public void setConnectionRetry(Integer connectionRetry) {
		this.connectionRetry = connectionRetry;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}


	public String getHighPriorityQueueName() {
		return highPriorityQueueName;
	}


	public void setHighPriorityQueueName(String highPriorityQueueName) {
		this.highPriorityQueueName = highPriorityQueueName;
	}

}
