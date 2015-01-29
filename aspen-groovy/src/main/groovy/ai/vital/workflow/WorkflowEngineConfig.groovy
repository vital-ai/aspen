/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Workflow engine configuration
 * @author Derek
 *
 */
public class WorkflowEngineConfig {

	private List<ConfProperty> properties;
	
	private List<WorkflowConfig> workflows = new ArrayList<WorkflowConfig>();

	private Boolean globalHashtableEnabled = true;
	
	public Boolean getGlobalHashtableEnabled() {
		return globalHashtableEnabled;
	}

	public void setGlobalHashtableEnabled(Boolean globalHashtableEnabled) {
		this.globalHashtableEnabled = globalHashtableEnabled;
	}

	public List<ConfProperty> getProperties() {
		return properties;
	}

	public void setProperties(List<ConfProperty> properties) {
		this.properties = properties;
	}
	
	private transient Map<String, String> propsMap = null;
	
	public Map<String, String> getPropertiesMap() {
		if(propsMap == null) {
			propsMap = new HashMap<String, String>();
			if(properties != null) {
				for(ConfProperty p : properties) {
					if(p.getName() == null || p.getName().isEmpty()) throw new RuntimeException("Empty or null property name");
					if(p.getValue() == null || p.getValue().isEmpty()) throw new RuntimeException("Empty or null property value");
					if(propsMap.containsKey(p.getName())) throw new RuntimeException("Duplicated property with name: " + p.getName());
					propsMap.put(p.getName(), p.getValue());
				}
			}
		}
		return propsMap;
	}	

	public List<WorkflowConfig> getWorkflows() {
		return workflows;
	}

	public void setWorkflows(List<WorkflowConfig> workflows) {
		this.workflows = workflows;
	}
	
}
