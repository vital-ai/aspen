/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow.impl;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import ai.vital.workflow.IWorkflowStep;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowEngineConfig;

public abstract class WorkflowStepImpl<T extends WorkflowEngineConfig> implements IWorkflowStep<T> {

	protected T config;
	
	@Override
	public void init(T config) throws StepInitializationException {
		this.config = config;
	}
	
	@Override
	public void shutdown() {
		//default implementation does nothing - nothing to clean up
	}

	@Override
	public T getConfig() {
		return config;
	}

	protected <V extends Serializable> V getRequiredTypedParameter(Class<V> typeClass, Map<String, Serializable> ctx, String paramName) {
		
		Serializable v = ctx.get(paramName);
		if(v == null) throw new RuntimeException("No required context paramater: \"" + paramName + "\"");
		
		if(!(typeClass.isAssignableFrom(v.getClass()))) {
			throw new RuntimeException("The context parameter is not of type " + typeClass.getCanonicalName() + ": " + v.getClass().getCanonicalName() );
		}
		
		return typeClass.cast(v);
	}
	
	
	protected String getRequiredStringParameter(Map<String, Serializable> ctx, String paramName) {
		String value = getRequiredTypedParameter(String.class, ctx, paramName);
		if(value.isEmpty()) throw new RuntimeException("The context string parameter cannot be empty: " + paramName);
		return value;
	}		
	
	protected int getRequiredIntegerParameter(Map<String, Serializable> ctx, String paramName) {
		return getRequiredTypedParameter(Integer.class, ctx, paramName);
	}
	
	protected boolean getRequiredBooleanParameter(Map<String, Serializable> ctx, String paramName) {
		return getRequiredTypedParameter(Boolean.class, ctx, paramName);
	}
	
	protected Date getRequiredDateParameter(Map<String, Serializable> ctx, String paramName) {
		return getRequiredTypedParameter(Date.class, ctx, paramName);
	}

	protected long time() {return System.currentTimeMillis();}
	
	protected <V extends Serializable> V getOptionalTypedParameter(Class<V> typeClass, Map<String, Serializable> ctx, String paramName) {
		
		Serializable v = ctx.get(paramName);
		
		if(v == null) return null;
		
		if(!(typeClass.isAssignableFrom(v.getClass()))) {
			throw new RuntimeException("The context parameter is not of type " + typeClass.getCanonicalName() + ": " + v.getClass().getCanonicalName() );
		}
		
		return typeClass.cast(v);		
	}
	
	protected String getOptionalStringParameter(Map<String, Serializable> ctx, String paramName) {
		String optionalTypedParameter = getOptionalTypedParameter(String.class, ctx, paramName);
		if(optionalTypedParameter == null) return null;
		if(optionalTypedParameter.isEmpty()) return null;
		return optionalTypedParameter;
	}
	
	protected boolean getOptionalBooleanParameter(Map<String, Serializable> ctx, String paramName, boolean default_) {
		
		Boolean param = getOptionalTypedParameter(Boolean.class, ctx, paramName);
		
		if(param == null) return default_;
		
		return param;
		
	}
	
	protected Date getOptionalDateParameter(Map<String, Serializable> ctx, String paramName) {
		return getOptionalTypedParameter(Date.class, ctx, paramName);
	}

	
}
