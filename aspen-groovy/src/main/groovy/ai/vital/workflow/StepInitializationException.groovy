/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow;

/**
 * thrown when the workflow step couldn't be initialized
 * @author Derek
 *
 */
public class StepInitializationException extends Exception {

	private static final long serialVersionUID = 8409558333667775062L;

	public StepInitializationException() {
		super();
	}

	public StepInitializationException(String message, Throwable cause) {
		super(message, cause);
	}

	public StepInitializationException(String message) {
		super(message);
	}

	public StepInitializationException(Throwable cause) {
		super(cause);
	}

	
}
