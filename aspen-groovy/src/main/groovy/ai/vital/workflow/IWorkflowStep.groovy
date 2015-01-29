/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow;

import java.io.Serializable;
import java.util.Map;

import com.hp.hpl.jena.rdf.model.Model;

public interface IWorkflowStep<T extends WorkflowEngineConfig> {

	/**
	 * Returns name of this workflow.
	 * @return workflow name
	 */
	public String getName();

	/**
	 * Each workflow step operates on jena RDF model. The input model is mutable thus usually the returned object
	 * is the modified input instance.
	 * @param model
	 * @param context
	 * @return
	 * @throws WorkflowHaltException when workflow step halts the workflow
	 * @throws ProcessflowHaltException when workflow step halts the whole processflow
	 * @throws Exception on error
	 */
	public Model processModel(Model model, Map<String, Serializable> context) throws WorkflowHaltException, ProcessflowHaltException, Exception;

	public void init(T config) throws StepInitializationException;
	
	public T getConfig();
	
	public void shutdown();

	/**
	 * This exception when thrown halts the workflow 
	 */
	public static class WorkflowHaltException extends Exception {
		
		private static final long serialVersionUID = 5140065913304535140L;
		
		private Model model;

		/**
		 * Constructor for V2
		 */
		public WorkflowHaltException() {
			super();
		}

		public WorkflowHaltException(Model model) {
			super();
			this.model = model;
		}

		public Model getModel() {
			return model;
		}

		public void setModel(Model model) {
			this.model = model;
		}
		
	}
	
	
	/**
	 * This exception is thrown when the whole processflow should stop (no more processflow steps)
	 * There's a flag to indicate if the the current processflow step should complete all workflow steps or halt immediately.
	 */
	public static class ProcessflowHaltException extends Exception {
		
		private static final long serialVersionUID = 6828228163391473966L;

		private Model model;
		
		private boolean haltWorkflow;

		/**
		 * Constructor for V2
		 * @param haltWorkflow
		 */
		public ProcessflowHaltException(boolean haltWorkflow) {
			super();
			this.haltWorkflow = haltWorkflow;
		}

		public ProcessflowHaltException(Model model, boolean haltWorkflowSteps) {
			super();
			this.model = model;
			this.haltWorkflow = haltWorkflowSteps;
		}

		public Model getModel() {
			return model;
		}

		public void setModel(Model model) {
			this.model = model;
		}

		public boolean isHaltWorkflow() {
			return haltWorkflow;
		}

		public void setHaltWorkflow(boolean haltWorkflow) {
			this.haltWorkflow = haltWorkflow;
		}
		
	}
}
