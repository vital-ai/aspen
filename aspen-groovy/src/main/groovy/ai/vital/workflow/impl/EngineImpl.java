/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow.impl;


import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasToken;
import ai.vital.vitalsigns.global.GlobalHashTable;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.PropertyImplementation;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.vitalsigns.model.container.ProcessFlowStep;
import ai.vital.vitalsigns.model.container.WorkflowStep;
import ai.vital.workflow.IEngine;
import ai.vital.workflow.IWorkflowStep;
import ai.vital.workflow.IWorkflowStep.ProcessflowHaltException;
import ai.vital.workflow.IWorkflowStep.WorkflowHaltException;
import ai.vital.workflow.IWorkflowStepV2;
import ai.vital.workflow.StepsPackageProvider;
import ai.vital.workflow.Workflow;
import ai.vital.workflow.WorkflowConfig;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.classes.ClassesFinder;
import ai.vital.workflow.WorkflowEngineConfig;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;

public abstract class EngineImpl<T extends WorkflowEngineConfig> implements IEngine<T> {

	private static final String VITAL_STEPS_PKGS = "VITAL_STEPS_PKGS";

	private final static Logger log = LoggerFactory.getLogger(EngineImpl.class);
	
	private Map<String, Workflow> workflows = new HashMap<String, Workflow>(); 
	
	private Map<String, IWorkflowStep<T>> stepsMap = new HashMap<String, IWorkflowStep<T>>();
	
	protected T config;
	
//	private VitalClassLoader vitalClassLoader;
	
	private Set<String> stepsPackages = new HashSet<String>(); 
	
	
	public boolean registerStepPackage(StepsPackageProvider stepsPackageProvider) {
		return stepsPackages.add(stepsPackageProvider.getStepsPackageName());
	}
	
	/**
	 * A subclass should override it if an engine comes with predefined packages with steps
	 * @return
	 */
	protected String[] getBaseStepsPackages() {
		return new String[0];
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void initEngine(T config) throws Exception {
		
		String pkgs = System.getenv(VITAL_STEPS_PKGS);
		//loading packages
		
		String[] basePackages = getBaseStepsPackages();
		log.info("Base engine steps packages: {}", Arrays.asList(basePackages));
		
		for(String p : basePackages) {
			stepsPackages.add(p);
		}
			
		
		log.info(VITAL_STEPS_PKGS + " environment property: {}", pkgs);
		
		
		if(pkgs!=null) {
			String[] pkgsA = pkgs.split(",");
			for(String p : pkgsA) {
				log.info("Adding package provider from environment property: {}", p);
				stepsPackages.add(p);
			}
		}
		
		ServiceLoader<StepsPackageProvider> loader = ServiceLoader.load(StepsPackageProvider.class);
		
		for(Iterator<StepsPackageProvider> i = loader.iterator(); i.hasNext(); ) {
			StepsPackageProvider p = i.next();
			String pkg = p.getStepsPackageName();
			boolean added = registerStepPackage(p);
			log.info("Registering dynamic steps package provider: {}, already exists: {}", pkg, !added);
			
		}
		
		
		Set<Class<IWorkflowStep<T>>> classes = new HashSet<Class<IWorkflowStep<T>>>();
		
		log.info("Total steps packages {}: {}", stepsPackages.size(), stepsPackages.toString());
		
		for(String pkg : stepsPackages) {
			
			log.info("Looking for step classes in {}", pkg);
			
			List<Class<IWorkflowStep>> classesX = ClassesFinder.findClasses(IWorkflowStep.class, pkg);
			
			log.info("Found {} step classes in package: {}", classesX.size(), pkg);
			
			for(Class c : classesX) {
				classes.add(c);
			}
			
		}
		
		
		//collect all workflow steps implementation using reflection
		/*
		Package package1 = this.getClass().getPackage();
		//start looking at package parent
		String name = package1.getName();
		int lastIndexOf = name.lastIndexOf('.');
		String root = "";
		if(lastIndexOf >= 0) {
			root = name.substring(0, lastIndexOf);
		} else {
			root = name;
		}
		
		List<Class<? extends IWorkflowStep<T>>> classes = new ArrayList<Class<? extends IWorkflowStep<T>>>();
		
		List<Class<IWorkflowStep>> classesX = ClassesFinder.findClasses(IWorkflowStep.class, root);
		for(Class cls : classesX) {
			classes.add(cls);
		}
		
		List<Class<IWorkflowStep>> classesY = ClassesFinder.findClasses(IWorkflowStep.class, "");
		for(Class cls : classesY) {
			if(!classes.contains(cls)) {
				classes.add(cls);
			}
		}
		*/
		
		log.info("Found {} V1 workflow step classes, registering steps...", classes.size());
		
		Set<String> initialized = new HashSet<String>();
		
		for(Class<? extends IWorkflowStep<T>> c : classes) {
			
			if( Modifier.isAbstract(c.getModifiers()) ) {
				log.warn("Workflow step class is abstract: {} - skipping...", c.getCanonicalName());
				continue;
			}
			
			long start = System.currentTimeMillis();
			
			log.info("Registering and initializing {} ...", c.getCanonicalName());
			
			IWorkflowStep<T> newInstance = c.newInstance();
			
			
			try {
				
				long stop = System.currentTimeMillis();
				
//				newInstance.init(config);
				stepsMap.put(newInstance.getName(), newInstance);
				log.info("Registered {}, {}ms", c.getCanonicalName(), stop-start);
				
			} catch(Exception e){
				log.error(e.getLocalizedMessage(), e);
			}
			
			
			
		}
		

		/*XXX no vital classloader
		log.info("Creating new vital class loader...");
		
		vitalClassLoader = new VitalClassLoader(new GroovyClassLoaderFactory() {
			
			@Override
			public AppGroovyClassLoader createNewClassLoader(VitalClassLoader _parent,
					String _package) {
				return new AppGroovyClassLoader(_parent, _package);
			}
		});
		*/
		
		/*
		VitalSigns.get().setCustomClassLoader(vitalClassLoader);
		
		String jarsDirPath = config.getDynamicJarsDir();
		if(jarsDirPath == null || jarsDirPath.isEmpty()) throw new Exception("No dynamic jars directory in config!");
		
		File jarsDir = new File(jarsDirPath);
		if(!jarsDir.exists()) throw new Exception("Dynamic jars path does not exist: " + jarsDir.getAbsolutePath());
		if(!jarsDir.isDirectory()) throw new Exception("Dynamic jars path is not a directory: " + jarsDir.getAbsolutePath());
		if(!jarsDir.canExecute()) throw new Exception("Dynamic jars dir not executable: " + jarsDir.getAbsolutePath());
		if(!jarsDir.canWrite()) throw new Exception("Dynamic jars dir not writable: " + jarsDir.getAbsolutePath());
		if(!jarsDir.canRead()) throw new Exception("Dynamic jars dir not readable: " + jarsDir.getAbsolutePath());
		
		File[] dynamicJars = jarsDir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File arg0, String arg1) {
				return arg1.endsWith(".jar");
			}
		});
		
		log.info("Dynamic (app) jars found: {}", dynamicJars.length);

		
		for(File f : dynamicJars) {
		
			if( ! f.getName().endsWith(".jar") ) continue;
			
			String appID = f.getName().substring(0, f.getName().length() - 4);
			
			log.info("Loading jar for appID: {}, path: {}", appID, f.getAbsolutePath());
			
			if( vitalClassLoader.getAppClassLoader(appID) != null) {
				log.info("Previous jar version exists - unloading...");
				vitalClassLoader.unloadJar(appID);
			}
			
			vitalClassLoader.loadJar(appID, new ZipInputStream(new FileInputStream(f)));

			log.info("Jar {} loaded", f.getAbsolutePath());
			
		}
		
		log.info("");
		*/
		
//		rootPackage.
//		
//		IWorkflowStep.class.get
		
		
		//first iteration is just to make sure all steps are there - it will exit immediately if it's not true
		for( WorkflowConfig c : config.getWorkflows()) {
			
			String workflowName = c.getName();
			
			List<StepName> steps = c.getSteps();
			
//			Set<String> stepsSet = new HashSet<String>();
//			
//			for(String s: steps) {
//				
//				if(!stepsSet.add(s.toLowerCase())) {
//					throw new Exception("Duplicate step name: ");
//				}
//				
//			}
			
			if(steps.size() < 1) throw new Exception("Wokflow: " + workflowName + " - the steps list must be of lenght > 0 !");
			
			for(StepName stepName : steps) {
				
				String step = stepName.getName();
				
				IWorkflowStep<T> iWorkflowStep = stepsMap.get(step);

				if(iWorkflowStep == null) throw new Exception("Workflow: " + workflowName + " - workflow step with name: " + step + " not found!");
			}		
		}
		
		for( WorkflowConfig c : config.getWorkflows()) {
			
			String workflowName = c.getName();
			
			List<StepName> steps = c.getSteps();
			
//			Set<String> stepsSet = new HashSet<String>();
//			
//			for(String s: steps) {
//				
//				if(!stepsSet.add(s.toLowerCase())) {
//					throw new Exception("Duplicate step name: ");
//				}
//				
//			}
			
			if(steps.size() < 1) throw new Exception("The steps list must be of lenght > 0 !");
			
			Workflow w = new Workflow();
			w.setName(workflowName);
			List<IWorkflowStep> stepsList = new ArrayList<IWorkflowStep>();
			
			for(StepName stepName : steps) {
				
				String step = stepName.getName();
				
				IWorkflowStep<T> iWorkflowStep = stepsMap.get(step);

				if(iWorkflowStep == null) throw new Exception("Workflow step with name: " + step + " not found!");
				
				if(initialized.add(iWorkflowStep.getName())) {
					
					log.info("Step not initialized yet - initializing - " + step);
					iWorkflowStep.init(config);
					
				} else {
					
					log.info("Step already initialized: " + step);
					
				}
				
				stepsList.add(iWorkflowStep);
				
			}
			
			w.setSteps(stepsList);
			
			workflows.put(workflowName, w);
			
//			Class<?> clazz = Class.forName(steps);
//			
//			if( ! IWorkflow.class.isAssignableFrom(clazz)) throw new Exception("The implementation class: " + clazz.getCanonicalName() + " does not implement " + IWorkflow.class.getCanonicalName() + " interface.");
//
//			IWorkflow iWorkflow = workflows.get(workflowName);
//			
//			if(iWorkflow != null) throw new Exception("Duplicated implementation class for workflow name: " + workflowName +" - " + iWorkflow.getClass().getCanonicalName() + " vs. " + clazz.getCanonicalName());
//
//			IWorkflow<T> newInstance = (IWorkflow<T>) clazz.newInstance();
//			
//			newInstance.setConfig(config);
//			
//			workflows.put(workflowName, (IWorkflow) newInstance);
			
		}
		
		this.config = config;
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Model processModel(String workflow, Model inputModel,
			Map<String, Serializable> context) throws Exception, ProcessflowHaltException {

		Workflow iWorkflow = workflows.get(workflow);
		
		if(iWorkflow == null) throw new Exception("Workflow with name:\"" + workflow + "\" not found.");

		List<IWorkflowStep> steps = iWorkflow.getSteps();
		
		ProcessflowHaltException haltProcessEx = null;
		
		for(IWorkflowStep step : steps) {
			
			
			log.info("Processing with step: {}", step.getName());
			
			long stepStart = System.currentTimeMillis();
			
			
			//listener
			try {
				inputModel = step.processModel(inputModel, context);
			} catch(WorkflowHaltException e) {
				log.warn("Step {} halted the workflow.",  step.getName() );
				inputModel = e.getModel();
				if(inputModel == null) {
					log.error("On halt exception the model has to be set!");
					throw new NullPointerException("On halt exception the model has to be set!");
				}
				break;
			} catch(ProcessflowHaltException e) {
				log.warn("Step {} halted the processflow (halt workflow ? {}.", step.getName(), e.isHaltWorkflow());
				haltProcessEx = e;
				inputModel = e.getModel();
				if( e.isHaltWorkflow() ) {
					log.warn("Halting the workflow.");
					break;
				}
			}

			long stepStop = System.currentTimeMillis();
			log.info("Processed with step: {} model version, {}ms", new Object[]{ step.getName(), stepStop - stepStart });
			
		}
		
		if(haltProcessEx != null) {
			log.warn("Halting processflow now...");
			haltProcessEx.setModel(inputModel);
			throw haltProcessEx;
		}
		
		
		
//		return iWorkflow.processModel(inputModel, context);
		return inputModel;
		
	}

	@Override
	public void shutdown() {
		
		log.info("Shutting down engine {} with {} workflow steps", this.getClass().getCanonicalName(), stepsMap.size());
		
		for( Iterator<IWorkflowStep<T>> iterator = stepsMap.values().iterator(); iterator.hasNext(); ) {
			
			IWorkflowStep<T> next = iterator.next();
			
			log.info("Shutting down step: {} ({})", next.getName(), next.getClass().getCanonicalName());
			
			try {
				next.shutdown();
			} catch(Exception e){
				log.error(e.getLocalizedMessage(), e);
			}
			
		}
		
	}
	
	@Override
	public void processPayload(String workflow, Payload payload, ProcessFlowStep flowStep) throws Exception {
		
		Workflow iWorkflow = workflows.get(workflow);
		
		if(iWorkflow == null) throw new Exception("Workflow with name:\"" + workflow + "\" not found.");

		List<IWorkflowStep> steps = iWorkflow.getSteps();
		
		ProcessflowHaltException haltProcessEx = null;
		
		List<WorkflowStep> stepsStatus = new ArrayList<WorkflowStep>(steps.size());
		
		//initially insert all objects into hashtable
		if( config.getGlobalHashtableEnabled() == null || Boolean.TRUE.equals( config.getGlobalHashtableEnabled() ) ) {
			GlobalHashTable.get().putAll(payload.getAllObjects());
		}
		
		for(IWorkflowStep step : steps) {
			
			if( ! ( step instanceof IWorkflowStepV2) ) throw new Exception("The step was expected to be of type: " + IWorkflowStepV2.class.getCanonicalName());

			IWorkflowStepV2 stepV2 = (IWorkflowStepV2) step;
			
			log.info("Processing with step: {} payload: {}", stepV2.getName(), payload.URI);
			
			long stepStart = System.currentTimeMillis();
			
			//listener
			
			boolean _break = false;
			
			try {
				
				stepV2.processPayload(payload);
				
				//refresh the global objects cache after each run
				if(config.getGlobalHashtableEnabled() == null || Boolean.TRUE.equals(config.getGlobalHashtableEnabled())) {
					GlobalHashTable.get().purge();
					GlobalHashTable.get().putAll(payload.getAllObjects());
				}
				
			} catch(WorkflowHaltException e) {
				log.warn("Step {} halted the workflow.",  step.getName() );
				_break = true;
				
				flowStep.setMessage("Workflow halted by : " + step.getName());
				
			} catch(ProcessflowHaltException e) {
				log.warn("Step {} halted the processflow (halt workflow ? {}.", step.getName(), e.isHaltWorkflow());
				haltProcessEx = e;
				
				flowStep.setMessage("Processflow halted by : " + step.getName() + " with current workflow ? " + e.isHaltWorkflow() );
				
				if( e.isHaltWorkflow() ) {
					log.warn("Halting the workflow.");
					_break = true;
				}
			}
			
			long stepStop = System.currentTimeMillis();
			log.info("Processed with step: {} payload: {}, {}ms", new Object[]{ stepV2.getName(), payload.URI, stepStop - stepStart });

			WorkflowStep ws = new WorkflowStep();
			ws.setName(step.getName());
			ws.setExitTime(new Date());
			stepsStatus.add(ws);
			
			if(_break) break;
			
		}
		
		flowStep.setSteps(stepsStatus);
		
		for(Iterator<Document> iter = payload.iterator(Document.class); iter.hasNext();) {
			Document doc = iter.next();
			//java side, set the property explicitly
			String statusShortProperty = getStatusShortPropertyName();
			Property statusProperty = getStatusProperty();
			
			//TODO
			//PropertyImplementation p1 = new PropertyImplementation(statusShortProperty, statusProperty.getURI(), String.class);
			// p1.setValue("OK");
			
			String timestampShortProperty = getTimestampShortPropertyName();
			Property timestampProperty = getTimestampProperty();
			
			//TODO
			//PropertyImplementation p2 = new PropertyImplementation(timestampShortProperty, timestampProperty.getURI(), Long.class);
			//p2.setValue(System.currentTimeMillis());
			
			//doc.getProperties().put(p1.getShortName(), p1);
			//doc.getProperties().put(p2.getShortName(), p2);
		}
		
		Map<String, GraphObject> map = payload.getMap();
		
		//filter out the tokens and hasToken Edges
		for(Iterator<Edge_hasToken> iter = payload.iterator(Edge_hasToken.class, true); iter.hasNext(); ) {
			
			Edge_hasToken hasTokenEdge = iter.next();
			
			map.remove(hasTokenEdge.getDestinationURI());
			
			map.remove(hasTokenEdge.getURI());
			
			
		}
		
		//remove all tag elements
		
		if(haltProcessEx != null) {
			log.warn("Halting processflow now...");
			throw haltProcessEx;
		}
		
	}

}
