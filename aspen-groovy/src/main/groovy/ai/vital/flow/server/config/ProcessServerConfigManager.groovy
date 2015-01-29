/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.config;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import ai.vital.workflow.ConfProperty;
import ai.vital.workflow.IEngine;
import ai.vital.workflow.WorkflowConfig;
import ai.vital.workflow.WorkflowEngineConfig;
import ai.vital.workflow.WorkflowConfig.StepName;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.DomDriver;

/**
 * Configuration manager for handling the configuration file.
 * @author Derek
 *
 */
public class ProcessServerConfigManager {

	private static XStream xStream = null;
	
	//use console for the config manager output
	private static void out(String msg) {
		System.out.println(msg);
	}
	
	private static synchronized void initXStream(Class<?> extraConfClass) throws IOException {
		
//		if(xStream == null) {
			
			xStream = new XStream(new DomDriver());
		
			
			xStream.setMode(XStream.NO_REFERENCES);
			
			xStream.alias("config", ProcessServerConfig.class);
			
			xStream.alias("jms", JMSComponentConfig.class);
			
			xStream.alias("logger", LoggerConfig.class);
		
			xStream.alias("workflow", WorkflowConfig.class);
			
			xStream.alias("step", StepName.class);
			
			xStream.alias("property", ConfProperty.class);
			
			xStream.registerConverter(new StepNameConverter());
			
			xStream.registerConverter(new ConfPropertyConverter());
			
			if(extraConfClass != null) {
				//load the 
				try {
					Method declaredMethod = extraConfClass.getDeclaredMethod("initXStream", XStream.class);
					declaredMethod.invoke(null, xStream);
				} catch (Exception e) {
					throw new IOException(e);
				}
				
			}
//		}
		
	}
	
	/**
	 * Loads the configuration from given xml file. Extra xstream configuration may be set up if the extraConfClass 
	 * with <code>public static void initXStream(XStream xs)</code> method is provided.
	 *  
	 * @param xmlFileConfig
	 * @param extraConfClass  may be <code>null</code>
	 * @return
	 * @throws IOException
	 */
	public static ProcessServerConfig loadConfigFromFile(String xmlFileConfig, Class<?> extraConfClass) throws IOException {
		
		out("Loading configuration from file: " + xmlFileConfig);
		
		long start = System.currentTimeMillis();
		
		
		initXStream(extraConfClass);
		
		ProcessServerConfig config = (ProcessServerConfig) xStream.fromXML(new FileInputStream(xmlFileConfig));
		
		if(config == null) throw new NullPointerException("Null object deserialized from xml.");
		
		out("Configuration loaded successfully from file: " + xmlFileConfig + " [" + (System.currentTimeMillis() - start) + "ms]");
		
		return config;
		
	}
	
	public static ProcessServerConfig generateDefaultConfig(Class<? extends IEngine> engineClass, WorkflowEngineConfig engineConfig) throws IOException {
		
//		out("Generating default configuration object...");
//		long start = System.currentTimeMillis();
		
		initXStream(engineClass);
		
		ProcessServerConfig config = new ProcessServerConfig();
		
		config.setEngine(engineConfig);
		
		config.setId("NLPServer#1");

		HttpConnector httpConnector = new HttpConnector();
		httpConnector.setHost("0.0.0.0");
		httpConnector.setPort(9090);
		
		config.setHttpConnector(httpConnector);
		
		LoggerConfig logger = new LoggerConfig();
		logger.setProperties("log4j.properties");
		config.setLogger(logger);
		
		
		JMSComponentConfig jms = new JMSComponentConfig();
		jms.setBrokerURL("tcp://127.0.0.1:61616");
		jms.setQueueName("someQueueName");
		jms.setConnectionRetry(5);
		config.setJms(jms);
		
		
		
		return config;
		
	}
	
	public static void writeToFile(ProcessServerConfig config, String outputFile) throws IOException {
		xStream.toXML(config, new FileOutputStream(outputFile));
	}
	
	
	public static class StepNameConverter implements Converter {

		@Override
		public boolean canConvert(Class arg0) {
			return StepName.class.equals(arg0);
		}

		@Override
		public void marshal(Object o, HierarchicalStreamWriter writer,
				MarshallingContext ctx) {

			StepName sn = (StepName) o;
//			writer.startNode("step");
			writer.setValue(sn.getName());
//			writer.endNode();

		}

		@Override
		public Object unmarshal(HierarchicalStreamReader reader,
				UnmarshallingContext ctx) {

//			reader.moveDown();
			StepName sn = new StepName(reader.getValue());
//			reader.moveUp();
			
			return sn;
			
		}
		
	}
	
	public static class ConfPropertyConverter implements Converter {

		@Override
		public boolean canConvert(Class arg0) {
			return ConfProperty.class.equals(arg0);
		}

		@Override
		public void marshal(Object o, HierarchicalStreamWriter writer,
				MarshallingContext ctx) {

			ConfProperty cp = (ConfProperty) o;
			
			writer.addAttribute("name", cp.getName());
			writer.addAttribute("value", cp.getValue());
			
			
		}

		@Override
		public Object unmarshal(HierarchicalStreamReader reader,
				UnmarshallingContext ctx) {

			ConfProperty cp = new ConfProperty();
			
			cp.setName(reader.getAttribute("name"));
			cp.setValue(reader.getAttribute("value"));
			
			return cp;
		}
		
		
		
	}
}
