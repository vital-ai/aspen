/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.main;


public class GenerateDefaultServerConfiguration extends AbstractScript {

	/**
	 * @param args
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		if(args.length < 2) {
			out("Usage: <processorImplClass> <output_xml_file>");
			System.exit(-1);
		}

		String processorImplClass = args[0];
		String outputXMLFile = args[1];
		out("Processor impl. class: " + processorImplClass);
		out("Output XML file: " + outputXMLFile);
		
		
		
		
	}

}
