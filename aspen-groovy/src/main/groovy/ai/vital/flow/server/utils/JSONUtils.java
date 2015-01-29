/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import ai.vital.vitalsigns.model.container.Payload;

public class JSONUtils {

	static ObjectMapper mapper = new ObjectMapper();
	
	public static String newMessagePriority = "newMessagePriority";
	
	public static Map<String, Object> getContextMap(Payload payload) throws IOException {
		
		String ctx = payload.getSerializedContext();
		if(ctx != null && !ctx.isEmpty()) {
			@SuppressWarnings("unchecked")
			LinkedHashMap<String, Object> map = mapper.readValue(ctx, LinkedHashMap.class);
			return map;
		} else {
			return Collections.emptyMap();
		}
		
	}
	
	public static void serializeContext(Payload payload, LinkedHashMap<String, Object> context) throws IOException {
		
		StringWriter w = new StringWriter();
		
		// String jsonString = mapper.writeValue(w, context); .writeValueAsString(context);
		
		
		mapper.writeValue(w, context);
		
		payload.setSerializedContext(w.toString());
		
	}
	
	
	
}
