/********************************************************************************
 * Java Wordnet Interface Library (JWI) v2.4.0
 * Copyright (c) 2007-2015 Mark A. Finlayson
 *
 * JWI is distributed under the terms of the Creative Commons Attribution 4.0 
 * International Public License, which means it may be freely used for all 
 * purposes, as long as proper acknowledgment is made.  See the license file 
 * included with this distribution for more details.
 *******************************************************************************/

package edu.mit.jwi.item;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Default, hard-coded, implementation of {@code IVerbFrame} that does not read
 * from the actual file. This is not implemented as an {@code Enum} so that
 * clients can instantiate their own custom verb frame objects.
 * 
 * @version 2.4.0
 * @since JWI 2.1.0
 */
public class VerbFrame implements IVerbFrame {
	
	/**
	 * This serial version UID identifies the last version of JWI whose
	 * serialized instances of the VerbFrame class are compatible with this
	 * implementation.
	 * 
	 * @since JWI 2.4.0
	 */
	private static final long serialVersionUID = 240;
	
	// standard verb frames
	public static final VerbFrame NUM_01 = new VerbFrame(1, "Something ----s");
	public static final VerbFrame NUM_02 = new VerbFrame(2, "Somebody ----s");
	public static final VerbFrame NUM_03 = new VerbFrame(3, "It is ----ing");
	public static final VerbFrame NUM_04 = new VerbFrame(4, "Something is ----ing PP");
	public static final VerbFrame NUM_05 = new VerbFrame(5, "Something ----s something Adjective/Noun");
	public static final VerbFrame NUM_06 = new VerbFrame(6, "Something ----s Adjective/Noun");
	public static final VerbFrame NUM_07 = new VerbFrame(7, "Somebody ----s Adjective");
	public static final VerbFrame NUM_08 = new VerbFrame(8, "Somebody ----s something");
	public static final VerbFrame NUM_09 = new VerbFrame(9, "Somebody ----s somebody");
	public static final VerbFrame NUM_10 = new VerbFrame(10, "Something ----s somebody");
	public static final VerbFrame NUM_11 = new VerbFrame(11, "Something ----s something");
	public static final VerbFrame NUM_12 = new VerbFrame(12, "Something ----s to somebody");
	public static final VerbFrame NUM_13 = new VerbFrame(13, "Somebody ----s on something");
	public static final VerbFrame NUM_14 = new VerbFrame(14, "Somebody ----s somebody something");
	public static final VerbFrame NUM_15 = new VerbFrame(15, "Somebody ----s something to somebody");
	public static final VerbFrame NUM_16 = new VerbFrame(16, "Somebody ----s something from somebody");
	public static final VerbFrame NUM_17 = new VerbFrame(17, "Somebody ----s somebody with something");
	public static final VerbFrame NUM_18 = new VerbFrame(18, "Somebody ----s somebody of something");
	public static final VerbFrame NUM_19 = new VerbFrame(19, "Somebody ----s something on somebody");
	public static final VerbFrame NUM_20 = new VerbFrame(20, "Somebody ----s somebody PP");
	public static final VerbFrame NUM_21 = new VerbFrame(21, "Somebody ----s something PP");
	public static final VerbFrame NUM_22 = new VerbFrame(22, "Somebody ----s PP");
	public static final VerbFrame NUM_23 = new VerbFrame(23, "Somebody's (body part) ----s");
	public static final VerbFrame NUM_24 = new VerbFrame(24, "Somebody ----s somebody to INFINITIVE");
	public static final VerbFrame NUM_25 = new VerbFrame(25, "Somebody ----s somebody INFINITIVE");
	public static final VerbFrame NUM_26 = new VerbFrame(26, "Somebody ----s that CLAUSE");
	public static final VerbFrame NUM_27 = new VerbFrame(27, "Somebody ----s to somebody");
	public static final VerbFrame NUM_28 = new VerbFrame(28, "Somebody ----s to INFINITIVE");
	public static final VerbFrame NUM_29 = new VerbFrame(29, "Somebody ----s whether INFINITIVE");
	public static final VerbFrame NUM_30 = new VerbFrame(30, "Somebody ----s somebody into V-ing something");
	public static final VerbFrame NUM_31 = new VerbFrame(31, "Somebody ----s something with something");
	public static final VerbFrame NUM_32 = new VerbFrame(32, "Somebody ----s INFINITIVE");
	public static final VerbFrame NUM_33 = new VerbFrame(33, "Somebody ----s VERB-ing");
	public static final VerbFrame NUM_34 = new VerbFrame(34, "It ----s that CLAUSE");
	public static final VerbFrame NUM_35 = new VerbFrame(35, "Something ----s INFINITIVE");

	// final instance fields
	private final int num;
	private final String template;
	
	/** 
	 * Constructs a new verb frame.
	 * 
	 * @param num the verb frame number
	 * @param template the template representing the verb frame
	 * @since JWI 2.1.0
	 */
	public VerbFrame(int num, String template){
		this.num = num;
		this.template = template; 
	}
	
	/* 
	 * (non-Javadoc) 
	 *
	 * @see edu.mit.jwi.item.IVerbFrame#getNumber()
	 */
	public int getNumber(){
		return num;
	}
	
	/* 
	 * (non-Javadoc) 
	 *
	 * @see edu.mit.jwi.item.IVerbFrame#getTemplate()
	 */
	public String getTemplate(){
		return template;
	}
	
	/* 
	 * (non-Javadoc) 
	 *
	 * @see edu.mit.jwi.item.IVerbFrame#instantiateTemplate(java.lang.String)
	 */
	public String instantiateTemplate(String verb){
		if(verb == null)
			throw new NullPointerException();
		int index = template.indexOf("----");
		if (index == -1) return "";
		return template.substring(0, index) + verb + template.substring(index + 5, template.length());
	}
	
	/* 
	 * (non-Javadoc) 
	 *
	 * @see java.lang.Object#toString()
	 */
	public String toString(){
		return "[" + num + " : " + template + " ]";
	}
	
	/**
	 * This utility method implements the appropriate deserialization for this
	 * object.
	 *
	 * @return the appropriate deserialized object.
	 * @since JWI 2.4.0
	 */
	protected Object readResolve(){
		VerbFrame staticFrame = getFrame(num);
		return (staticFrame == null) ?
				this : 
					staticFrame;
	}
	
	// verb frame cache
	private static final Map<Integer, VerbFrame> verbFrameMap;
	
	static {
		
		// get the instance fields
		Field[] fields = VerbFrame.class.getFields();
		List<Field> instanceFields = new ArrayList<Field>();
		for(Field field : fields)
			if(field.getGenericType() == VerbFrame.class)
				instanceFields.add(field);
		
		// this is our backing collection
		Map<Integer, VerbFrame> hidden = new LinkedHashMap<Integer, VerbFrame>(instanceFields.size());

		// get the instances
		VerbFrame frame;
		for(Field field : instanceFields){
			try{
				frame = (VerbFrame)field.get(null);
				if(frame != null)
					hidden.put(frame.getNumber(), frame);
			} catch(IllegalAccessException e){
				// Ignore
			}
		}

		// make the value map unmodifiable
		verbFrameMap = Collections.unmodifiableMap(hidden);
	}

	/**
	 * This emulates the Enum.values() method, in that it returns an
	 * unmodifiable collection of all the static instances declared in this
	 * class, in the order they were declared.
	 * 
	 * @return an unmodifiable collection of verb frames defined in this class
	 * @since JWI 2.1.0
	 */
	public static Collection<VerbFrame> values(){
		return verbFrameMap.values();
	}

	/**
	 * Returns the frame indexed by the specified number defined in this class,
	 * or <code>null</code> if there is
	 * 
	 * @param number
	 *            the verb frame number
	 * @return the verb frame with the specified number, or <code>null</code> if
	 *         nuone
	 * @since JWI 2.1.0
	 */
    public static VerbFrame getFrame(int number) {
    	return verbFrameMap.get(number);
    }
    
    

}
