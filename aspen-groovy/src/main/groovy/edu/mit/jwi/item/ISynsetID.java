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

/**
 * A unique identifier for a synset,
 * sufficient to retrieve it from the Wordnet database. It consists of a
 * part of speech and an offset.
 * 
 * @author Mark A. Finlayson
 * @version 2.4.0
 * @since JWI 1.0
 */
public interface ISynsetID extends IHasPOS, IItemID<ISynset> {

	/**
	 * Returns the offset for the specified synset.
	 * 
	 * @return the byte offset for the specified synset
	 * @since JWI 1.0
	 */
	public int getOffset();

}