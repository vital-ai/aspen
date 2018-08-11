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
 * A unique identifier sufficient to retrieve the specified
 * exception entry from Wordnet.
 * 
 * @author Mark A. Finlayson
 * @version 2.4.0
 * @since JWI 1.0
 */
public interface IExceptionEntryID extends IHasPOS, IItemID<IExceptionEntry> {

	/**
	 * Returns the surface form (i.e., not the root form) of the word for which
	 * a morphological exception entry is desired. Because all surface forms in
	 * the exception files are lower case, the string returned by this call is
	 * also lower case.
	 * 
	 * @return the lowercase surface form of the exception entry indicated by
	 *         this id object
	 * @since JWI 1.0
	 */
	public String getSurfaceForm();

}