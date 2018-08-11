/********************************************************************************
 * Java Wordnet Interface Library (JWI) v2.4.0
 * Copyright (c) 2007-2015 Mark A. Finlayson
 *
 * JWI is distributed under the terms of the Creative Commons Attribution 4.0 
 * International Public License, which means it may be freely used for all 
 * purposes, as long as proper acknowledgment is made.  See the license file 
 * included with this distribution for more details.
 *******************************************************************************/

package edu.mit.jwi.data;

import java.io.IOException;

/**
 * Object that has a lifecycle. Objects implementing this interface can be
 * opened, closed, and have their open state queried. In general, the open state
 * of the object should be reversible, that is, an object may be closed and
 * re-opened. What happens when the object is used when closed is implementation
 * dependent.
 * 
 * @author Mark A. Finlayson
 * @version 2.4.0
 * @since JWI 2.2.0
 */
public interface IHasLifecycle extends IClosable {
	
	/**
	 * This opens the object by performing any required initialization steps. If
	 * this method returns <code>false</code>, then subsequent calls to
	 * {@link #isOpen()} will return <code>false</code>.
	 * 
	 * @return <code>true</code> if there were no errors in initialization;
	 *         <code>false</code> otherwise.
	 * @throws IOException
	 *             if there was IO error while performing initializataion
	 * @since JWI 2.2.0
	 */
	public boolean open() throws IOException;
	
	/**
	 * Returns <code>true</code> if the dictionary is open, that is, ready to
	 * accept queries; returns <code>false</code> otherwise
	 * 
	 * @return <code>true</code> if the object is open; <code>false</code>
	 *         otherwise
	 * @since JWI 2.2.0
	 */
	public boolean isOpen();
	
	/**
	 * An enum that represents the four different lifecycle states an object may
	 * be in. It may be closed, open, in the processing of opening, or in the
	 * process of closing.
	 *
	 * @author Mark A. Finlayson
	 * @version 2.4.0
	 * @since JWI 2.4.0
	 */
	public enum LifecycleState {
		CLOSED,
		OPENING,
		OPEN,
		CLOSING;
	}

	/**
	 * Indicates that the object was closed when some method was called
	 * requiring it to be open.
	 * 
	 * @author Mark A. Finlayson
	 * @version 2.4.0
	 * @since JWI 2.2.0
	 */
	public class ObjectClosedException extends RuntimeException {

		// serial version id
		private static final long serialVersionUID = -4703264035869277920L;

		/**
		 * Constructs a new exception with <code>null</code> as its detail
		 * message. The cause is not initialized, and may subsequently be
		 * initialized by a call to {@link #initCause}.
		 * 
		 * @since JWI 2.2.0
		 */
		public ObjectClosedException() {
			super();
		}

		/**
		 * Constructs a new exception with the specified detail message. The cause
		 * is not initialized, and may subsequently be initialized by a call to
		 * {@link #initCause}.
		 * 
		 * @param message
		 *            the detail message. The detail message is saved for later
		 *            retrieval by the {@link #getMessage()} method.
		 * @since JWI 2.2.0
		 */
		public ObjectClosedException(String message) {
			super(message);
		}

		/**
		 * Constructs a new exception with the specified detail message and cause.
		 * <p>
		 * Note that the detail message associated with <code>cause</code> is
		 * <i>not</i> automatically incorporated in this runtime exception's detail
		 * message.
		 * 
		 * @param message
		 *            the detail message (which is saved for later retrieval by the
		 *            {@link #getMessage()} method).
		 * @param cause
		 *            the cause (which is saved for later retrieval by the
		 *            {@link #getCause()} method). (A {@code null} value is
		 *            permitted, and indicates that the cause is nonexistent or
		 *            unknown.)
		 * @since JWI 2.2.0
		 */
		public ObjectClosedException(String message, Throwable cause) {
			super(message, cause);
		}

		/**
		 * Constructs a new exception with the specified cause and a detail message
		 * of {@code (cause==null ? null : cause.toString())} (which typically
		 * contains the class and detail message of {@code cause}). This
		 * constructor is useful for runtime exceptions that are little more than
		 * wrappers for other throwables.
		 * 
		 * @param cause
		 *            the cause (which is saved for later retrieval by the
		 *            {@link #getCause()} method). (A {@code null} value is
		 *            permitted, and indicates that the cause is nonexistent or
		 *            unknown.)
		 * @since JWI 2.2.0
		 */
		public ObjectClosedException(Throwable cause) {
			super(cause);
		}
	}
	
	/**
	 * Indicates that the object was open when some method was called
	 * requiring it to be closed.
	 * 
	 * @author Mark A. Finlayson
	 * @version 2.4.0
	 * @since JWI 2.4.0
	 */
	public class ObjectOpenException extends RuntimeException {

		// serial version id
		private static final long serialVersionUID = -4703264035869277920L;

		/**
		 * Constructs a new exception with <code>null</code> as its detail
		 * message. The cause is not initialized, and may subsequently be
		 * initialized by a call to {@link #initCause}.
		 * 
		 * @since JWI 2.4.0
		 */
		public ObjectOpenException() {
			super();
		}

		/**
		 * Constructs a new exception with the specified detail message. The cause
		 * is not initialized, and may subsequently be initialized by a call to
		 * {@link #initCause}.
		 * 
		 * @param message
		 *            the detail message. The detail message is saved for later
		 *            retrieval by the {@link #getMessage()} method.
		 * @since JWI 2.4.0
		 */
		public ObjectOpenException(String message) {
			super(message);
		}

		/**
		 * Constructs a new exception with the specified detail message and cause.
		 * <p>
		 * Note that the detail message associated with <code>cause</code> is
		 * <i>not</i> automatically incorporated in this runtime exception's detail
		 * message.
		 * 
		 * @param message
		 *            the detail message (which is saved for later retrieval by the
		 *            {@link #getMessage()} method).
		 * @param cause
		 *            the cause (which is saved for later retrieval by the
		 *            {@link #getCause()} method). (A {@code null} value is
		 *            permitted, and indicates that the cause is nonexistent or
		 *            unknown.)
		 * @since JWI 2.4.0
		 */
		public ObjectOpenException(String message, Throwable cause) {
			super(message, cause);
		}

		/**
		 * Constructs a new exception with the specified cause and a detail message
		 * of {@code (cause==null ? null : cause.toString())} (which typically
		 * contains the class and detail message of {@code cause}). This
		 * constructor is useful for runtime exceptions that are little more than
		 * wrappers for other throwables.
		 * 
		 * @param cause
		 *            the cause (which is saved for later retrieval by the
		 *            {@link #getCause()} method). (A {@code null} value is
		 *            permitted, and indicates that the cause is nonexistent or
		 *            unknown.)
		 * @since JWI 2.4.0
		 */
		public ObjectOpenException(Throwable cause) {
			super(cause);
		}
	}


}
