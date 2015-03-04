package edu.cmu.minorthird.classify;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;

/**
 * Provides a facitlity for constructing and displaying explanations for classification. 
 * An explanation may be constructed using a String or by giving it the top Node of a tree.
 *
 * @author Cameron Williams
 * Date: Aug 18, 2005
 */

public class Explanation {

	private String stringEx = null;
	private JTree treeEx = null;
	private Explanation.Node top = null;

	public Explanation (String explanation) {
		stringEx = explanation;
	}

	public Explanation(Explanation.Node top) {
		this.top = top;
		treeEx = new JTree(top);
	}

	/** Returns the top node of the explanation tree or creates a Node from the string explanation */
	public Node getTopNode() {
		if(top != null)
			return top;
		else {
			Node simple = new Node(stringEx);
			return simple;
		}
	}

	/** A Node in the Explanation Tree */
	static public class Node extends DefaultMutableTreeNode
	{
		
		static final long serialVersionUID=20071015;
		
		public Node(String ex) {
			super(ex);	    	    
		}
	}

}