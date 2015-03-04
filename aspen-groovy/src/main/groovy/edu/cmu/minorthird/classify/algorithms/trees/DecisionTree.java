package edu.cmu.minorthird.classify.algorithms.trees;

import java.io.Serializable;

import javax.swing.JComponent;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;

import edu.cmu.minorthird.classify.BinaryClassifier;
import edu.cmu.minorthird.classify.Explanation;
import edu.cmu.minorthird.classify.Feature;
import edu.cmu.minorthird.classify.Instance;

/**
 * A decision tree.
 * 
 * @author William Cohen
 */

/* package */abstract class DecisionTree extends BinaryClassifier implements
		Serializable{

	static final long serialVersionUID=20080609L;

	/** Print routine */
	public String toString(){
		StringBuffer buf=new StringBuffer("");
		toString(buf,0);
		return buf.toString();
	}

	public void toString(StringBuffer buf,int tab){
		for(int i=0;i<tab;i++)
			buf.append("|  ");
		if(this instanceof InternalNode){
			InternalNode in=(InternalNode)this;
			buf.append(in.test+">="+in.threshold+":\n");
			// in.getTrueBranch().toString(buf,tab+1);
			// in.getFalseBranch().toString(buf,tab+1);
		}else{
			Leaf leaf=(Leaf)this;
			buf.append(leaf.getScore()+"\n");
		}
	}

	/**
	 * An internal node of a decision tree.
	 */
	public static class InternalNode extends DecisionTree {

		static final long serialVersionUID=20080609L;

		private Feature test;

		private double threshold;

		private DecisionTree ifTrue,ifFalse;

		public InternalNode(Feature test,DecisionTree ifTrue,DecisionTree ifFalse){
			this(test,0.5,ifTrue,ifFalse);
		}

		public InternalNode(Feature test,double threshold,DecisionTree ifTrue,
				DecisionTree ifFalse){
			this.test=test;
			this.threshold=threshold;
			this.ifTrue=ifTrue;
			this.ifFalse=ifFalse;
		}

		public String explain(Instance instance){
			if(instance.getWeight(test)>=threshold){
				return test+"="+instance.getWeight(test)+">="+threshold+"\n"+
						ifTrue.explain(instance);
			}else{
				return test+"="+instance.getWeight(test)+"<"+threshold+"\n"+
						ifFalse.explain(instance);
			}
		}

		public Explanation getExplanation(Instance instance){
			Explanation.Node top=new Explanation.Node("DecisionTree Explanation");
			if(instance.getWeight(test)>=threshold){
				Explanation.Node node=
						new Explanation.Node(test+"="+instance.getWeight(test)+">="+
								threshold);
				Explanation.Node childEx=ifTrue.getExplanation(instance).getTopNode();
				node.add(childEx);
				top.add(node);
			}else{
				Explanation.Node node=
						new Explanation.Node(test+"="+instance.getWeight(test)+"<"+
								threshold);
				Explanation.Node childEx=ifFalse.getExplanation(instance).getTopNode();
				node.add(childEx);
				top.add(node);
			}
			Explanation ex=new Explanation(top);
			return ex;
		}

		public double score(Instance instance){
			if(instance.getWeight(test)>=threshold)
				return ifTrue.score(instance);
			else
				return ifFalse.score(instance);
		}

		public DecisionTree getTrueBranch(){
			return ifTrue;
		}

		public DecisionTree getFalseBranch(){
			return ifFalse;
		}

	}

	/**
	 * A decision tree leaf.
	 */
	public static class Leaf extends DecisionTree{

		static final long serialVersionUID=20080609L;

		private double myScore;

		public Leaf(double myScore){
			this.myScore=myScore;
		}

		public String explain(Instance instance){
			return "leaf: "+myScore;
		}

		public Explanation getExplanation(Instance instance){
			Explanation.Node top=new Explanation.Node("leaf: "+myScore);
			Explanation ex=new Explanation(top);
			return ex;
		}

		public double score(Instance instance){
			return myScore;
		}

		public double getScore(){
			return myScore;
		}

	}

}
