package edu.cmu.minorthird.text;

import java.util.*;

import org.apache.log4j.*;

/** A TextLabels which is defined by two TextLabels's.
 *
 * <p> Operationally, new assertions are passed to the 'outer' TextLabels.
 * Assertions about property definitions from the outer TextLabels shadow
 * assertions made in the inner TextLabels, and other assertions are added
 * to assertions in the inner TextLabels.
 *
 * <p> Pragmatically, this means that if you create a NestedTextLabels
 * from outerLabels and innerLabels, where outerLabels is empty, the
 * NestedTextLabels will initially look like innerLabels.  But if you modify
 * it, innerLabels will not be changed, so you can at any point easily
 * revert to the old innerLabels TextLabels.
 *
 *
 * @author William Cohen
 */

public class NestedTextLabels implements MonotonicTextLabels{

	private static final Logger log=Logger.getLogger(NestedTextLabels.class);

	private MonotonicTextLabels outer;

	private TextLabels inner;

	private Set<String> shadowedProperties=new HashSet<String>();

	/** Create a NestedTextLabels. */
	public NestedTextLabels(MonotonicTextLabels outer,TextLabels inner){
		if(outer.getTextBase()!=inner.getTextBase())
			throw new IllegalArgumentException("mismatched text bases?");
		this.outer=outer;
		this.inner=inner;
	}

	/** Create a NestedTextLabels with an empty outer labeling. */
	public NestedTextLabels(TextLabels inner){
		this.outer=new BasicTextLabels(inner.getTextBase());
		this.inner=inner;
	}

	public TextBase getTextBase(){
		return inner.getTextBase();
	}

	public boolean hasDictionary(String dictionary){
		return inner.hasDictionary(dictionary)||outer.hasDictionary(dictionary);
	}

	public boolean isAnnotatedBy(String s){
		return outer.isAnnotatedBy(s)||inner.isAnnotatedBy(s);
	}

	public void setAnnotatedBy(String s){
		outer.setAnnotatedBy(s);
	}

	public void setAnnotatorLoader(AnnotatorLoader newLoader){
		outer.setAnnotatorLoader(newLoader);
	}

	public AnnotatorLoader getAnnotatorLoader(){
		return outer.getAnnotatorLoader();
	}

	public void defineDictionary(String dictName,Set<String> dict){
		outer.defineDictionary(dictName,dict);
	}

	/** Associate a dictionary from this file */
	public void defineDictionary(String dictName,List<String> fileNames,
			boolean ignoreCase){
		outer.defineDictionary(dictName,fileNames,ignoreCase);
	}

	/** Return a trie if defined */
	public Trie getTrie(){
		return outer.getTrie();
	}


	@Override
	public void defineTrie(List<String> phraseList, boolean caseSensitive) {
		outer.defineTrie(phraseList, caseSensitive);
	}

	/** Define a trie */
	public void defineTrie(List<String> phraseList){
		this.defineTrie(phraseList, true);
	}

	public boolean inDict(Token token,String dictionary){
		boolean outDict=outer.hasDictionary(dictionary);
		boolean innerDict=inner.hasDictionary(dictionary);
		if(outDict)
			return outer.inDict(token,dictionary);
		else if(innerDict)
			return inner.inDict(token,dictionary);
		else
			throw new IllegalArgumentException("undefined dictionary "+dictionary);
	}

	/** Effectively, remove the property from this TextLabels. 
	 * Specifically ensure that for this property (a) calls to setProperty 
	 * do nothing but cause a warning (b) calls to getProperty return null.
	 */
	public void shadowProperty(String prop){
		shadowedProperties.add(prop);
	}

	public void setProperty(Token token,String prop,String value){
		if(shadowedProperties.contains(prop))
			log.warn("Property "+prop+" has been shadowed");
		else
			outer.setProperty(token,prop,value);
	}

	public void setProperty(Token token,String prop,String value,Details details){
		if(shadowedProperties.contains(prop))
			log.warn("Property "+prop+" has been shadowed");
		else
			outer.setProperty(token,prop,value);
	}

	public String getProperty(Token token,String prop){
		if(shadowedProperties.contains(prop))
			return null;
		else{
			String r=outer.getProperty(token,prop);
			return r!=null?r:inner.getProperty(token,prop);
		}
	}

	public Iterator<Span> getSpansWithProperty(String prop){
		if(shadowedProperties.contains(prop))
			return Collections.EMPTY_SET.iterator();
		else if(!outer.getSpanProperties().contains(prop))
			return inner.getSpansWithProperty(prop);
		else if(!inner.getSpanProperties().contains(prop))
			return outer.getSpansWithProperty(prop);
		else
			return new MyUnionIterator(outer.getSpansWithProperty(prop),inner
					.getSpansWithProperty(prop));
	}

	public Iterator<Span> getSpansWithProperty(String prop,String id){
		if(shadowedProperties.contains(prop))
			return Collections.EMPTY_SET.iterator();
		else if(!outer.getSpanProperties().contains(prop))
			return inner.getSpansWithProperty(prop,id);
		else if(!inner.getSpanProperties().contains(prop))
			return outer.getSpansWithProperty(prop,id);
		else
			return new MyUnionIterator(outer.getSpansWithProperty(prop,id),inner
					.getSpansWithProperty(prop,id));
	}

	public Set<String> getTokenProperties(){
		Set<String> set=setUnion(outer.getTokenProperties(),inner.getTokenProperties());
		set.removeAll(shadowedProperties);
		return set;
	}

	public void setProperty(Span span,String prop,String value){
		outer.setProperty(span,prop,value);
	}

	public void setProperty(Span span,String prop,String value,Details details){
		outer.setProperty(span,prop,value,details);
	}

	public String getProperty(Span span,String prop){
		String r=outer.getProperty(span,prop);
		return r!=null?r:inner.getProperty(span,prop);
	}

	public Set<String> getSpanProperties(){
		return setUnion(outer.getSpanProperties(),inner.getSpanProperties());
	}

	public void addToType(Span span,String type){
		if(!inner.hasType(span,type))
			outer.addToType(span,type);
	}

	public void addToType(Span span,String type,Details details){
		if(!inner.hasType(span,type))
			outer.addToType(span,type,details);
	}

	public boolean hasType(Span span,String type){
		return outer.hasType(span,type)||inner.hasType(span,type);
	}

	public Iterator<Span> instanceIterator(String type){
		if(!outer.isType(type))
			return inner.instanceIterator(type);
		else if(!inner.isType(type))
			return outer.instanceIterator(type);
		else
			return new MyUnionIterator(outer.instanceIterator(type),inner
					.instanceIterator(type));
	}

	public Iterator<Span> instanceIterator(String type,String documentId){
		if(!outer.isType(type))
			return inner.instanceIterator(type,documentId);
		else if(!inner.isType(type))
			return outer.instanceIterator(type,documentId);
		else
			return new MyUnionIterator(outer.instanceIterator(type,documentId),inner
					.instanceIterator(type,documentId));
	}

	public Iterator<Span> closureIterator(String type){
		if(!outer.isType(type))
			return inner.closureIterator(type);
		else if(!inner.isType(type))
			return outer.closureIterator(type);
		else
			return new MyUnionIterator(outer.closureIterator(type),inner
					.closureIterator(type));
	}

	public Iterator<Span> closureIterator(String type,String documentId){
		if(!outer.isType(type))
			return inner.closureIterator(type,documentId);
		else if(!inner.isType(type))
			return outer.closureIterator(type,documentId);
		else
			return new MyUnionIterator(outer.closureIterator(type,documentId),inner
					.closureIterator(type,documentId));
	}

	public Set<String> getTypes(){
		return setUnion(outer.getTypes(),inner.getTypes());
	}

	public Set<Span> getTypeSet(String type,String documentId){
		return setUnion(outer.getTypeSet(type,documentId),inner.getTypeSet(type,
				documentId));
	}

	public boolean isType(String type){
		return outer.isType(type)||inner.isType(type);
	}

	public void declareType(String type){
		//System.out.println("NestedTextLabels: declareType: "+type);
		if(!isType(type))
			outer.declareType(type);
	}

	public Details getDetails(Span span,String type){
		Details result=outer.getDetails(span,type);
		if(result!=null)
			return result;
		return inner.getDetails(span,type);
	}

	public void require(String annotationType,String fileToLoad){
		BasicTextLabels.doRequire(this,annotationType,fileToLoad,outer
				.getAnnotatorLoader());
	}

	public void require(String annotationType,String fileToLoad,
			AnnotatorLoader loader){
		BasicTextLabels.doRequire(this,annotationType,fileToLoad,loader);
	}

	/** Annotate labels with annotator named fileToLoad */
	public void annotateWith(String annotationType,String fileToLoad){
		BasicTextLabels.annotateWith(this,annotationType,fileToLoad);
	}

	public String showTokenProp(TextBase base,String prop){
		return "outer: "+outer.showTokenProp(base,prop)+" inner: "+
				inner.showTokenProp(base,prop);
	}

	//
	// private routines and classes
	//

	private <T> Set<T> setUnion(Set<T> a,Set<T> b){
		if(a.isEmpty())
			return b;
		else{
			Set<T> u=new HashSet<T>();
			u.addAll(a);
			u.addAll(b);
			return u;
		}
	}

	private class MyUnionIterator implements Iterator<Span>{

		Iterator<Span> i,j,currentLooper;

		int estSize=-1;

		public MyUnionIterator(Iterator<Span> i,Iterator<Span> j){
			this.i=i;
			this.j=j;
			currentLooper=i;
		}

		public void remove(){
			currentLooper.remove();
		}

		public boolean hasNext(){
			return currentLooper.hasNext()||(currentLooper==i&&j.hasNext());
		}

		public Span next(){
			if(currentLooper==i&&!currentLooper.hasNext())
				currentLooper=j;
			return currentLooper.next();
		}

		public int estimatedSize(){
			return estSize;
		}
	}


	public String toString(){
		return "[NestedLabels: outer="+outer+"; inner="+inner+"]";
	}
}
