/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain.rdf;


import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Arrays
import java.util.Collections
import java.util.Comparator
import java.util.Date
import java.util.GregorianCalendar
import java.util.HashMap
import java.util.HashSet
import java.util.List
import java.util.Locale
import java.util.Map
import java.util.Set
import java.util.TimeZone

import ai.vital.aspen.groovy.nlp.domain.Abbreviation;
import ai.vital.aspen.groovy.nlp.domain.AbbreviationInstance;
import ai.vital.aspen.groovy.nlp.domain.Annotation;
import ai.vital.aspen.groovy.nlp.domain.Category;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Entity;
import ai.vital.aspen.groovy.nlp.domain.EntityInstance;
import ai.vital.aspen.groovy.nlp.domain.NormalizedEntity;
import ai.vital.aspen.groovy.nlp.domain.NormalizedTopic;
import ai.vital.aspen.groovy.nlp.domain.NounPhrase;
import ai.vital.aspen.groovy.nlp.domain.PosTag;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TagElement;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.Topic;
import ai.vital.aspen.groovy.nlp.domain.URIResource;
import ai.vital.aspen.groovy.nlp.domain.VerbPhrase;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.AbbreviationInstanceRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.AbbreviationRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.AnnotationRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.CategoryRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.DocumentRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.EntityInstanceRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.EntityRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.NormalizedEntityRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.NormalizedTopicRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.NounPhraseRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.PosTagRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.SentenceRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.TagElementRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.TextBlockRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.TokenRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.TopicRDFConverter;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.VerbPhraseRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;


import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;

public abstract class AbstractRDFConverter<T extends URIResource> implements RDFConverter<T> {

	public static TimeZone tz = TimeZone.getTimeZone("EST");
	
	public static GregorianCalendar c1 = new GregorianCalendar(tz);
	
	public static DateFormat reviewDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
	
	protected abstract void fill(T object, Model model, Resource resource);
	
	public static Map<Class<? extends URIResource>, RDFConverter<?>> converters = new HashMap<Class<? extends URIResource>, RDFConverter<?>>();

	static {
		
		converters.put(Abbreviation.class, new AbbreviationRDFConverter());
		converters.put(AbbreviationInstance.class, new AbbreviationInstanceRDFConverter());
		converters.put(Annotation.class, new AnnotationRDFConverter());
		converters.put(Category.class, new CategoryRDFConverter());
		converters.put(Document.class, new DocumentRDFConverter());
		converters.put(Entity.class, new EntityRDFConverter());
		converters.put(EntityInstance.class, new EntityInstanceRDFConverter());
		converters.put(NormalizedEntity.class, new NormalizedEntityRDFConverter());
		converters.put(NormalizedTopic.class, new NormalizedTopicRDFConverter());
		converters.put(NounPhrase.class, new NounPhraseRDFConverter());
		converters.put(PosTag.class, new PosTagRDFConverter());
		converters.put(Sentence.class, new SentenceRDFConverter());
		converters.put(TagElement.class, new TagElementRDFConverter());
		converters.put(TextBlock.class, new TextBlockRDFConverter());
		converters.put(Token.class, new TokenRDFConverter());
		converters.put(Topic.class, new TopicRDFConverter());
		converters.put(VerbPhrase.class, new VerbPhraseRDFConverter());
		
		/*
		
		//demo converters
		converters.put(City.class, new CityRDFConverter());
		converters.put(Company.class, new CompanyRDFConverter());
		converters.put(Continent.class, new ContinentRDFConverter());
		converters.put(Country.class, new CountryRDFConverter());
		converters.put(MedicalCondition.class, new MedicalConditionRDFConverter());
		converters.put(MedicalTreatment.class, new MedicalTreatmentRDFConverter());
		converters.put(Organization.class, new OrganizationRDFConverter());
		converters.put(Person.class, new PersonRDFConverter());
		converters.put(Product.class, new ProductRDFConverter());
		converters.put(ProvinceOrState.class, new ProvinceOrStateRDFConverter());
		converters.put(Region.class, new RegionRDFConverter());
		converters.put(Technology.class, new TechnologyRDFConverter());
		
		*/
		
		
	}
	
	@Override
	public void delete(String uri, Model model) {
		
		//by default only direct properties are removed
		
		//delete the all direct 
		Resource subject = ResourceFactory.createResource(uri);
		
		//traverse graph and delete
		
		//all associated properties
		model.removeAll(subject, null, null);
		
//		throw new RuntimeException(this.getClass().getCanonicalName() + ": delete not imeplemnted!");
	}

	@Override
	public final Resource toRDF(T object, Model model) {
	
		Resource resource = model.createResource(object.getUri(), getRDFType());
		
		fill(object, model, resource);
		
		return resource;
	}

	public static void addStringLiteral(Resource r, Property p, String value) {
		if(value != null) {
			r.addLiteral(p, value);
		}
	}
	
	public static void addBooleanLiteral(Resource r, Property p, Boolean v) {
		if(v != null) {
			r.addLiteral(p, v.booleanValue());
		}
	}
	
	
	public static String getStringLiteral(Resource r, Property p) {
		Statement stmt = r.getProperty(p);
		if(stmt == null) return null;
		return stmt.getString();
	}
	
	
	public static boolean getBooleanLiteral(Resource r, Property p, boolean default_) {
		Statement stmt = r.getProperty(p);
		if(stmt == null) return default_;
		return stmt.getBoolean();
	}
	
	public static void addIntegerLiteral(Resource r, Property p, Integer value) {
		if(value != null) {
			r.addProperty(p, value.toString(),  XSDDatatype.XSDint);
		}
	}
	
	public static void addLongLiteral(Resource r, Property p, Long value) {
		if(value != null) {
			r.addProperty(p, value.toString(),  XSDDatatype.XSDlong);
		}
	}
	
	public static Integer getIntegerLiteral(Resource r, Property p, Integer defaultValue) {
		
		Statement stmt = r.getProperty(p);
		if(stmt == null) return defaultValue;
		return stmt.getInt();
		
	}
	
	public static long getLongLiteral(Resource r, Property p, Long defaultValue) {

		//get most recent long value
		
		List<Long> longs = null;
		
		for( StmtIterator iterator = r.listProperties(p); iterator.hasNext(); ) {
			
			if(longs == null) longs = new ArrayList<Long>();
			
			Statement statement = iterator.nextStatement();
			
			longs.add(statement.getLong());
			
		}
		
		if(longs == null) return defaultValue;
		
		Collections.sort(longs);
		
		return longs.get(longs.size() -1);
		
	}
	
	public static Float getFloatLiteral(Resource r, Property p, Float defaultValue) {
		
		Statement stmt = r.getProperty(p);
		if(stmt == null) return defaultValue;
		try {
			return stmt.getFloat();
		} catch(Exception e) {
			e.printStackTrace();
			return defaultValue;
		}
		
	}
	
	public static Double getDoubleLiteral(Resource r, Property p, Double defaultValue) {
		
		Statement stmt = r.getProperty(p);
		if(stmt == null) return defaultValue;
		return stmt.getDouble();
		
	}
	
	public static void addFloatLiteral(Resource r, Property p, Float value) {
		if(value != null) {
			r.addProperty(p, value.toString(),  XSDDatatype.XSDfloat);
		}
	}
	
	public static void addDoubleLiteral(Resource r, Property p, Double value) {
		if(value != null) {
			r.addProperty(p, value.toString(), XSDDatatype.XSDdouble);
		}
		
	}
	
	static void addStringResource(Resource r, Property p, String value) {
		if(value != null) {
			r.addProperty(p, ResourceFactory.createResource(value));
		}
	}
	
	static synchronized void addDateLiteral(Resource r, Property p, String value, DateFormat format) throws ParseException {
		
		if(value != null && !value.isEmpty()) {
			
			format.setTimeZone(tz);
			Date date = format.parse(value);
			c1.setTimeZone(tz);
			c1.setTime(date);
			
			XSDDateTime x1 = new XSDDateTime(c1);
			r.addLiteral(p, x1);

		}
	}
	
	static synchronized void addDateLiteral(Resource r, Property p, Date d) throws ParseException {
		
		if(d != null) {
			
			c1.setTimeZone(tz);
			c1.setTime(d);
			XSDDateTime x1 = new XSDDateTime(c1);
			r.addLiteral(p, x1);
		}
	}
	
	static String getDateLiteral(Resource r, Property p, DateFormat format) {
		
		Statement stmt = r.getProperty(p);
		if(stmt == null) return null;
		
		format.setTimeZone(tz);
		XSDDateTime xsdDateTime = (XSDDateTime) stmt.getLiteral().getValue();
		return format.format(xsdDateTime.asCalendar().getTime());
	}
	
	static Date getDateLiteral(Resource r, Property p) {
		
		Statement stmt = r.getProperty(p);
		if(stmt == null) return null;
		
		XSDDateTime xsdDateTime = (XSDDateTime) stmt.getLiteral().getValue();
		return xsdDateTime.asCalendar().getTime();
	}
	

	
	synchronized static XSDDateTime toXSDate(Date datein) throws ParseException {
		c1.setTimeZone(tz);
		c1.setTime(datein);
		XSDDateTime x1 = new XSDDateTime(c1);
		return x1;
	}
	
	
	static void re(String message) {
		throw new RuntimeException(message);
	}
	
	@SuppressWarnings([ "rawtypes", "unchecked" ])
	static void addSingleResourceProperty(Model m, Resource r, Property p, URIResource u) {
		if(u == null)return;
		
		RDFConverter converter = converters.get(u.getClass());
		
		if(converter == null) re("No converter for class: " + u.getClass() + " registered!");
		
		Resource o = converter.toRDF(u, m);
		
		r.addProperty(p, o);
		
	}
	
	@SuppressWarnings([ "rawtypes"])
	public static void deleteSingleResourceProperty(Model m, Resource r, Property p, RDFConverter c) {
		
		for( StmtIterator listStatements = m.listStatements(r, p, (RDFNode) null); listStatements.hasNext(); ) {
			
			Statement nextStatement = listStatements.nextStatement();
			
			c.delete(nextStatement.getResource().getURI(), m);
			
//			nextStatement.remove();
			
		}

		m.removeAll(r, p, null);
		
	}
	
	public static Resource getResourceProperty(Resource r, Property p) {
		Statement stmt = r.getProperty(p);
		if(stmt == null) return null;
		Resource resource = stmt.getResource();
		if(resource != null) return resource;
		return null;
	}
	
	public static String getStringResourceProperty(Resource r, Property p) {
		Statement stmt = r.getProperty(p);
		if(stmt == null) return null;
		Resource resource = stmt.getResource();
		if(resource != null) return resource.getURI();
		return null;
		
	}
	
	@SuppressWarnings([ "rawtypes", "unchecked" ])
	public static <T extends URIResource> T getSingleResourceProperty(Class<T> clazz, Model m, Resource r, Property p) {
		
		Statement stmt = r.getProperty(p);
		
		if(stmt == null) return null;
		
		RDFConverter converter = converters.get(clazz);
		
		if(converter == null) re("No converter for class: " + clazz + " registered!");
		
		return (T) converter.fromRDF(m, stmt.getResource());
		
	}
	
	@SuppressWarnings([ "rawtypes", "unchecked" ])
	static void addListProperty(Model m, Resource r, Property p, URIResource[] value) {
		
		if(value == null) return;

//		Seq seq = m.createSeq(sequenceURI);
//		r.addProperty(p, seq);
		
		
		for(int i = 0; i < value.length; i++) {
			
			URIResource u = value[i];
			
			RDFConverter converter = converters.get(u.getClass());
			
			if(converter == null) re("No converter for class: " + u.getClass() + " registered!");
			
			Resource ur = converter.toRDF(u, m);
			
			r.addProperty(p, ur);
			
//			seq.add(ur);
			
			addIntegerLiteral(ur, VitalOntology.hasListIndex, i);
			
			
		}
		
	}
	
	@SuppressWarnings("rawtypes")
	public static void deleteListProperty(Model m, Resource r, Property p, RDFConverter converter) {
		
		for( StmtIterator stmts = m.listStatements(r, p, (RDFNode)null); stmts.hasNext(); ) {
			
			Statement nextStatement = stmts.nextStatement();
			
			Resource object = nextStatement.getResource();
			
			if(object == null) continue;
			
//			Resource seqResource = nextStatement.getResource();
//			Seq seq = m.getSeq(seqResource);
//			
//			int size = seq.size();
//			
//			for(int i = size; i > 0; i--) {
//				
//				Resource resource = seq.getResource(i);
//				
//				converter.delete(resource.getURI(), m);
//				
//				seq.remove(i);
//				
//			}
			
			converter.delete(object.getURI(), m);
			
			//clear all statements that 
			m.removeAll(object, null, null);
			
		}
		
		//delete all statements
		m.removeAll(r, p, null);
		
	}
	
	public static String localURIPart(String uri) {
		return localURIPart(uri, false);
	}
	
	public static String localURIPart(String uri, boolean skipURLParams) {
	
		int lastHash = uri.lastIndexOf('#');
		
		int lastSlash = uri.lastIndexOf('/');
		
		int max = Math.max(lastHash, lastSlash);
		
		if(max < 0 || max >= uri.length()-1) {
			throw new RuntimeException("Couldn't extract local part from URI: " + uri);
		}
		
		uri = uri.substring(max + 1);
		
		if(skipURLParams) {
			
			int indexOf = uri.indexOf('?');
			
			if(indexOf > 0) {
				
				return uri.substring(0, indexOf);
				
			}
			
		}
		
		return uri;
		
	}
	
	public static List<Resource> addListPropertyAsEdges(Model m, Resource r, Resource edgeType, String edgeURIBase, List<? extends URIResource> value) {
		if(value == null) {
			return Collections.emptyList();
		}
		return addListPropertyAsEdges(m, r, edgeType, edgeURIBase, value.toArray(new URIResource[value.size()]));
	}
	
	@SuppressWarnings([ "rawtypes", "unchecked" ])
	public static List<Resource> addListPropertyAsEdges(Model m, Resource r, Resource edgeType, String edgeURIBase, URIResource... value) {
		
		if(value == null) return Collections.emptyList();
		
		List<Resource> edges = new ArrayList<Resource>();
		int index = 0;
		for(URIResource u : value) {

			RDFConverter converter = converters.get(u.getClass());
			
			//check if the resource hasn't been already converter - has type property
			
			String uri = u.getUri();
			
			Resource ur = m.getResource(uri);
					
			Resource type = getResourceProperty(ur, RDF.type);
			
			
			if(type != null) {
				
				//already conver
//				System.out.print("");
				
			} else {
				
				if(converter == null) re("No converter for class: " + u.getClass() + " registered!");
				
				ur = converter.toRDF(u, m);
				
			}
			
			String edgeURI = edgeURIBase + localURIPart(r.getURI()) + "-to-" + localURIPart(ur.getURI()); 
			
			Resource edge = m.createResource(edgeURI, edgeType);
			
			edge.addProperty(VitalOntology.hasEdgeSource, r);
			edge.addProperty(VitalOntology.hasEdgeDestination, ur);
			edge.addLiteral(VitalOntology.hasListIndex, index);
			edges.add(edge);
			
			index++;
			
		}
		
		return edges; 
		
	}
	
	public static List<Resource> addListPropertyURIsAsEdges(Model m, Resource r, Resource edgeType, String edgeURIBase, String... uris) {
		return addListPropertyURIsAsEdges(m, r, edgeType, edgeURIBase, Arrays.asList(uris));
		
	}
	
	public static List<Resource> addListPropertyURIsAsEdges(Model m, Resource r, Resource edgeType, String edgeURIBase, List<String> uris) {

		if(uris == null) return Collections.emptyList();
		
		List<Resource> edges = new ArrayList<Resource>();
		
		for(String uri : uris) {
			
			String edgeURI = edgeURIBase + localURIPart(r.getURI()) + "-to-" + localURIPart(uri); 
			
			Resource edge = m.createResource(edgeURI, edgeType);
			
			edge.addProperty(VitalOntology.hasEdgeSource, r);
			edge.addProperty(VitalOntology.hasEdgeDestination, ResourceFactory.createResource(uri));
			
			edges.add(edge);
			
		}
		
		return edges;
		
	}
	
	@SuppressWarnings("rawtypes")
	//if no converter, the target will remain
	public static void deleteListPropertyAsEdges(Model m, Resource r, Resource edgeType, RDFConverter converter) {
		
//		Selector selector;
		
		//first all edges with srcURI sele

		Set<Resource> edges = new HashSet<Resource>();
		
		Set<Resource> destinations = new HashSet<Resource>();
		
		for( StmtIterator edgeSrcStatements = m.listStatements(null, VitalOntology.hasEdgeSource, r); edgeSrcStatements.hasNext(); ) {
			
			Statement next = edgeSrcStatements.nextStatement();
			
			Resource edge = next.getSubject();
			
			if(edgeType.equals(edge.getPropertyResourceValue(RDF.type))) {
				
				for( StmtIterator stmts = m.listStatements(edge, VitalOntology.hasEdgeDestination, (RDFNode) null); stmts.hasNext(); ) {

					Statement next2 = stmts.next();
					
					Resource destResource = next2.getResource();
					
					destinations.add(destResource);
					
//					converter.delete(destResource.getURI(), m);
					
					
				}
				
				edges.add(edge);
				
//				m.removeAll(edge, null, null);
				
			}
			
		}

		if(converter != null) {
			for( Resource dest : destinations) {
				
				converter.delete(dest.getURI(), m);
				
			}
		}
		
		for(Resource edge : edges) {
			
			m.removeAll(edge, null, null);
			
		}
		
		
		
	}
	
	
	static List<String> getListPropertyUris(Model m, Resource r, Property p) {
		
		List<String> uris = new ArrayList<String>();
		
		for( NodeIterator nodeIterator =  m.listObjectsOfProperty(r, p); nodeIterator.hasNext(); ) {
			
			Resource asResource = nodeIterator.nextNode().asResource();
			
			if(asResource != null && asResource.getURI() != null) {
				uris.add(asResource.getURI());
			}
			
		}
		
		return uris;
		
	}
	@SuppressWarnings([ "rawtypes", "unchecked" ])
	static <T extends URIResource> List<T> getListProperty(Class<T> clazz, Model m, Resource r, Property p) {

		RDFConverter converter = converters.get(clazz);
		
		if(converter == null) re("No converter for class: " + clazz.getCanonicalName());
		
		
		Map<Integer, T> deserialized = new HashMap<Integer, T>();
		
		int unordered = 1000001;
		
		for( StmtIterator listStatements = m.listStatements(r, p, (RDFNode)null); listStatements.hasNext(); ) {
			
			Statement stmt = listStatements.next();
			
			Resource object = stmt.getResource();
			
			if(object == null) continue;
			
			T fromRDF = (T) converter.fromRDF(m, object);

			int index = getIntegerLiteral(object, VitalOntology.hasListIndex, -1);
			
			if(index < 0) {
			
				index = unordered++;
				
			}
			
			deserialized.put(index, fromRDF);
			
		}
		
		List<T> res = new ArrayList<T>();

		List<Integer> keys = new ArrayList<Integer>(deserialized.keySet());
		
		Collections.sort(keys);
		
		for(Integer k : keys) {
			
			T t = deserialized.get(k);
			
			if(t != null) {
				res.add(t);
			}
			
		}
		
		/*
		Statement stmt = r.getProperty(p);
		
		if(stmt == null) return new ArrayList<T>();
		
		RDFNode object = stmt.getObject();
		Seq seq = m.getSeq((Resource)object);
		
		if(seq == null) re("Expected sequence collection, got null!");
		
		
//		r.addProperty(p, seq);
		
		List<T> res = new ArrayList<T>();
		
		for(int i = 1; i <= seq.size(); i++ ) {
			
			Resource r_ = seq.getResource(i);
			
			T fromRDF = (T) converter.fromRDF(m, r_);
			
			res.add(fromRDF);
			
		}
		
		*/
		
		return res;
		
	}
	

	@SuppressWarnings([ "rawtypes", "unchecked" ])
	public static <T extends URIResource> List<T> getListPropertyFromEdges(Class<T> clazz, Model m, Resource r, Resource edgeType) {
		return getListPropertyFromEdges(clazz, m, r, edgeType, 1);
	}	
	
	@SuppressWarnings([ "rawtypes", "unchecked" ])
	static <T extends URIResource> List<T> getListPropertyFromEdges(Class<T> clazz, Model m, Resource r, Resource edgeType, int maxDepth) {
		
		RDFConverter converter = converters.get(clazz);
		
		if(converter == null) re("No converter for class: " + clazz.getCanonicalName());
		
		final Map<String, Integer> destToIndex = new HashMap<String, Integer>();
		
		List<T> res = new ArrayList<T>();
		
		for( StmtIterator edges = m.listStatements(null, RDF.type, edgeType); edges.hasNext(); ) {
			
			Statement stmt = edges.nextStatement();
			
			Resource edge = stmt.getSubject();
			
			Resource src = getResourceProperty(edge, VitalOntology.hasEdgeSource);
			
			boolean passed = false; 
			
			if(src.equals(r))  {
				passed = true;
			}
			
			if(!passed && maxDepth > 1) {
			
				List<Resource> inputList = new ArrayList<Resource>();
				
				inputList.add(src);
				
				boolean doDepthCheck = true;
				
				int depth = 1;
				
				while(doDepthCheck) {

					List<Resource> newList = new ArrayList<Resource>();
					
					for(Resource parent : inputList) {
						
						for( ResIterator i = m.listSubjectsWithProperty(VitalOntology.hasEdgeDestination, parent); i.hasNext(); ) {
							
							Resource e = i.nextResource();
							
							Resource newSrc = e.getPropertyResourceValue(VitalOntology.hasEdgeSource);
							
							if(newSrc.equals(r)) {
								
								passed = true;
								doDepthCheck = false;
								break;
								
							} else {
								
								newList.add(newSrc);
								
							}
							
							
						}
						
					}
					
					depth++;
					
					if(depth >= maxDepth) {
						
						doDepthCheck = false;
						
					}
					
					inputList = newList;
					
				}
				
			}
			
//			if(maxDepth == 1 && src.equals(r)) {
//				passed = true;
//			} else {
//				
//				//check distance
//				if(src.equals(r)) {
//					
//				}
//				
//			}
			
			if(passed) {
				
				Resource dest = getResourceProperty(edge, VitalOntology.hasEdgeDestination);
				
				T fromRDF = (T) converter.fromRDF(m, dest);
				
				res.add(fromRDF);
				
				int index = getIntegerLiteral(edge, VitalOntology.hasListIndex, Integer.MAX_VALUE);
				
				destToIndex.put(fromRDF.getUri(), index);
				
			}
			
		}
		
		Collections.sort(res, new Comparator<T>() {

			//@Override
			public int compare(T o1, T o2) {
				
				Integer index1 = destToIndex.get(o1.getUri());
				Integer index2 = destToIndex.get(o2.getUri());
				return Integer.compare(index1, index2);
				
			}
		});
		
		return res;
		
	}
	
	@SuppressWarnings([ "rawtypes", "unchecked" ])
	static List<String> getListPropertyURIsFromEdges(Model m, Resource r, Resource edgeType) {
		
		List<String> res = new ArrayList<String>();
		
		for( StmtIterator edges = m.listStatements(null, RDF.type, edgeType); edges.hasNext(); ) {
			
			Statement stmt = edges.nextStatement();
			
			Resource edge = stmt.getSubject();
			
			Resource src = getResourceProperty(edge, VitalOntology.hasEdgeSource);
			
			if(r.equals(src)) {
				
				Resource dest = getResourceProperty(edge, VitalOntology.hasEdgeDestination);
				
				res.add(dest.getURI());
				
			}
			
		}
		
		Collections.sort(res);
		
		return res;
		
	}
	
//	static void addListLiterals(Model m, Resource r, Property p, String sequenceURI, int[] literals) {
//		
//		if(literals == null) return;
//		
//		Seq seq = m.createSeq(sequenceURI);
//		
//		r.addProperty(p, seq);
//		
//		for(int u : literals) {
//			
//			seq.add(u);
//			
//		}
//	}
	
//	static void addListLiterals(Model m, Resource r, Property p,  String sequenceURI, Object[] literals) {
//		
//		if(literals == null) return;
//		
//		Seq seq = m.createSeq(sequenceURI);
//		
//		r.addProperty(p, seq);
//		
//		for(Object u : literals) {
//			
//			seq.add(u);
//			
//		}
//		
//	}
	
	
//	static void deleteListLiterals(Model m, Resource r,
//			Property p) {
//
//		for( StmtIterator listStatements = m.listStatements(r, p, (RDFNode) null); listStatements.hasNext(); ) {
//			
//			Statement nextStatement = listStatements.nextStatement();
//			
//			Resource resource = nextStatement.getResource();
//			
//			resource.removeProperties();
//			
//			
//		}
//		
//		
//	}
	
//	@SuppressWarnings("unchecked")
//	static <T> List<T> getListLiterals(Class<T> clazz, Model m, Resource r, Property p) {
//		
//		Statement stmt = r.getProperty(p);
//		
//		if(stmt == null) return new ArrayList<T>();
//		
//		List<T> l = new ArrayList<T>();
//		
//		Seq seq = m.getSeq(stmt.getResource());
//		
//		for(int i = 1 ; i <= seq.size(); i++) {
//
//			Literal literal = seq.getLiteral(i);
//			
//			T v = (T) literal.getValue();
//			if(clazz == Integer.class) {
//				v = (T) new Integer(literal.getInt());
//			} else {
//				v = (T) literal.getValue();
//			}
//			
//			
//			l.add(v);
//
//		}
//		
//		return l;
//		
//	}


	public static String[] getStringArrayFromCSV(Resource r,
			Property p) {

		String s = getStringLiteral(r, p);
		if(s == null || s.isEmpty()) return null;
		
		List<String> list = new ArrayList<String>();
		
		String[] split = s.split(",");
		
		for(String sp : split) {
			if(!sp.isEmpty()) {
				list.add(sp);
			}
		}
		
		if(list.size() > 0) {
			return list.toArray(new String[list.size()]);
		}
		
		return null;
	}
	
	
	public static void addStringCSVFromArray(Resource r,
			Property p, String[] value) {
		
		if(value == null || value.length < 1) return;
		
		StringBuilder sb = new StringBuilder();
		
		for(String s : value) {
			
			if(sb.length() > 0) sb.append(',');
			
			sb.append(s);
			
		}
		
		addStringLiteral(r, p, sb.toString());
		
	}

	@Override
	public boolean hasRDFType(Model model, String resource) {
		for( StmtIterator listStatements = model.listStatements(ResourceFactory.createResource(resource), RDF.type, getRDFType()); listStatements.hasNext(); ) {
			return true;
		}
		return false;
	}

	
	static void addListPropertyUris(Model model, Resource pr,
			Property p, String[] uris) {

		if(uris == null) return;
		
		for(String uri : uris) {
			
			pr.addProperty(p, ResourceFactory.createResource(uri));
			
		}
		
	}

	public static Set<Resource> listResourceProperties(Model model, Resource r, Property type) {

		Set<Resource> set = new HashSet<Resource>();
		
		for( NodeIterator listObjectsOfProperty = model.listObjectsOfProperty(r, type); listObjectsOfProperty.hasNext(); ) {
			Resource asResource = listObjectsOfProperty.nextNode().asResource();
			if(asResource != null) {
				set.add(asResource);
			}
		}
		
		return set;
		
	}
	
}
