package ai.vital.aspen.groovy.taxonomy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.Edge_hasChildCategory;
import ai.vital.vitalsigns.VitalSigns;

public class HierarchicalCategories {

	static void o(String m) { System.out.println(m); }

	private TaxonomyNode rootNode = null;
	
	public Map<String, TaxonomyNode> URI2TaxonomyNode = new HashMap<String, TaxonomyNode>();
	
	private final static Logger log = LoggerFactory.getLogger(HierarchicalCategories.class);
	
	public HierarchicalCategories(String rootURI) throws IOException {
		
		log.info("Loading taxonomy from ontology, rootURI: " + rootURI);
		
		Map<String, VITAL_Category> URI2cat = new HashMap<String, VITAL_Category>();
		
//		String filter = rootURI;
//		int lastHash = rootURI.lastIndexOf('#');
//		if(lastHash > 0) {
//			filter = filter.substring(0, lastHash);
//		}
		
		
		for(VITAL_Category c : VitalSigns.get().listDomainIndividuals(VITAL_Category.class, null)) {
			URI2cat.put(c.getURI(), c);
		}
		
		VITAL_Category rootCategory = URI2cat.get(rootURI);
		if(rootCategory == null) throw new IOException("Root category not found: " + rootURI);

		Map<String, List<Edge_hasChildCategory>> edgesMap = new HashMap<String, List<Edge_hasChildCategory>>();
		
		for(Edge_hasChildCategory e : VitalSigns.get().listDomainIndividuals(Edge_hasChildCategory.class, null)) {
			
			//connect taxonomy nodes
			List<Edge_hasChildCategory> l = edgesMap.get(e.getSourceURI());
			if(l == null) {
				l = new ArrayList<Edge_hasChildCategory>();
				edgesMap.put(e.getSourceURI(), l);
			}
			l.add(e);
			
		}
		
		int depth = 0;
		
		rootNode = new TaxonomyNode(rootURI, (String) rootCategory.getProperty("name"), depth);
		
		URI2TaxonomyNode.put(rootNode.getURI(), rootNode);
		
		List<TaxonomyNode> taxonomyNodesToProcess = new ArrayList<TaxonomyNode>(Arrays.asList(rootNode));
		
		while(taxonomyNodesToProcess.size() > 0) {
			
			depth++;
			
			List<TaxonomyNode> newList = new ArrayList<TaxonomyNode>();
			
			for(TaxonomyNode node : taxonomyNodesToProcess) {
				
				List<Edge_hasChildCategory> list = edgesMap.get(node.getURI());
				
				if(list == null) continue;
				
				for(Edge_hasChildCategory cat : list) {
					
					String newURI = cat.getDestinationURI();
					
					if(URI2TaxonomyNode.containsKey(newURI)) {
						throw new IOException("Category node in two branches: " + newURI);
//						System.out.println("Category node in two branches: " + newURI);
//						continue;
					}
					
					VITAL_Category category = URI2cat.get(newURI);
					String label = (String) (category != null ? category.getProperty("name") : null);
					
					TaxonomyNode newNode = new TaxonomyNode(newURI, label, depth);
					newNode.setParent(node);
					node.getChildren().add(newNode);
					newList.add(newNode);
					
					URI2TaxonomyNode.put(newURI, newNode);
					
				}
				
			}
			
			taxonomyNodesToProcess = newList;
			
		}
		
		log.info("Taxonomy nodes count: " + URI2TaxonomyNode.size());
		
		
	}
	
	static Pattern pattern = Pattern.compile("(\\++)\\s+(\\S+)(\\s+)?(\".+[^\\\\]\")?", Pattern.CASE_INSENSITIVE);
	
	public HierarchicalCategories(File taxonomyFile) throws IOException {
		this(new FileInputStream(taxonomyFile), true);
	}
	
	public HierarchicalCategories(InputStream inputStream, boolean closeStream) throws IOException {
	
		BufferedReader reader = null;
		
		int nodes = 0;
		
		try {
			
			reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));

//			Pattern pattern = Pattern.compile("\\++");

			
			int lineno = 1;
			
			for(String l = reader.readLine(); l != null; l = reader.readLine(), lineno++) {
				
				l = l.trim();
				
				if(l.isEmpty() || l.startsWith("#")) continue;
				
				if(!l.startsWith("+")) throw new IOException("The line is expected to start with '+...'" + " - line " + lineno + ": " + l);
				
				Matcher matcher = pattern.matcher(l);
				
				if( ! matcher.matches() ) throw new IOException("The taxonomy line must match pattern: " + pattern.pattern() + " - line " + lineno + ": " + l);
				
				int depth = matcher.group(1).length() - 1;
				
				String URI = matcher.group(2);
				
				String label = null; 
				
				int gc = matcher.groupCount();
				
				if( gc > 3 ) {
					
					label = matcher.group(4);
				
					
				}
				
				if(label != null) {
					
					label = label.substring(1, label.length() - 1);
					
					label = StringEscapeUtils.unescapeJava(label);
					
				}
				
				/*
				String[] cols = l.split("\\s+");
				
				if(cols.length < 2) throw new IOException("Expected two columns");
				
				String level = cols[0];
				
				if(!pattern.matcher(level).matches()) throw new IOException("First columns is expected to match pattern: " + pattern.pattern());
				
				int depth = level.length() - 1;
				
				String URI = l.substring(level.length()).trim();
				*/
				
				if(depth == 0) {
					if(rootNode != null)throw new IOException("Two root nodes detected ! " + " - line " + lineno + ": " + l);
					rootNode = new TaxonomyNode(URI, label, depth);
					nodes++;
					continue;
					
				}
				
				if(rootNode == null) throw new IOException("No root node defined.");
				
				TaxonomyNode newNode = new TaxonomyNode(URI, label, depth);
				nodes++;

				TaxonomyNode parent = getLastLeaf(rootNode, depth);
				
				if(URI2TaxonomyNode.containsKey(newNode.getURI())) {
					throw new RuntimeException("Two nodes with same URIs detected: " + newNode.getURI() + " - line " + lineno + ": " + l);
				}
				URI2TaxonomyNode.put(newNode.getURI(), newNode);
				
				parent.getChildren().add(newNode);
				newNode.setParent(parent);
				
			}
		
		} finally {
			if(closeStream) IOUtils.closeQuietly(reader);
			reader = null;
		}
		
		log.info("Taxonomy nodes count: " + nodes);
		
	}
	
	
	private TaxonomyNode getLastLeaf(TaxonomyNode startNode, int childDepth) throws IOException {

		if(startNode.getDepth() == childDepth - 1) {
			return startNode;
		} else {
			
			//forward it to last child
			
			List<TaxonomyNode> chn = startNode.getChildren();
			
			if(chn.size() < 1) throw new IOException("Cannot find a parent node for child depth: " + childDepth + " : " + startNode.getURI() + " " + startNode.getLabel());
			
			return getLastLeaf(chn.get(chn.size()-1), childDepth); 
			
		}
	}

	public static String localURI(String uri) {
		int lastIndexOf = uri.lastIndexOf('/');
		if(lastIndexOf >0 && lastIndexOf < uri.length() - 1 ) {
			return uri.substring(lastIndexOf + 1);
		}
		return uri;
	}

	public static class TaxonomyNode {
		
		private String URI;

		String label;
		
		private Integer depth = 0;
		
		private List<TaxonomyNode> children = new ArrayList<TaxonomyNode>();
		
		private TaxonomyNode parent;
		
		public TaxonomyNode(String URI, String label, Integer depth) {
			this.URI = URI;
			this.depth = depth;
			if(label != null) {
				this.label = label;
			} else {
				this.label = URI2Label(URI);
			}
		}
		
		public TaxonomyNode(String URI, Integer depth) {
			this(URI, null, depth);
		}
		

		public String URI2Label(String URI) {

			String localURI = localURI(URI);
			
			String l = localURI.replace('_', ' ').trim();
			
			return l;
		}


		public String getURI() {
			return URI;
		}


		public void setURI(String uRI) {
			URI = uRI;
		}


		public List<TaxonomyNode> getChildren() {
			return children;
		}

		public void setChildren(List<TaxonomyNode> children) {
			this.children = children;
		}


		public TaxonomyNode getParent() {
			return parent;
		}


		public void setParent(TaxonomyNode parent) {
			this.parent = parent;
		}


		public Integer getDepth() {
			return depth;
		}


		public void setDepth(Integer depth) {
			this.depth = depth;
		}


		public String getLabel() {
			return label;
		}


		public void setLabel(String label) {
			this.label = label;
		}
	
		
		
	}

	private void printTaxonomy() {

		printNode(rootNode);
		
	}


	private void printNode(TaxonomyNode node ) {

		String l = "";
		for(int i = 0; i <= node.getDepth(); i++) {
			
			l += "+";
			
		}
		
		l+= ( " " + node.getLabel() );
		
		o(l);
		
		if(node.getDepth() == 1) {
			o("");
		}
		
		for(TaxonomyNode n : node.getChildren()) {
			printNode(n);
		}
		
		if(node.getDepth() == 1) {
			o("");
		}
		
	}


	public TaxonomyNode getRootNode() {
		return rootNode;
	}

}
