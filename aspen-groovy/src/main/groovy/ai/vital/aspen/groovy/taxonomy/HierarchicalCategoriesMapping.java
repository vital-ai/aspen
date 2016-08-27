package ai.vital.aspen.groovy.taxonomy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.taxonomy.HierarchicalCategories.TaxonomyNode;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.Edge_hasChildCategory;
import ai.vital.vitalsigns.model.VITAL_Category;

public class HierarchicalCategoriesMapping {

	static void o(String m) { System.out.println(m); }

	private final static Logger log = LoggerFactory.getLogger(HierarchicalCategoriesMapping.class);
	
	public final static String DMOZ_PREFIX = "DMOZ:";
	
	public final static String URL_PREFIX = "URL:";
	
	public final static String CATEGORY_PREFIX = "CATEGORY:";
	

	//from now on the relation may be M:N
	
	public Map<String, Set<String>> taxonomy2OtherCategory = new HashMap<String, Set<String>>();
	public Map<String, Set<String>> other2TaxonomyCategory = new HashMap<String, Set<String>>();
	
	public Map<String, Set<String>> taxonomy2DmozCategory = new HashMap<String, Set<String>>();
	public Map<String, Set<String>> dmoz2TaxonomyCategory = new HashMap<String, Set<String>>();
	
	public Map<String, Set<String>> taxonomy2URL = new HashMap<String, Set<String>>();
	public Map<String, Set<String>> URL2taxonomyCategory = new HashMap<String, Set<String>>();
	
	public HierarchicalCategories categories;
	
	List<HierarchicalCategories> otherTaxnomies = new ArrayList<HierarchicalCategories>();
	
	public HierarchicalCategoriesMapping(File taxonomyFile, File mappingFile) throws IOException {
		this(new HierarchicalCategories(taxonomyFile), mappingFile);
		
	}
	
	public HierarchicalCategoriesMapping(String taxonomyRootURI, File mappingFile) throws IOException {
		this(new HierarchicalCategories(taxonomyRootURI), mappingFile);
	}
		
	public HierarchicalCategoriesMapping(HierarchicalCategories hierarchicalCategories, File mappingFile) throws IOException {

		this.categories = hierarchicalCategories;
		
		Map<String, VITAL_Category> otherCategories = new HashMap<String, VITAL_Category>();
		
		for(VITAL_Category category : VitalSigns.get().listDomainIndividuals(VITAL_Category.class, null)) {
			
			if ( this.categories.URI2TaxonomyNode.containsKey(category.getURI()) ) continue;
			
			otherCategories.put(category.getURI(), category);
			
		}
		
		//the only way to find other taxonomies is to look for nodes that are not child category edges destination
		
		Histogram<String> destURI2Count = new Histogram<String>();
		for(Edge_hasChildCategory e : VitalSigns.get().listDomainIndividuals(Edge_hasChildCategory.class, null)) {
			destURI2Count.increment(e.getDestinationURI());
		}
		
		for(VITAL_Category c : otherCategories.values()) {
			Integer integer = destURI2Count.get(c.getURI());
			if(integer == null || integer.intValue() < 1) {
				otherTaxnomies.add(new HierarchicalCategories(c.getURI()));
			}
		}
		
		//validate if other nodes are in taxonomies as well?
		
		BufferedReader reader = null;
		
		try {
			
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(mappingFile)));
			
			int n = 0;
			
			int mappingsCount = 0;
			
			for( String l = reader.readLine(); l != null ; l = reader.readLine() ) {
				
				l = l.trim();
				
				n++;
				
				if(l.isEmpty() || l.startsWith("#")) continue;
				
				String[] columns = l.split("\\s+");
				
				if(columns.length != 3) throw new RuntimeException("Expected 3 chunks, line " +n + ": " + l);
			
				String rel = columns[1];
				
				
				if(rel.equals("includes")) {
					
					String taxonomyCat = columns[0];
					
					String cat = columns[2];
					
					Map<String, Set<String>> targetMap = null;
					
					Map<String, Set<String>> targetReverseMap = null;
					
					TaxonomyNode node = categories.URI2TaxonomyNode.get(taxonomyCat);
					
					if( node == null ) throw new RuntimeException("Unknown taxonomy category: "+ taxonomyCat + ", line " + n + ": "+ l);

					String nonPref = null;
					
					if( cat.startsWith(DMOZ_PREFIX)) {
						
						targetMap = taxonomy2DmozCategory;
						
						targetReverseMap = dmoz2TaxonomyCategory;
					
						nonPref = cat.substring(DMOZ_PREFIX.length());
						
						/*
						if(nonPref.startsWith("http://www.dmoz.org/")) {
							nonPref = nonPref.substring("http://www.dmoz.org/".length());
						}
						
						if(nonPref.startsWith("/")) {
							nonPref = nonPref.substring(1);
						}
						if(nonPref.endsWith("/")) {
							nonPref = nonPref.substring(0, nonPref.length() - 1); 
						}
						
						if(!nonPref.startsWith("Top/")) {
							nonPref = "Top/" + nonPref;
						}
						*/
						
					} else if( cat.startsWith(URL_PREFIX) ) {
						
						targetMap = taxonomy2URL;
						targetReverseMap = URL2taxonomyCategory;
						
						nonPref = cat.substring(URL_PREFIX.length());
						
					} else {
						
						String catURI = null;
						if(!cat.startsWith(CATEGORY_PREFIX)) {
							log.warn("Default " + CATEGORY_PREFIX + " prefix assumed for " + cat);
							catURI = cat;
						} else {
							catURI = cat.substring(CATEGORY_PREFIX.length());
						}
						
						if(categories.URI2TaxonomyNode.containsKey(catURI)) throw new RuntimeException("Cannot map to category in same tree: " + taxonomyCat + " -> " + catURI + " line number: " + n);
						
						if( ! otherCategories.containsKey(catURI) ) throw new RuntimeException("Target category not found: " + catURI + " line number: " + n);
						
						targetMap = taxonomy2OtherCategory;
						
						targetReverseMap = other2TaxonomyCategory;
						
						nonPref = catURI;
						
					}
					
					Set<String> set = targetMap.get(taxonomyCat);
					if(set == null) {
						set = new HashSet<String>();
						targetMap.put(taxonomyCat, set);
					}
					
					
//					nonPref = cat;
					
					if( !set.add(nonPref) ) throw new RuntimeException("Duplicated mapping: " + taxonomyCat + " " + rel + " " + cat + " line number: "+ n);
					
					Set<String> set2 = targetReverseMap.get(nonPref);
					if(set2 == null) {
						set2 = new HashSet<String>();
						targetReverseMap.put(nonPref, set2);
					}
					
					if( set2.contains(taxonomyCat) ) {
						log.warn("Duplicated inverse mapping: " + cat + " -> " + taxonomyCat + " line number: " + n);
//						throw new RuntimeException("Duplicated inverse mapping: " + cat + " -> " + shCat + " line number: " + n);
//						throw new RuntimeException("Duplicated inverse mapping: " + cat + " -> " + shCat + " line number: " + n);
					}
					
					set2.add(taxonomyCat);
//					targetReverseMap.put(nonPref, shCat);
					
					mappingsCount++;
					
				} else {
					throw new RuntimeException("Unsupported relation: " + rel + ", line " + n + ": " + l);
				}
				
			}
			
			log.info("Total mappings count: " + mappingsCount);
			
			log.info("Taxonomy -> DMOZ mappings count: " + taxonomy2DmozCategory.size());
			
			log.info("Taxonomy -> URL mappings count: " + taxonomy2URL.size());
			
			log.info("Taxonomy -> other taxonomy mappings count: " + taxonomy2OtherCategory.size());
			
		} finally {
			if(reader != null) reader.close();
			reader = null;
		}
			
	}
	
	
	


	public Set<String> getTaxonomyCategory(String inputCategory) {
		
//		
		
		//direct hits
		
		Set<String> set = other2TaxonomyCategory.get(inputCategory);

		if(set == null) set = new HashSet<String>();
		
		for(HierarchicalCategories o : otherTaxnomies) {
			
			TaxonomyNode tn = o.URI2TaxonomyNode.get(inputCategory);
			
			while(tn != null) {
					
				Set<String> s = other2TaxonomyCategory.get(tn.getURI());
				if(s != null) {
					set.addAll(s);
				}
				tn = tn.getParent();
					
			}
			
		}
		
		return set;
		
	}
	
	//dmozPath assumed to be an url
	public Set<String> getTaxonomyCategoriesMappedToDMOZ(String dmozPath) {
		
		Set<String> m = new HashSet<String>();
		
		//first iterate to find exact matches
		
		Set<String> direct = dmoz2TaxonomyCategory.get(dmozPath);
		
		if(direct != null) {
			m.addAll(direct);
		}
		
//	
		
		return m;
		
	}
}
