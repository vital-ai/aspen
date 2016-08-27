package ai.vital.aspen.groovy.modelmanager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ai.vital.aspen.groovy.taxonomy.HierarchicalCategories;
import ai.vital.aspen.groovy.taxonomy.HierarchicalCategories.TaxonomyNode;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.Edge_hasChildCategory;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.utils.StringUtils;

public class ModelTaxonomySetter {

	//performs a pass over PredictionModel object and loads taxonomies objects either from VitalSigns/Vitalservice (rootURI)
	//path
	public static void loadTaxonomies(AspenModel aspenModel, VitalService forcedService) throws Exception {
		
		List<Taxonomy> taxonomies = aspenModel.getModelConfig().getTaxonomies();
		
		if(taxonomies == null) return;
		
		for(Taxonomy taxonomy : taxonomies) {
			
			if( taxonomy.getRootCategory() != null ) continue;
			
			if( ! StringUtils.isEmpty(taxonomy.getRoot()) ) {
				
				String rootURI = taxonomy.getRoot();
				
				VITAL_Category rootCategory = null;
				
				GraphObject individual = VitalSigns.get().getIndividual(rootURI);
				
				ResultList rl = null;
				
				if(individual != null) {
					if(individual instanceof VITAL_Category) {
						rootCategory = (VITAL_Category) individual;
					} else {
						throw new IOException("Graph object with URI: " + rootURI + " is not a VITAL_Category (found in vitalsigns model)");
					}
						
					rootCategory = (VITAL_Category) individual;
				
					VitalPathQuery pq = ModelTaxonomyQueries.getTaxonomyPathQuery(rootURI);
					
					rl = VitalSigns.get().query(pq);
					
				}
				
				if(rootCategory == null) {
					
					if(forcedService == null) {
						forcedService = VitalSigns.get().getVitalService();
						if(forcedService == null) throw new RuntimeException("Vitalservice instance not set in vitalsigns singleton");
//						throw new RuntimeException("Service not set, cannot lookup taxonomy root");
					}
					
					individual = forcedService.get(GraphContext.ServiceWide, URIProperty.withString(rootURI)).first();
					
					if(individual != null) {
						if(individual instanceof VITAL_Category) {
							rootCategory = (VITAL_Category) individual;
						} else {
							throw new IOException("Graph object with URI: " + rootURI + " is not a VITAL_Category (found in vitalservice)");
						}
					}
					
					if(rootCategory != null) {
						
						VitalPathQuery pq = ModelTaxonomyQueries.getTaxonomyPathQuery(rootURI);
						rl = forcedService.query(pq);
						
					}
					
				}
				
				
				if(rootCategory == null) throw new RuntimeException("Taxonomy root not found, neither in vitalsigns model nor vitalserivce");
				
				if(VitalStatus.Status.ok != rl.getStatus().getStatus()) throw new RuntimeException("Path query error: " + rl.getStatus().getMessage());
				
				taxonomy.setRootCategory(rootCategory);
				
				VITAL_Container container = new VITAL_Container();
				List<GraphObject> l = new ArrayList<GraphObject>();
				for(GraphObject g : rl) {
					l.add(g);
				}
				container.putGraphObjects(l);
				taxonomy.setContainer(container);
				
			} else if( ! StringUtils.isEmpty(taxonomy.getTaxonomyPath()) ) {
				
				Path p = new Path(taxonomy.getTaxonomyPath());
				
				FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
				
				InputStream inS = null;
				
				HierarchicalCategories hc = null;
				
				String content = null;
				
				try {
					
					inS = fs.open(p);
					
					content = IOUtils.toString(inS, "UTF-8");
					
					hc = new HierarchicalCategories(new ByteArrayInputStream(content.getBytes("UTF-8")), true);
					
				} finally {
					IOUtils.closeQuietly(inS);
				}
				
				TaxonomyNode rootNode = hc.getRootNode();
				
				VITAL_Container container = new VITAL_Container();
				VITAL_Category root = processTaxonomyNode(container, null, rootNode);
				taxonomy.setRootCategory(root);
				taxonomy.setContainer(container);

				aspenModel.getTaxonomy2FileContent().put(taxonomy.getProvides(), content);
				
				
			}
				
			
		}
		
	}

	public static VITAL_Category processTaxonomyNode(VITAL_Container container, VITAL_Category parentNode, 
			TaxonomyNode node) {

		VITAL_Category category = new VITAL_Category();
		category.setURI(node.getURI());
		category.setProperty("name", node.getLabel());
		
		if(parentNode != null) {
			Edge_hasChildCategory edge = new Edge_hasChildCategory();
			edge.generateURI((VitalApp)null);
			edge.addSource(parentNode).addDestination(category);
			container.putGraphObject(edge);
		}
		container.putGraphObject(category);
		
		List<TaxonomyNode> children = node.getChildren();
		
		if(children != null) {
			for(TaxonomyNode child : children) {
				
				processTaxonomyNode(container, category, child);
				
			}
		}
		
		return category;
		
	}
	
}
