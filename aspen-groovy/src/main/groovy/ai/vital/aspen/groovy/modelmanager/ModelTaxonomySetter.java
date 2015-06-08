package ai.vital.aspen.groovy.modelmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.utils.StringUtils;

public class ModelTaxonomySetter {

	//performs a pass over PredictionModel object and loads taxonomies objects either from VitalSigns/Vitalservice (rootURI)
	//path
	public static void loadTaxonomies(PredictionModel model, VitalService forcedService) throws Exception {
		
		List<Taxonomy> taxonomies = model.getTaxonomies();
		
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
						forcedService = VitalServiceFactory.getVitalService();
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
				
				//TODO copy from vital taxonomy file
				throw new RuntimeException("NOT IMPLEMENTED!");
				
			}
				
			
		}
		
	}
	
}
