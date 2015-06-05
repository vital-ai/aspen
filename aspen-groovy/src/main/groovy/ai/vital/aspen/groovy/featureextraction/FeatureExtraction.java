package ai.vital.aspen.groovy.featureextraction;

import groovy.lang.Closure;
import groovy.lang.GString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.predictmodel.CategoricalFeature;
import ai.vital.predictmodel.Feature;
import ai.vital.predictmodel.Feature.RestrictionLevel;
import ai.vital.predictmodel.Function;
import ai.vital.predictmodel.NumericalFeature;
import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.Restriction;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.predictmodel.TextFeature;
import ai.vital.predictmodel.WordFeature;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Category;

public class FeatureExtraction {

	private PredictionModel model;
	
	private final static Logger log = LoggerFactory.getLogger(FeatureExtraction.class);
	
	Map<String, Double> AGGREGATE;
	
	Map<String, TaxonomyWrapper> TAXONOMY;

	public FeatureExtraction(PredictionModel model, Map<String, Double> aggregationValues) {
		this.model = model;
		this.AGGREGATE = aggregationValues;
		this.TAXONOMY = new HashMap<String, TaxonomyWrapper>();
		
		for(Taxonomy t : model.getTaxonomies()) {
			TAXONOMY.put(t.getProvides(), new TaxonomyWrapper(t));
		}
		
	}
	
	/**
	 * Extracts features for given block, model functions must be sorted, aggregates must be calculated 
	 * @param model
	 * @param block
	 * @param aggregationValues
	 * @return
	 */
	@SuppressWarnings("unchecked")
	
	public Map<String, Object> extractFeatures(VitalBlock block) {
		return extractFeatures(block, null);
	}
	
	public Map<String, Object> extractFeatures(VitalBlock block, List<Function> functionsFilter) {
		
		Map<String, Object> extractedFeatures = new HashMap<String, Object>();
		
		//this variable will be visible in closure scope
		//Map<String, Number> 
		
		for( Function function : model.getFunctions()) {
			
			boolean fpassed = true;
			if(functionsFilter != null) {
				fpassed = functionsFilter.contains(function);
			}
			
			if(!fpassed) continue;
			
			
			Feature feature = null;
			for(Feature f : model.getFeatures()) {
				if(function.getProvides().equals(f.getName())) {
					feature = f;
					break;
				}
			}
			
			if(feature == null) throw new RuntimeException("Feature with name not found");
			
			Closure closure = function.getFunction();
			closure = closure.rehydrate(this, this, this);
			closure.setDelegate(this);
			Object x = closure.call(block, extractedFeatures);
			
			if(x == null) {
				if(!feature.getAllowedMissing()) throw new RuntimeException("Feature " + feature.getName() + " missing values are not allowed!");
				
				extractedFeatures.put(feature.getName(), null);
				
				continue;
				
			}
			
			List<Object> vals = new ArrayList<Object>();
			
			if(x instanceof Collection) {
				if(!feature.getMultivalue()) throw new RuntimeException("Feature " + feature.getName() + " is not a multivalue one, it does not accept a collection of objects");
				vals.addAll((Collection<? extends Object>) x);
			} else {
				vals.add(x);
			}
			
			for(Object v : vals) {
				
				RestrictionLevel restrictionLevel = feature.getRestrictionLevel();
				if(restrictionLevel == null) restrictionLevel = RestrictionLevel.unchecked;
				
				if(feature instanceof TextFeature) {
					
					if(v instanceof String || v instanceof GString) {
						
					} else {
						throw new RuntimeException("Text Feature " + feature.getName() + " only accepts strings");
					}
					
				} else if(feature instanceof NumericalFeature) {
					
					if(v instanceof Number || v instanceof Date) {
						
					} else {
						throw new RuntimeException("Numerical Feature " + feature.getName() + " only accepts numbers");
					}
					
				} else if(feature instanceof WordFeature) {
					
//					throw new
//					if(v instanceof Integer || v instanceof Long || v instanceof String || v instanceof GString) {
//					}
					
				} else if(feature instanceof CategoricalFeature) {
					
					CategoricalFeature cf = (CategoricalFeature) feature;
					
					if(v instanceof String || v instanceof GString) {
						
						TaxonomyWrapper taxonomyWrapper = TAXONOMY.get( cf.getTaxonomy() );
						if(taxonomyWrapper == null) throw new RuntimeException("Categorical feature taxonomy not found");
						
						Taxonomy taxonomy = taxonomyWrapper.taxonomy;
						
						String catURI = v.toString();
						if( taxonomy.getRootCategory().getURI().equals(catURI) ) throw new RuntimeException("Category URI must not be equal to root URI");
						
						GraphObject graphObject = taxonomy.getContainer().get(catURI);
						if(graphObject == null) throw new RuntimeException("Category with URI " + catURI + " not found in taxonomy: " + cf.getTaxonomy());
						
						if(!(graphObject instanceof VITAL_Category)) throw new RuntimeException("Category with URI " + catURI + " is not a category in container, but: " + graphObject.getClass().getCanonicalName());
						
						
						//ok
						
						
						
					} else {
						
						throw new RuntimeException("Categorical feature " + feature.getName() + " only accepts strings");
						
					}
					
				}
				
				if(restrictionLevel != null && restrictionLevel != RestrictionLevel.unchecked) {
					
					if( feature.getRestrictions() != null && feature.getRestrictions().size() > 0) {
						
						if(!(feature instanceof NumericalFeature)) throw new RuntimeException("Only numerical features restrictions supported, current type: " + feature.getClass().getCanonicalName());
						
						boolean passed = true;
						
						String msg = "";
						
						for(Restriction restriction : feature.getRestrictions()) {
							
							Object maxValueExclusive = restriction.getMaxValueExclusive();
							
							if(maxValueExclusive != null) {
								
								String m = validate(v, maxValueExclusive, true, false);
								
								if(m != null) msg += m + "\n"; 
								
							}
							
							Object maxValueInclusive = restriction.getMaxValueInclusive();
							
							if(maxValueInclusive != null) {
								String m = validate(v, maxValueInclusive, true, true);
								if(m != null) msg += m + "\n"; 
							}
							
							Object minValueExclusive = restriction.getMinValueExclusive();
							if(minValueExclusive != null) {
								String m = validate(v, minValueExclusive, false, false);
								if(m != null) msg += m + "\n"; 
							}
							
							Object minValueInclusive = restriction.getMinValueExclusive();
							if(minValueInclusive != null) {
								String m = validate(v, minValueInclusive, false, true);
								if(m != null) msg += m + "\n"; 
							}
							
							
						}
						
						if(msg.length() > 0) {
							
							if(restrictionLevel == RestrictionLevel.warning) {
								
								log.warn("Feature " + feature.getName() + " restriction not passed: " + msg.trim());
								
								
							} else {
								
								throw new RuntimeException(msg);
								
							}
						}
						
					}
					
				}
				
			}
			
			extractedFeatures.put(feature.getName(), x);
			
		}
		
		return extractedFeatures;
		
	}

	private String validate(Object v, Object res, boolean max,
			boolean inclusive) {

		double v1 = 0;
		double v2 = 0;
		
		if(v instanceof Number) {
			v1 = ((Number)v).doubleValue();
		} else if(v instanceof Date) {
			v1 = new Long( ((Date)v).getTime() ).doubleValue();
		} else {
			throw new RuntimeException("Only numbers or dates supported at this moment");
		}
		
		if(res instanceof Number) {
			v2 = ((Number)res).doubleValue();
		} else if(v instanceof Date) {
			v2 = new Long( ((Date)res).getTime() ) .doubleValue();
		} else {
			throw new RuntimeException("Only numbers or dates supported at this moment");
		}
		
		if(max) {
			
			if(inclusive) {
				
				if(v1 > v2) {
				
					return "max inclusive: " + res + " input:" + v;
					
				}
				
			} else {
				
				if(v1 >= v2) {
					
					return "max exclusive: " + res + " input:" + v;
					
				}
				
			}
			
		} else {
			
			if(inclusive) {
				
				if( v1 < v2 ) {
					
					return "min inclusive: " + res + " input: " + v;
					
				}
				
			} else {
				
				if( v1 <= v2 ) {
					
					return "min exclusive: " + res + " input: " + v;
					
				}
				
			}
			
		}
		
		return null;
	}
	
}
 