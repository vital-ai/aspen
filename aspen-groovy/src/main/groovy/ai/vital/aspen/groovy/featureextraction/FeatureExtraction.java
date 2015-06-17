package ai.vital.aspen.groovy.featureextraction;

import groovy.lang.Closure;
import groovy.lang.GString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.predictmodel.BinaryFeature;
import ai.vital.predictmodel.CategoricalFeature;
import ai.vital.predictmodel.DateFeature;
import ai.vital.predictmodel.DateTimeFeature;
import ai.vital.predictmodel.Feature;
import ai.vital.predictmodel.FeatureBase;
import ai.vital.predictmodel.FeatureBase.RestrictionLevel;
import ai.vital.predictmodel.FeatureQuery;
import ai.vital.predictmodel.Function;
import ai.vital.predictmodel.GeoLocationFeature;
import ai.vital.predictmodel.NumericalFeature;
import ai.vital.predictmodel.OrdinalFeature;
import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.QueryElement;
import ai.vital.predictmodel.Restriction;
import ai.vital.predictmodel.StringFeature;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.predictmodel.TextFeature;
import ai.vital.predictmodel.TrainFeature;
import ai.vital.predictmodel.TrainQuery;
import ai.vital.predictmodel.URIFeature;
import ai.vital.predictmodel.WordFeature;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.GraphElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.URIReference;
import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.property.BooleanProperty;
import ai.vital.vitalsigns.model.property.DateProperty;
import ai.vital.vitalsigns.model.property.GeoLocationProperty;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.LongProperty;
import ai.vital.vitalsigns.model.property.IntegerProperty;
import ai.vital.vitalsigns.model.property.NumberProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.URIProperty;

public class FeatureExtraction {

	private PredictionModel model;
	
	private final static Logger log = LoggerFactory.getLogger(FeatureExtraction.class);
	
	Map<String, Double> AGGREGATE;
	
	Map<String, Taxonomy> TAXONOMY;

	public FeatureExtraction(PredictionModel model, Map<String, Double> aggregationValues) {
		this.model = model;
		this.AGGREGATE = aggregationValues;
		this.TAXONOMY = new HashMap<String, Taxonomy>();
		
		for(Taxonomy t : model.getTaxonomies()) {
			TAXONOMY.put(t.getProvides(), t);
		}
		
	}
	
	
	public Map<String, Taxonomy> getTAXONOMY() {
		return TAXONOMY;
	}


	/**
	 * Extracts features for given block, model functions must be sorted, aggregates must be calculated 
	 * @param model
	 * @param block
	 * @param aggregationValues
	 * @return
	 */
	public Map<String, Object> extractFeatures(VitalBlock block) {
		return extractFeatures(block, null);
	}
	
	
	public void composeBlock(VitalBlock block) {
		
		composeMainObject(block);
		
		for( FeatureQuery fq : model.getFeatureQueries()) {
			
			processQueryElement(block, fq);
		}
		
		for(TrainQuery tq : model.getTrainQueries()) {
			
			processQueryElement(block, tq);
			
		}
		
	}
	
	public void composeMainObject(VitalBlock block) {
		
		//check if a block is a special use case
		if(block.getMainObject() instanceof URIReference) {
			
//			if( modelConfig.getFeatureQueries().
			
			if(block.getDependentObjects().size() != 0) throw new RuntimeException("URIReference must be a single object in a block!");
			URIReference ref = (URIReference) block.getMainObject();
			Object v = ref.getProperty("uRIRef");
			if(v == null) throw new RuntimeException("uRIRef property not set in URIReference");
			URIProperty uri = (URIProperty) ((IProperty)v).unwrapped();
			
			VitalService service = VitalSigns.get().getVitalService();
			if(service == null) throw new RuntimeException("No active vitalservice found in VitalSigns singleton");
			ResultList rl = null;
			try {
				rl = service.get(GraphContext.ServiceWide, uri);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			if(rl.getStatus().getStatus() != VitalStatus.Status.ok) throw new RuntimeException("Error when getting graph object " + uri + ":" + rl.getStatus().getMessage());
			
			GraphObject first = rl.first();
			if(first == null) throw new RuntimeException("Graph object not found: " + uri);
			
			block.setMainObject(first);
		
		} else {
			
			throw new RuntimeException("Main object expected to be a URIReference");
			
		}
		
	}
	
	private void processQueryElement(VitalBlock block, QueryElement fq) {

		Map<String, GraphObject> map = new HashMap<String, GraphObject>();
		map.put(block.getMainObject().getURI(), block.getMainObject());
		
		for(GraphObject g : block.getDependentObjects()) {
			map.put(g.getURI(), g);
		}
		
		//execute the query
		VitalQuery vq = null;
		try {
			vq = (VitalQuery) fq.getQuery().clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
		
		if(vq instanceof VitalGraphQuery) {
			
			VitalGraphQuery vgq = (VitalGraphQuery) vq;
			VitalGraphCriteriaContainer cc = new VitalGraphCriteriaContainer();
			URIProperty rootURI = URIProperty.withString(block.getMainObject().getURI());
			VitalGraphQueryPropertyCriterion c = new VitalGraphQueryPropertyCriterion(VitalGraphQueryPropertyCriterion.URI).equalTo(rootURI);
			c.setSymbol(GraphElement.Source);
			cc.add(c);
			vgq.getTopContainer().add(cc);
			vgq.setPayloads(true);

			//TODO $URI references properties filter
			
			VitalService service = VitalSigns.get().getVitalService();
			if(service == null) throw new RuntimeException("No active vitalservice found in VitalSigns singleton");
			
			ResultList queryRS = null;
			try {
				queryRS = service.query(vq);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			if(queryRS.getStatus().getStatus() != VitalStatus.Status.ok) {
				throw new RuntimeException("Vital graph query exception: " + queryRS.getStatus().getMessage());
			}
			
			for(GraphObject g : queryRS) {
				GraphMatch gm = (GraphMatch) g;
				for(Entry<String, IProperty> entry : gm.getPropertiesMap().entrySet() ) {
					if(entry.getValue().unwrapped() instanceof URIProperty) {
						String objURI = ((URIProperty)entry.getValue().unwrapped()).get();
						if(map.containsKey(objURI)) continue;
						Object property = gm.getProperty(objURI);
						if(property != null) {
							GraphObject x = (GraphObject) property;
							map.put(x.getURI(), x);
							block.getDependentObjects().add(x);
						}
					}
					
				}
				
			}
			
			
		} else {
			throw new RuntimeException("Query not supported in builder yet: " + vq.getClass().getCanonicalName());
		}
		
		
	}
	
	/**
	 * extracts features for a given block
	 * the block is updated while the extraction is progressing
	 * @param block
	 * @param functionsFilter
	 * @return
	 */
	public Map<String, Object> extractFeatures(VitalBlock block, List<Function> functionsFilter) {
		
		//decompose block
		boolean composeBlock = false;
		
		//check if a block is a special use case
		if(block.getMainObject() instanceof URIReference) {
			
			composeMainObject(block);
			
			composeBlock = true;
			
		}
		
		
		Map<String, Object> extractedFeatures = new HashMap<String, Object>();
		
		//this variable will be visible in closure scope
		//Map<String, Number> 
		
		for( Function function : model.getFunctions()) {
			
			if(composeBlock) {
				
				if(model.getFeatureQueries() != null) {
					
					for(FeatureQuery fq : model.getFeatureQueries()) {

						if(function.getProvides().equals( fq.getProvides() ) ) {
							
							processQueryElement(block, fq);
							
						}
						
					}
				}
				
			}
			
			boolean fpassed = true;
			if(functionsFilter != null) {
				fpassed = functionsFilter.contains(function);
			}
			
			if(!fpassed) continue;
			
			
			Feature feature = null;
			for(Feature f : model.getFeatures()) {
				if(function.getProvides().equals(f.getName()) || function.getProvides().equals(f.getURI())) {
					feature = f;
					break;
				}
			}
			
			if(feature == null) throw new RuntimeException("Feature with name or URI: " + function.getProvides() + " not found");
			
			@SuppressWarnings("rawtypes")
			Closure closure = function.getFunction();
			closure = closure.rehydrate(this, this, this);
			closure.setDelegate(this);
			Object x = closure.call(block, extractedFeatures);
			
			String fn = feature.getName();
			
			if(x == null) {
				if(!feature.getAllowedMissing()) throw new RuntimeException("Feature " + fn + " missing values are not allowed!");
				
				extractedFeatures.put(fn, null);
				extractedFeatures.put(feature.getURI(), null);
				
				continue;
				
			}
			
			List<Object> vals = new ArrayList<Object>();
			
			if(x instanceof Collection) {
				if(!feature.getMultivalue()) throw new RuntimeException("Feature " + fn + " is not a multivalue one, it does not accept a collection of objects");
				vals.addAll((Collection<? extends Object>) x);
			} else {
				vals.add(x);
			}
			
			for(Object v : vals) {
				
				validateFeatureValue(feature, v);
				
			}
			
			extractedFeatures.put(fn, x);
			extractedFeatures.put(feature.getURI(), x);
			
		}
		
		return extractedFeatures;
		
	}


	public void validateFeatureValue(FeatureBase featureBase, Object v) {
		
		if(v instanceof IProperty) v = ((IProperty)v).unwrapped();
		
		if(featureBase instanceof Feature) {
			Feature feature = (Feature) featureBase;
		
			RestrictionLevel restrictionLevel = feature.getRestrictionLevel();
			if(restrictionLevel == null) restrictionLevel = RestrictionLevel.unchecked;
			
			if(feature instanceof BinaryFeature) {
	
				if(v instanceof Boolean) {
					
				} else if(v instanceof BooleanProperty) {
					
//					v = ((BooleanProperty)v).booleanValue();
					
				} else {
					throw new RuntimeException("Binary feature " + feature.getName() + " only accepts booleans");
				}
				
			} else if(feature instanceof CategoricalFeature) {
				
				CategoricalFeature cf = (CategoricalFeature) feature;
				
				
				if(v instanceof VITAL_Category) {
					
					VITAL_Category c = (VITAL_Category) v;
					
					Taxonomy taxonomy = TAXONOMY.get(cf.getTaxonomy());
					if(taxonomy == null) throw new RuntimeException("Taxonomy not found: " + cf.getTaxonomy());
					
					//root category is set by the training phase
					if( !taxonomy.isIntrospect() || taxonomy.getRootCategory() != null ) {
						
						String catURI = c.getURI();
						if( taxonomy.getRootCategory().getURI().equals(catURI) ) throw new RuntimeException("Category URI must not be equal to root URI");
						
						GraphObject graphObject = taxonomy.getContainer().get(catURI);
						if(graphObject == null) throw new RuntimeException("Category with URI " + catURI + " not found in taxonomy: " + cf.getTaxonomy());
						
						if(!(graphObject instanceof VITAL_Category)) throw new RuntimeException("Category with URI " + catURI + " is not a category in container, but: " + graphObject.getClass().getCanonicalName());
						
					}
					
				} else {
					
					throw new RuntimeException("Categorical feature must return a category node");
					
				}
				
			} else if(feature instanceof DateFeature) {
				
				if(v instanceof Date) {
				} else if(v instanceof DateProperty) {
					
//					v = ((DateProperty)v).getDate();
					
				} else {
					throw new RuntimeException("Date feature " + feature.getName() + " only accepts dates");
				}
				
			} else if(feature instanceof DateTimeFeature) {
	
				if(v instanceof Date) {
					
				} else if(v instanceof DateProperty) {
					
//					v = ((DateProperty)v).getDate();
					
				} else {
					throw new RuntimeException("Datetime feature " + feature.getName() + " only accepts dates");
				}
				
			} else if( feature instanceof GeoLocationFeature ) {
			
				if(v instanceof GeoLocationProperty) {
					
				} else {
					throw new RuntimeException("GeoLocation feature " + feature.getName() + " only accepts GeoLocationProperty instances");
				}
			} else if(feature instanceof NumericalFeature) {
				
				if(v instanceof Number) {
					
				} else if(v instanceof NumberProperty) {
					
//					v = ((NumberProperty)v).doubleValue();
					
				} else {
					throw new RuntimeException("Numerical Feature " + feature.getName() + " only accepts numbers");
				}
				
				
			} else if(feature instanceof OrdinalFeature) {
				
				if(v instanceof Integer || v instanceof Long) {
				
				} else if(v instanceof IntegerProperty || v instanceof LongProperty) {
					
				} else {
					throw new RuntimeException("Ordinal feature " + feature.getName() + " only accepts integers/longs");
				}
				
			} else if(feature instanceof StringFeature) {
	
				if(v instanceof String || v instanceof GString) {
				
				} else if(v instanceof StringProperty) {
					
				} else {
					throw new RuntimeException("String Feature " + feature.getName() + " only accepts strings");
				}
				
			} else if(feature instanceof TextFeature) {
				
				if(v instanceof String || v instanceof GString) {
				
				} else if(v instanceof StringProperty) {
					
//					v = ((StringProperty)v).toString();
					
				} else {
					throw new RuntimeException("Text Feature " + feature.getName() + " only accepts strings");
				}
				
			} else if(feature instanceof URIFeature) {
				
				if(v instanceof String || v instanceof GString || v instanceof URIProperty) {
					
				} else {
					throw new RuntimeException("URI Feature " + feature.getName() + " only accepts strings or URIProperty");
				}
				
			} else if(feature instanceof WordFeature) {
				
				if(v instanceof String || v instanceof GString) {
					
				} else if(v instanceof StringProperty) {
					
				} else {
					throw new RuntimeException("Word Feature " + feature.getName() + " only accepts strings");
				}
				
			} else {
				
				throw new RuntimeException("unknown feature type: " + feature.getClass());
				
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
		} else if(featureBase instanceof TrainFeature) {
			
			TrainFeature tf = (TrainFeature) featureBase;
			
			Feature f = null;
			
			try {
				f = tf.getType().newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			f.setName("TRAIN");
			f.setRestrictionLevel(tf.getRestrictionLevel());
			f.setRestrictions(tf.getRestrictions());
			f.setURI("TRAIN");
			
			if(f instanceof CategoricalFeature) {
				CategoricalFeature cf = (CategoricalFeature) f;
				cf.setTaxonomy(tf.getTaxonomy());
			}
			
			validateFeatureValue(f, v);
			
		} else {
			throw new RuntimeException("unhanlded feature type: " + featureBase.getClass());
		}
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
 