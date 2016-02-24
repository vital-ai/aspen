import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.modelmanager.AspenPrediction;
import com.vitalai.domain.nlp.Content;
import com.vitalai.domain.nlp.Document;
import com.vitalai.domain.nlp.Edge_hasCategory;
import com.vitalai.domain.nlp.TargetNode;
import ai.vital.predictmodel.Prediction

// the input is a block with all data needed for feature generation

// the pipeline DSL could be used to collect the data for the features
// so the input in that case would be the "main" object
// and pipeline DSL within features would be used to generate the components


// the model and features can be serialized via the graph object
// for model and feature nodes, such as:
// model123 / hasFeature / feature123
// feature123.name = "title"

// the feature functions may be serialized as strings within the
// feature node
// feature123.function = '''{ VitalBlock block, Map features ->
//	  	   def doc = block.filter(Document.class).get(0)
//		   return doc.title
//	  }'''



MODEL {

	value URI: 'my-uri'

	value name: 'reference-model'

	value type: 'reference-model'

	value algorithm: 'bayes'

	ALGORITHM {
		
		//value numTrees: 100
		
	}
	
	value preferredLocation: 'hdfs://somewhere'

	  // there is an input block, which minimally contains the main object
	  // it may contain other objects which could be used in the
	  // feature functions

	FEATURES {

		FEATURE {

			value URI: 'my-uri-1'

			value name: 'subject'

			value type: 'text'
			
		}

		FEATURE {

			value URI: 'my-uri-2'

			value name: 'body'

			value type: 'text'
	  
            value allowedMissing: true
            
		}
		
	}
	
	
	
	
	AGGREGATES {
		
	}


	
	
	// the annotations in the ontology can provide mappings
	// from property to feature name
	
	// these functions below would override those (if present)

	FEATURE_QUERIES {
		
		FEATURE_QUERY {
		
			value provides: 'subject'
		
		// the input will be the URI of the "main" object as $URI
		
		// vitalquery builder
		
		// the value of query is the VitalQuery output by the query builder
			value query : {
		
				GRAPH {
		
					value segments: ['20news']
					
					//root, uri will be set
					ARC {
						
						//edge
						ARC {
							edge_constraint { Edge_hasCategory.class }
						}
						
					}
					
					// top level is $URI
		
					// ARCs as needed to find normalized entities
					// query only captures edges and normalized entities in results.
					// the resulting container is merged with all other such containers to make up the final block.
		
				}
			}
		}
		
		
	}
	
	FUNCTIONS {

		FUNCTION {

			value provides: 'subject'

			value function: { VitalBlock block, Map features ->
				def doc = (Document) block.getMainObject()
				return doc.title?.toString()
			}

		}

		FUNCTION {

			value provides: 'body'

			value function: {  VitalBlock block, Map features ->
				def doc = (Document) block.getMainObject()
				return doc.body?.toString()
			}

		}
		
	}

	// returns the value to use for training
	// this could be specified in the ontology via annotations

	TRAIN {

        value type: 'numerical' 
        
		value function: { VitalBlock block, Map features ->

			def doc = block.filter(Document.class).get(0)

			def category_node = doc.getCategories(block).get(0)

			def value = category_node.value
		  
			return value
		}
		
	}


// returns the objects to assert for predictions
// this could include new objects and/or updated objects
// such as the doc object with a new property


// this could be specified in the ontology via annotations

	TARGET {

		value function: { VitalBlock block, Map features, Prediction result ->

			def doc = block.getMainObject()

            //the resource bytes            
            byte[] resourceBytes = getResourceBytes("csv/some.csv");
            
            InputStream resourceStream = getResource("/csvsome.csv");
            
            //process it, cache results etc
            
            resourceStream.close()
            
            Integer x = get("x")
            
            if(x == null) {
                x = 1
                // cache the x if not set
                put("x", 1)
            } else {
                // x already cached
            }
            
			def target = new TargetNode()
			target.generateURI()

			target.targetStringValue = ((AspenPrediction) result).getCategory()
			target.targetScore = 1D;

			def edge = doc.addEdge_hasTargetNode(target).generateURI()

			return [target, edge]

		}

	}
	

// end of MODEL
}
