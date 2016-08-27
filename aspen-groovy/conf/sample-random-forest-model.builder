Model {
  name: "spark-randomforest-prediction"
  URI: "http://vital.ai/models/spark/randomforest-prediction"
  type: "spark-randomforest-prediction"
  query: """
			GRAPH {
				
				ARC {
					
					node_constraint { 'URI = $URI' }
					
					ARC {
						//accept everything depth 1
					}
					
				}
				
			}
			
  """
}

Features : [ 
 {
  name: "document-title"
  URI: "http://vital.ai/models/spark/randomforest-document-title"
  type: "text"
  value: "ai.vital.domain.Document.title" 
 }
 {
  name: "document-body"
  URI: "http://vital.ai/models/spark/randomforest-document-body"
  type: "text"
  value: "ai.vital.domain.Document.body" 
 }
]