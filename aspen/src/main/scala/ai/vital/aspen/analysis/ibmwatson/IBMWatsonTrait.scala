package ai.vital.aspen.analysis.ibmwatson

import scala.beans.BeanProperty

@SerialVersionUID(1L)
class IBMWatsonTrait extends java.io.Serializable {
  
  @BeanProperty
  var id : String = null
  
  @BeanProperty
  var name : String = null
  
  @BeanProperty
  var category : String = null
  
  @BeanProperty
  var percentage : java.lang.Double = null
  
  @BeanProperty
  var sampling_error : java.lang.Double = null
  
  @BeanProperty
  var raw_score : java.lang.Double = null
  
  @BeanProperty
  var raw_sampling_error : String = null
  
  @BeanProperty
  var children : java.util.List[IBMWatsonTrait] = null
  
}