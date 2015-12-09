package ai.vital.aspen.analysis.ibmwatson

import scala.beans.BeanProperty

@SerialVersionUID(1L)
class IBMWatsonProfile extends java.io.Serializable {
 
  @BeanProperty
  var id : String = null
  
  @BeanProperty
  var source : String = null
  
  @BeanProperty
  var word_count : java.lang.Long = null
  
  @BeanProperty
  var word_count_message : String = null
  
  @BeanProperty
  var processed_lang : String = null
  
  @BeanProperty
  var tree : IBMWatsonTrait = null
  
}