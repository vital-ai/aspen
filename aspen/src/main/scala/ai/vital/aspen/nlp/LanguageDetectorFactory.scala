package ai.vital.aspen.nlp

import ai.vital.aspen.groovy.nlp.steps.LanguageDetectorStep

object LanguageDetectorFactory {
  
  var languageDetectorStep : LanguageDetectorStep = null
  
  def getInstance() : LanguageDetectorStep = {
    
    if(languageDetectorStep == null) {
      
      LanguageDetectorFactory.synchronized {
        
    	  if(languageDetectorStep == null) {
    		  languageDetectorStep = new LanguageDetectorStep()
    		  languageDetectorStep.init()
        }
        
      }
    }
    
    return languageDetectorStep
    
  }
  
}

