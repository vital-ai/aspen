package ai.vital.aspen.test

import ai.vital.aspen.logic.engine._

import java.io._


object PrologTest {

  
  def main(args: Array[String]) {
    

    var engine = new PrologEngine()
    
    // note: this is a just a quick test
    //  need to fix program loading relative to place in dist
    
    
    var code = "src/main/prolog/hanoi.pl"
    
    engine.loadProlog(new File(code))
    
    engine.solve("move(3,left,right,center).")

    
    
  }
  
}