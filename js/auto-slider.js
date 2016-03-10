$(function(){

  var currentTimer = null;

  //console.log('auto-slider.js');

  $('#pt-main .control-prev').on('click', function() {
      //pt.gotoPage(5, 'prev');
      //console.log('prev');
      onHitNextBack();
      return false;
  });
  
  $('#pt-main .control-next').on('click', function() {
      //pt.gotoPage(6, 'next');
      //console.log('next');
      onHitNextBack();
      return false;
  });

  function onHitNextBack(){
  
    if( currentTimer != null ) {
      
      clearTimeout(currentTimer);
      
      currentTimer = null;
      
    }
    
    currentTimer = setTimeout(function(){
    
      //console.log("timer went off");
    
      $('#pt-main .control-next').click();
    
    }, 10000);
  
  }
  
  onHitNextBack();

});