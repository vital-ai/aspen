package com.lgc.wsh.util;

/** Wrap an existing Monitor with a partial range.
 *  Note that only makes sense to call initReport()
 *  with the first wrapper used.
 *  */
public class PartialMonitor implements Monitor {
  private Monitor _wrapped =null;
  private double _begin = 0.;
  private double _end = 1.;
  /** An existing Monitor will be wrapped for progress in a limited range.
      @param wrapped The wrapped monitor.
      @param begin The first value to be updated to the wrapped monitor,
      corresponding to a 0 reported to this monitor.
      @param end The last value to be updated to the wrapped monitor
      corresponding to a 1 reported to this monitor.
  */
  public PartialMonitor(Monitor wrapped, double begin, double end) {
    _wrapped = wrapped;
    _begin = begin;
    _end = end;
  }

  public void report(double fraction) {
    if (_wrapped == null)
      return;
    _wrapped.report(fraction*(_end - _begin) + _begin);
  }

  public void initReport(double initFraction) {
    _wrapped.initReport(initFraction*(_end - _begin) + _begin);
  }
}

