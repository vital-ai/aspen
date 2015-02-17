package com.lgc.wsh.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Utilities for manipulating strings.
 */
public class StringUtil {
  private static final Logger LOG
    = Logger.getLogger(StringUtil.class.getName() ,StringUtil.class.getName());

  /**
   * Line separator
   */
  public static final String NL = System.getProperty("line.separator");

  /** The format used by getTimeStamp() methods,
      like 20050621-153318 */
  public static final DateFormat TIMESTAMP_FORMAT =
    new SimpleDateFormat("yyyyMMdd-HHmmss");

  /** The format used by getLongTimeStamp() methods,
      like 20050621-153318-255 */
  public static final DateFormat TIMESTAMP_LONG_FORMAT =
    new SimpleDateFormat("yyyyMMdd-HHmmss-SSS");

  /** Lock the previous Date. */
  private static final Object s_uniqueDateLock = new Object();

  /** Most recent unique Date. */
  private static long s_uniqueTime = 0;

  /** Pattern for cleaning numeric strings. Changes "1.00" to "1". */
  private static Pattern s_removeDecimalPointAndZeros =
      Pattern.compile("^\\s*([+-]?\\d*)\\.0*([eE][+-]?\\d+)?\\s*$");

  /** Pattern for cleaning numeric strings. Changes "1.10" to "1.1". */
  private static Pattern s_removeTrailingZeros =
      Pattern.compile("^\\s*([+-]?\\d*\\.\\d+?)0*([eE][+-]?\\d+)?\\s*$");

  /**
   * A helper class for creating a nicely formatted table to print as a string.
   */
  public static class TableBuilder {
    /** The list of rows used to make the table. */
    private ArrayList<String[]> _rows = new ArrayList<String[]>();

    /** The number of items in each row. */
    private int _nColumns = 0;

    /**
     * Constructs an empty table builder.
     */
    public TableBuilder() {
    }

    /**
     * Constructs a table builder with the specified items in the first row.
     * @param items Objects to put in the first row of the table.
     */
    public TableBuilder(Object... items) {
      this ();
      addRow(items);
    }

    /**
     * Adds a row of items to the table.  Floats and doubles are converted to
     * strings by the nsf method.  All other entities are
     * converted to strings by String.valueOf(item).
     * @param items Objects to add to the table.
     * @throws IllegalArgumentException If an attempt is made to add a row with
     * no items, or if the row has a different number of items than the rows
     * already in the table.
     * @see #nsf(double)
     */
    public void addRow(Object... items) {
      if (_nColumns==0)
        _nColumns = items.length;

      if (items.length==0)
        throw new IllegalArgumentException("Added row with no items.");
      if (items.length!=_nColumns)
        throw new IllegalArgumentException("Added row with "+items.length+
            " items to table with "+_nColumns+" columns.");

      String[] row = new String[items.length];
      for (int i=0; i<items.length; ++i) {
        if (items[i] instanceof Double || items[i] instanceof Float)
          row[i] = nsf(((Number)items[i]).doubleValue());
        else
          row[i] = String.valueOf(items[i]);
      }
      _rows.add(row);
    }

    /**
     * Returns the number of rows in this table.
     * @return the number of rows in this table.
     */
    public int size() {
      return _rows.size();
    }

    /**
     * Removes the row at the specified position from the table.
     * @param index the index of the row to remove.
     * @throws IndexOutOfBoundsException if index out of range
     *         (<code>index < 0 || index >= size()</code>).
     */
    public void removeRow(int index) {
      _rows.remove(index);
    }

    /**
     * Condenses the table to specified maximum number of lines.
     * If the table contains more lines than the maximum, lines
     * will be removed at regular intervals.
     * @param max maximum number of lines in the table
     */
    public void condense(int max) {
      if (max<0)
        throw new IllegalArgumentException("max size is "+max+"  Must be >0");

      int size = size();
      if (size<=max)
        return;

      int stride = (size+max-1)/max;
      assert stride>1 : "stride="+stride+"  size="+size+"  max="+max;
      for (int index=size-1; index>=0; index-=stride) {
        for (int i=0; i<stride-1 && size()>max; ++i) {
          removeRow(index-i);
        }
      }

      assert size()==max :
        "size="+size+"  max="+max+"  new size="+size()+"  stride="+stride+
        "\n"+toString();
       return;
    }

    /**
     * Return a nicely formatted table of values.
     * The table is returned as a string formatted by the joinTable method.
     * @return A formatted string.
     * @see #joinTable(String[][])
     */
    @Override
    public String toString() {
      return StringUtil.joinTable(_rows.toArray(new String[0][]));
    }
  }

  /** Join together Strings into a single String
      @param strings The Strings to join together.
      Gets the String from Object.toString().
      @param join Place this string between the joined strings,
      but not at the beginning or end.
      @return The joined String
  */
  public static String join(Object[] strings, String join) {
    if (strings == null) return null;
    StringBuilder result = new StringBuilder();
    for (int i=0; i<strings.length; ++i) {
      result.append(strings[i].toString());
      if (i<strings.length-1) result.append(join);
    }
    return result.toString();
  }

  /** Join together Strings into a single String.
      Split again with String.split(String regex).
      @param strings The Strings to join together.
      Gets the String from Object.toString().
      @param join Place this string between the joined strings,
      but not at the beginning or end.
      @return The joined String
  */
  public static String join(List strings, String join) {
    return join(strings.toArray(), join);
  }

  /**
   * Combine a 2D array of strings into a single aligned table.
   * Fast dimension will be vertical.
   * @param table Elements to be in table.
   * @return table as a printable string.
   */
  public static String joinTable(String[][] table) {
    table = alignTable(table);
    StringBuilder sb = new StringBuilder();
    for (int ir=0; ir < table.length; ++ir) {
      for (int ic=0; ic < table[ir].length; ++ic) {
        sb.append(table[ir][ic]);
      }
      if (ir!=table.length-1) sb.append(NL);
    }
    return sb.toString();
  }

  /** Return the stack trace of an Throwable as a String
      @param throwable The Throwable containing the stack trace.
      @return The stack trace that would go to stderr with
      throwable.printStackTrace().
  */
  public static String getStackTrace(Throwable throwable) {
    StackTraceElement[] stackTraceElements = throwable.getStackTrace();
    StringBuilder result = new StringBuilder();
    for (StackTraceElement se: stackTraceElements) {
      result.append("  ");
      result.append(se.toString());
      result.append("\n");
    }
    return result.toString();
  }

  /** Prepend a string to every line of text in a String
      @param prepend String to be prepended
      @param lines Lines separated by newline character, from
      System.getProperty("line.separator");
      @return Modified lines
  */
  public static String prependToLines(String prepend, String lines) {
    if (lines == null) return null;
    if (prepend == null) return lines;
    StringBuilder result = new StringBuilder();
    boolean hasFinalNL = lines.endsWith(NL);
    StringTokenizer divided = new StringTokenizer(lines, NL);
    while (divided.hasMoreTokens()) {
      result.append(prepend + divided.nextToken());
      if (divided.hasMoreTokens() || hasFinalNL) result.append(NL);
    }
    return result.toString();
  }

  /** Return a concise string that can be added as a timestamp to
      filenames
      @return String in format TIMESTAMP_FORMAT.
  */
  public static String getTimeStamp() {
    return getTimeStamp(new Date());
  }

  /** Return a concise string that can be added as a timestamp to
      filenames
      @param date Time in milliseconds since 1970
      @return String in format TIMESTAMP_FORMAT.
  */
  public static String getTimeStamp(long date) {
    return getTimeStamp(new Date(date));
  }

  /** Return a concise string that can be added as a timestamp to
      filenames
      @param date Date for timestamp
      @return String in format TIMESTAMP_FORMAT.
  */
  public static String getTimeStamp(Date date) {
    synchronized (TIMESTAMP_FORMAT) { // format is not thread-safe
      return TIMESTAMP_FORMAT.format(date);
    }
  }

  /** Convert a time stamp from getTimeStamp into the equivalent Date object.
      @param timeStamp A date created in the concise format TIMESTAMP_FORMAT
      returned by getTimeStamp().
      @return The Date equivalent to the time stamp.
      @throws ParseException When the string is in the wrong format.
   */
  public static Date parseTimeStamp(String timeStamp) throws ParseException {
    synchronized (TIMESTAMP_FORMAT) { // format is not thread-safe
      return TIMESTAMP_FORMAT.parse(timeStamp);
    }
  }

  /** Return a concise string that can be added as a timestamp to
      filenames.  Includes milliseconds.
      Guaranteed to return a unique value each time it is called.
      @return String in format TIMESTAMP_LONG_FORMAT.
  */
  public static String getLongTimeStamp() {
      return getLongTimeStamp(getUniqueDate());
  }

  /** Return a concise string that can be added as a timestamp to
      filenames.  Includes milliseconds.
      @param date Time in milliseconds since 1970
      @return String in format TIMESTAMP_FORMAT.
  */
  public static String getLongTimeStamp(long date) {
    return getLongTimeStamp(new Date(date));
  }

  /** Return a concise string that can be added as a timestamp to
      filenames.  Includes milliseconds.
      @param date Create a timestamp for this date.
      @return String in format TIMESTAMP_LONG_FORMAT.
  */
  public static String getLongTimeStamp(Date date) {
    synchronized (TIMESTAMP_LONG_FORMAT) { // format is not thread-safe
      return TIMESTAMP_LONG_FORMAT.format(date);
    }
  }

  /** Convert a time stamp from getTimeStamp into the equivalent Date object.
      Time stamp includes milliseconds.
      @param timeStamp A date created in the concise format TIMESTAMP_FORMAT
      returned by getTimeStamp().
      @return The Date equivalent to the time stamp.
      @throws ParseException When the string is in the wrong format.
   */
  public static Date parseLongTimeStamp(String timeStamp)
    throws ParseException {
    synchronized (TIMESTAMP_LONG_FORMAT) {
      try {
        return TIMESTAMP_LONG_FORMAT.parse(timeStamp);
      } catch (RuntimeException e) {
        LOG.severe("Could not parse "+timeStamp);
        throw e;
      }
    }
  }

  /** Read build time stamp as a Date
      from resource file created at time of build.
      @return Build time stamp.
 * @throws IOException file system error
  */
  public static Date getBuildDate() throws IOException {
    synchronized (TIMESTAMP_FORMAT) {
      try {
        return TIMESTAMP_FORMAT.parse(getBuildTimeStamp());
      } catch (ParseException e) {
        IOException ioe = new IOException(e.getMessage());
        ioe.initCause(e);
        throw ioe;
      }
    }
  }

  /** Read build time stamp from resource file created at time of build.
      @return Build time stamp or null if not available.
      @throws IOException if can't get timestamp file.
  */
  public static String getBuildTimeStamp() throws IOException {
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(
        (StringUtil.class.getResourceAsStream("BUILD_VERSION"))));
      String result = br.readLine();
      br.close();
      return result;
    } catch (Exception e) {
      throw new IOException("This build is incomplete.");
    }
  }

  /**
   * Replaces all occurences in string s of string x with string y.
   * Stole from com.lgc.gpr.util.XmlUtil.replaceAll.
   *
   * @param x string to be replaced.
   * @param y string replacing each occurence of x.
   * @param s string containing zero or more x to be replaced.
   * @return string s with all x replaced with y.
   */
  public static String replaceAll(String x, String y, String s) {
    if (s==null) return null;
    int from = 0;
    int to = s.indexOf(x,from);
    if (to<0) return s;
    StringBuilder d = new StringBuilder(s.length()+32);
    while (to>=0) {
      d.append(s.substring(from,to));
      d.append(y);
      from = to+x.length();
      to = s.indexOf(x,from);
    }
    return d.append(s.substring(from)).toString();
  }

  /** Break lines at convenient locations and indent
      if lines are too long.  Try to break at word boundaries,
      but resort to breaking in the middle of a word with a backslash
      if necessary.
      @param s String that needs to be broken.
      @param maxchars The maximum number of characters to use per line.
      @return String with newlines and indentations included as necessary.
      The result will always have a final newline
   */
  public static String breakLines(String s, int maxchars) {
    StringBuilder sb = new StringBuilder();
    StringTokenizer st = new StringTokenizer(s,NL);
    while (st.hasMoreTokens()) {
      sb.append(breakLine(st.nextToken(),maxchars));
    }
    return sb.toString();
  }

  /**
   * Remove the whitespace from the string
 * @param str clean this string
   * @return the string without any whitespace; null, if the string is null
   */
  public static String removeWhiteSpaces(String str) {
    if (str == null) return null;

    StringBuilder sb = new StringBuilder();
    CharacterIterator iter = new StringCharacterIterator(str);
    for (char c = iter.first(); c != CharacterIterator.DONE; c = iter.next()) {
      if (!Character.isWhitespace(c))
        sb.append(c);
    }

    return sb.toString();
  }

  /** Convert a number of seconds into words
      @param seconds Number of seconds
      @return Localized words describing the number of seconds.
  */
  public static String timeWords(long seconds) {
    if (seconds == 0) {
      return Localize.filter("0 ${seconds}", StringUtil.class);
    }
    String result = "";
    long minutes = seconds/60;
    long hours = minutes/60;
    long days = hours/24;
    seconds %= 60;
    minutes %= 60;
    hours %= 24;
    if (days >= 10) {
      if (hours >=12) ++days;
      hours = minutes = seconds = 0;
    } else if (hours >= 10 || days > 0) {
      if (minutes >=30) {
        ++hours;
        days += hours/24;
        hours %= 24;
      }
      minutes = seconds = 0;
    } else if (minutes >= 10 || hours > 0) {
      if (seconds >=30) {
        ++minutes;
        hours += minutes/60;
        minutes %= 60;
      }
      seconds = 0;
    }
    if (seconds != 0)
      result = " " + seconds + " ${second"+ ((seconds>1)?"s}":"}") + result;
    if (minutes != 0)
      result = " " + minutes + " ${minute"+ ((minutes>1)?"s}":"}") + result;
    if (hours != 0)
      result = " " + hours + " ${hour" + ((hours>1)?"s}":"}") + result;
    if (days != 0)
      result = " " + days + " ${day" + ((days>1)?"s}":"}") + result;

    return Localize.filter(result.trim(), StringUtil.class);
  }

  /** Make a string representing the array,
      for debugging and error messages
      @param v Array to print.
      @return String representing the vector,
  */
  public static String toString(int[] v) {
    StringBuilder sb = new StringBuilder("(");
    for (int i=0; i<v.length; ++i) {
      sb.append(Integer.toString(v[i]));
      if (i<v.length-1) {sb.append(",");}
    }
    sb.append(")");
    return sb.toString();
  }

  /** Make a string representing the array,
      for debugging and error messages
      @param v Array to print.
      @return String representing the vector,
  */
  public static String toString(float[] v) {
    StringBuilder sb = new StringBuilder("(");
    for (int i=0; i<v.length; ++i) {
      sb.append(Float.toString(v[i]));
      if (i<v.length-1) {sb.append(",");}
    }
    sb.append(")");
    return sb.toString();
  }

  /** Make a string representing the array,
      for debugging and error messages
      @param v Array to print.
      @return String representing the vector,
  */
  public static String toString(int[][] v) {
    StringBuilder sb = new StringBuilder();
    for (int[] col: v) {
      sb.append(toString(col));
    }
    return sb.toString();
  }

  /** Make a string representing the array,
      for debugging and error messages
      @param v Array to print.
      @return String representing the vector,
  */
  public static String toString(float[][] v) {
    StringBuilder sb = new StringBuilder();
    for (float[] col: v) {
      sb.append(toString(col));
    }
    return sb.toString();
  }

  /** Make a string representing the array,
      for debugging and error messages
      @param v Array to print.
      @return String representing the vector,
  */
  public static String toString(double[] v) {
    StringBuilder sb = new StringBuilder("(");
    for (int i=0; i<v.length; ++i) {
      sb.append(Double.toString(v[i]));
      if (i<v.length-1) {sb.append(",");}
    }
    sb.append(")");
    return sb.toString();
  }

  /** Convert a double to a string appropriate for writing
      to a file.  Larger numbers are treated as integers.
      This name is short and cryptic because it appears so often
      on a single line.  (Stands for Number String.)
      @param d Convert this double to a String.
      @return String suitable for a human to read, with acceptable precision.
  */
  public static String ns(double d) {
    if ( d == (int) d ) {
      return Integer.toString((int)d);
    } else if (d > 10000 || d < -10000) {
      return Integer.toString((int) Math.floor(d+0.5));
    } else if (d > -1.e-15 && d < 1.e-15) {
      return "0";
    } else {
      return Float.toString((float)d);
    }
  }

  /**
   * Converts a double to a string.  The number is initally formatted
   * with String.format("%g",d) which gives 6 significant digits of
   * precision.  Then the string is cleaned up by removing trailing zeros to
   * the right of the decimal point, and by suppressing the decimal point
   * if it is followed only by zeros.  Finally, any algebraic sign preceeding
   * zero is discarded.
   * This name is short and cryptic because it appears so often on
   * a single line (think "Number String Format").
   *
   * @param d Convert this double to a string.
   * @return String suitable for a human to read, with acceptable
   *         precision.
   */
  public static String nsf(double d) {
    String s = String.format("%g",d);
    s = removeTrailingZeros(s);
    s = removeDecimalPointAndZeros(s);
    s = removeSignFromZero(s);
    return s;
  }

  /** Get a date for a timestamp, and avoid any previous
      dates by adding a millisecond if necessary.
      @return unique Date
   */
  public static Date getUniqueDate() {
    synchronized (s_uniqueDateLock) {
      long time = System.currentTimeMillis();
      if (time <= s_uniqueTime) {
        time = s_uniqueTime+1;
      }
      s_uniqueTime = time;
      return new Date(s_uniqueTime);
    }
  }

  ///////////////////////   PRIVATE   //////////////////////////

  // pad strings in table to make a pretty print, right justify
  private static String[][] alignTable(String[][] table) {
    table = table.clone();

    int nrows = table.length;
    if (nrows == 0) return table;

    int ncols = table[0].length;
    if (ncols == 0) return table;

    for (int ir=0; ir<nrows; ++ir) {
      table[ir] = table[ir].clone();
    }

    for (int ic=0; ic<ncols; ++ic) {
      int length = 0;
      for (int ir=0; ir<nrows; ++ir) {
        if (table[ir][ic] == null) table[ir][ic] = "";
        length = Math.max(length, table[ir][ic].length());
      }
      length += 2; // pad between columns
      for (int ir=0; ir<nrows; ++ir) {
        while (table[ir][ic].length() < length) {
          table[ir][ic] = " " + table[ir][ic];
        }
      }
    }
    return table;
  }

  /** Break a line at convenient locations and indent
      if lines are too long.  Try to break at word boundaries,
      but resort to breaking in the middle of a word with a backslash
      if necessary.
      @param s String that needs to be broken.  Do not pass a string
      that already contains newlines.
      @param maxchars The maximum number of characters to use per line.
      @return String with newlines and indentations included as necessary.
      The result will always have a final newline
   */
  private static String breakLine(String s, int maxchars) {
    StringBuilder sb = new StringBuilder();
    printLine(s, maxchars, sb);
    return sb.toString();
  }

  private static void printLine(String s, int maxline, StringBuilder sb) {
    int maxbreak = maxline/3, maxindent = 2*maxline/3;
    if (s.length() < maxline) {sb.append(s+NL); return;}
    int n = maxline-1;
    try {
      while (n>=maxbreak && s.charAt(n) !=' ') {--n;}
    } catch (java.lang.StringIndexOutOfBoundsException e) {
      e.printStackTrace();
      throw new IllegalStateException
        ("s=|"+s+"|"+NL+"s.length()="+s.length()+NL+
         "n="+n+NL+"maxline="+maxline+NL+"maxbreak="+maxbreak+NL+
         e.getMessage());
    }
    String extra = "";
    String indent ="";
    // See if we stopped after all spaces
    boolean allAreSpaces = true;
    for (int i=0; allAreSpaces && i<n; ++i) {
      allAreSpaces = allAreSpaces && s.charAt(i) == ' ';}
    if (allAreSpaces ||
       s.charAt(n) !=' ') {// If must break a word, do so at the end
      n = maxline-2; extra = "\\";} // break at the end of the line
    else {indent = indentString(s, maxindent);} // break at word with same indentation
    printLine(s.substring(0,n)+extra, maxline, sb); // print before break
    printLine(indent + s.substring(n), maxline, sb); // print after break
  }

  private static String indentString(String s, int maxindent) {
    int n=0; while (n<maxindent && n<s.length() && s.charAt(n)==' ') {++n;}
    if (n<1) n = 1;
    return s.substring(0,n-1);
  }

  private static String removeTrailingZeros(String s) {
    return s_removeTrailingZeros.matcher(s).replaceFirst("$1$2");
  }

  private static String removeDecimalPointAndZeros(String s) {
    return s_removeDecimalPointAndZeros.matcher(s).replaceFirst("$1$2");
  }

  private static String removeSignFromZero(String s) {
    if ("-0".equals(s)) return "0";
    if ("+0".equals(s)) return "0";
    return s;
  }

  /**
   * Test code.
   * @param args command line
   * @throws Exception test failures
   */
  public static void main(String[] args) throws Exception {
    String lines = prependToLines("a","bbb"+NL+"ccc");
    assert (lines.equals("abbb"+NL+"accc"));
    {
      long seconds =(29L + 60*(9));
      String words = timeWords(seconds);
      assert (words.equals("9 minutes 29 seconds")) : words;
    }
    {
      long seconds =(29L + 60*(10));
      String words = timeWords(seconds);
      assert (words.equals("10 minutes")) : words;
    }
    {
      long seconds =(30L + 60*(10));
      String words = timeWords(seconds);
      assert (words.equals("11 minutes")) : words;
    }
    {
      long seconds =(29L + 60*(29 + 60*(9)));
      String words = timeWords(seconds);
      assert (words.equals("9 hours 29 minutes")) : words;
    }
    {
      long seconds =(30L + 60*(30 + 60*(9)));
      String words = timeWords(seconds);
      assert (words.equals("9 hours 31 minutes")) : words;
    }
    {
      long seconds =(30L + 60*(30 + 60*(10)));
      String words = timeWords(seconds);
      assert (words.equals("11 hours")) : words;
    }
    {
      long seconds =(30L + 60*(30 + 60*(11 +24*9)));
      String words = timeWords(seconds);
      assert (words.equals("9 days 12 hours")) : words;
    }
    {
      long seconds =(30L + 60*(30 + 60*(11 +24*10)));
      String words = timeWords(seconds);
      assert (words.equals("10 days")) : words;
    }
    {
      long seconds =(0L + 60*(0 + 60*(12 +24*10)));
      String words = timeWords(seconds);
      assert (words.equals("11 days")) : words;
    }
    {
      // 2 hours.
      long seconds = 3600L * 2;
      String words = timeWords(seconds);
      assert (words.equals("2 hours")) : words;
    }
    {
      // 1 second less than 2 hours.
      long seconds = 3600L * 2 - 1;
      String words = timeWords(seconds);
      assert (words.equals("2 hours")) : words;
    }
    {
      // 2 days.
      long seconds = 3600L * 24 * 2;
      String words = timeWords(seconds);
      assert (words.equals("2 days")) : words;
    }
    {
      // 1 second less than 2 days.
      long seconds = 3600L * 24 * 2 - 1;
      String words = timeWords(seconds);
      assert (words.equals("2 days")) : words;
    }

    {
      String stamp = getLongTimeStamp();
      Date date = parseLongTimeStamp(stamp);
      assert getLongTimeStamp(date).equals(getLongTimeStamp(date.getTime()));
      assert date.equals(parseLongTimeStamp(getLongTimeStamp(date)));
    }
    {
      String stamp = getTimeStamp();
      Date date = parseTimeStamp(stamp);
      assert getTimeStamp(date).equals(getTimeStamp(date.getTime()));
      assert date.equals(parseTimeStamp(getTimeStamp(date)));
    }

    {
      TableBuilder tb = new TableBuilder();
      tb.addRow(1,2,3,4.0,5.0,6.0f);
      tb.addRow("a","b","ccCC",1L,1.0F/3.0F,1.0/3.0);
      tb.addRow("a",new java.io.File("/tmp"),"barf",1L,1.0F/3.0F,1.0/3.0);
      tb.addRow(1.0, 0.1, 0.01, 0.001, 0.0001, 0.00001);
      tb.addRow(1.0, 10.0, 100.0, 1000.0, 10000.0, 1000000.0);
      tb.addRow(1e7, 1.1e7, 1.01e7, 1.0101e7, 1.01001e7, 1.0000001e7);
      tb.addRow(1000, 1000.0, 100000, 100000.0, 1000000, 1000000.0);
      tb.addRow(2000.0,100000000,12345.6789,1L,1.0F/3.0F,1.0/3.0);
      tb.addRow(Float.NaN,Float.NEGATIVE_INFINITY,Float.POSITIVE_INFINITY,
          Double.NaN,Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY);
      tb.addRow(Float.MAX_VALUE,Float.MIN_VALUE,1.0f,
          Double.MAX_VALUE,Double.MIN_VALUE,1.0);
      tb.addRow(Integer.MAX_VALUE,1L+Integer.MAX_VALUE,Boolean.TRUE,4,5,6);
      Float nf = null;
      Double nd = null;
      tb.addRow(null,(String)null,null,null,nf,nd);

      String expectedResult =
"            1           2         3             4            5         6"+NL+
"            a           b      ccCC             1     0.333333  0.333333"+NL+
"            a        /tmp      barf             1     0.333333  0.333333"+NL+
"            1         0.1      0.01         0.001       0.0001     1e-05"+NL+
"            1          10       100          1000        10000     1e+06"+NL+
"        1e+07     1.1e+07  1.01e+07    1.0101e+07  1.01001e+07     1e+07"+NL+
"         1000        1000    100000        100000      1000000     1e+06"+NL+
"         2000   100000000   12345.7             1     0.333333  0.333333"+NL+
"          NaN   -Infinity  Infinity           NaN    -Infinity  Infinity"+NL+
"  3.40282e+38  1.4013e-45         1  1.79769e+308     4.9e-324         1"+NL+
"   2147483647  2147483648      true             4            5         6"+NL+
"         null        null      null          null         null      null";
      assert expectedResult.equals(tb.toString());
    }

    {
      // Test the TableBuilder.condense method.
      for (int n=1; n<=500; ++n) {
        for (int nmax=1; nmax<n && nmax<101; ++nmax) {
          TableBuilder tb = new TableBuilder();
          for (int i=0; i<n; ++i) {
            tb.addRow("Row "+i);
          }
          tb.condense(nmax);
          assert tb.size()==nmax : "n="+n+"  nmax="+nmax+"  Size is "+tb.size();
        }
      }
    }
  }
}
