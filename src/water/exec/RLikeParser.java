/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.exec;

import java.util.Arrays;
import water.Stream;

/**
 * 
 * 
 * 
 * 
 * 
 * Grammar:
 * 
 * S  -> T S1
 * S1 -> e
 *       + T S1
 *       - T S1
 * T  -> number 
 *
 * @author peta
 */
public class RLikeParser {
/*
  public static Expr parse( String x ) { return parse(new Stream(x.getBytes())); }
  public static Expr parse( Stream x ) {
    // Remove outer surrounding double-quotes
    if( x.peek1() == '"' ) {
      if( x._buf[x._buf.length-1] != '"' )
        throw new RuntimeException("unbalanced quotes, unable to parse "+new String(x._buf));
      x = new Stream(Arrays.copyOfRange(x._buf,1,x._buf.length-1));
    }
    
    // now parse - we start with S
    
    

    
    
    
    char c = (char)x.peek1();
    if( Character.isDigit(c) ) {
      return parse_num(x);
    } else {
      throw new RuntimeException("unable to parse at "+new String(x._buf,x._off,x._buf.length-x._off));
    }
  }
  
  static Expr parse_S(Stream x) {
    Expr e = parse_T(x);
    if (e==null) {
      // TODO make sure we are at the end of stream
      
    }
    
    
    
    
  }
  
  static Expr parse_T(Stream x) {
    char c = (char)x.peek1();
    if (Character.isDigit(c))
      return parse_number(x);
    else
      throw new RuntimeException("unable to parse");
  }
  
  static Expr parse_number(Stream x) {
    return null;
  }
  
  
*/  
}
