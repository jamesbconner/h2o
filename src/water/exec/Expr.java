package water.exec;
import water.*;
import water.parser.ParseDataset;
import java.util.Arrays;

/**
 * Parse & eval expressions.  The namespace is the Keys in H2O.
 * Evaluation is like in R.
 *
 * @author cliffc@0xdata.com
 */
public abstract class Expr {

  // Evaluate, in the current context
  public abstract Key eval(Key k);

  // Parse some generic R string.  Builds an expression DAG.
  // Classic 1-char-lookahead recursive-descent parser.
  public static Expr parse( String x ) { return parse(new Stream(x.getBytes())); }
  public static Expr parse( Stream x ) {
    // Remove outer surrounding double-quotes
    if( x.peek1() == '"' ) {
      if( x._buf[x._buf.length-1] != '"' )
        throw new RuntimeException("unbalanced quotes, unable to parse "+new String(x._buf));
      x = new Stream(Arrays.copyOfRange(x._buf,1,x._buf.length-1));
    }

    char c = (char)x.peek1();
    if( Character.isDigit(c) ) {
      return parse_num(x);
    } else {
      throw new RuntimeException("unable to parse at "+new String(x._buf,x._off,x._buf.length-x._off));
    }
  }

  public static NumExpr parse_num( Stream x ) {
    int off = x._off;           // Number start
    // This next stanza just tries to figure out the end of the number.
    // Probably should be some horrible regex, or a version of NumberFormat for
    // parsing.
    int i=0;
    boolean dot = false;
    boolean e = false;
    while( true && x._off < x._buf.length ) {
      char c = (char)x.get1();
      if( Character.isDigit(c) ) continue;
      if( c=='.' ) {
        if( dot ) break;
        dot = true;
        continue;
      }
      if( c=='E' || c=='e' ) {
        if( e ) break;
        e = dot = true;
        if( x.peek1()=='+' || x.peek1()=='-' ) x.get1();
        continue;
      }
    }

    String s = new String(x._buf,off,x._off);
    double d= Double.parseDouble(s);
    return new NumExpr(d);
  }
}

// -----------------------------------------------
class NumExpr extends Expr {
  public static final ValueArray.Column C = new ValueArray.Column();
  public static final ValueArray.Column[] CC = new ValueArray.Column[]{C};
  static {
    C._name = "";
    C._scale = 1;
    C._size = -8;
    C._domain = new ParseDataset.ColumnDomain();
    C._domain.kill();
  }
  public final double _d;
  public NumExpr( double d ) { _d=d; }

  public Key eval( Key res ) {
    // The 1 tiny arraylet
    Key key2 = ValueArray.make_chunkkey(res,0);
    byte[] bits = new byte[8];
    UDP.set8d(bits,0,_d);
    Value val = new Value(key2,bits);
    DKV.put(key2,val);

    // The metadata
    C._min = C._max = _d;
    ValueArray ary = ValueArray.make(res,Value.ICE,res,Double.toString(_d),1,8,CC);
    DKV.put(res,ary);
    return res;
  }
}

//class PlusExpr extends Expr {
//  public PlusExpr( Expr l, Expr r ) {
//    k1 = l.eval();
//    k2 = r.eval();
//    assert compatible;
//    MRTask() {
//      map() {
//        for( int i=0; i<rows; i++ )
//
//      }
//    }
//  }
//}
