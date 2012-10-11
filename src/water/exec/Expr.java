package water.exec;
import java.io.IOException;
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

  /** Evaulates the expression and returns its result in a new temporary key.
   * 
   * TODO the plan is to change this to produce proper random and temporary keys
   * in a centralized and well documented manner. But for now, I just do the
   * stupid simple thing.
   * 
   * Also this whole API might change with more complex expressions. 
   * 
   * @return 
   */
  public Key eval() {
    return eval(Key.make());
  }
  
  
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
      // this is so stupid I am ashamed of myself, but for the time being...
      Expr e = parse_num(x);
      if (!x.eof()) {
        c = (char) x.peek1();
        if (c == '+') {
          x.get1();
          Expr e2 = parse_num(x);
          e = new OperatorPlus(e, e2);
        }
      }
      return e;
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
      x._off--; // rollback
      break;
    }
//    System.out.println(new String(x._buf));
//    System.out.println(off);
//    System.out.println(x._off);
    
    String s = new String(x._buf,off,x._off-off);
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

class OperatorPlus extends Expr {

  public static final ValueArray.Column C = new ValueArray.Column();
  public static final ValueArray.Column[] CC = new ValueArray.Column[]{C};
  static {
    C._name = "";
    C._scale = 1;
    C._size = -8;
    C._domain = new ParseDataset.ColumnDomain();
    C._domain.kill();
  }
  
  
  
  private final Expr left_;
  private final Expr right_;
  
  
  public OperatorPlus(Expr left, Expr right) {
    left_ = left;
    right_ = right;
    
  }
  
  @Override public Key eval(Key res) {
    // evaluate the left and right guys to their 
    Key kl = left_.eval();
    Key kr = right_.eval();
    // get the values
    Value v = DKV.get(kl);
    assert (v instanceof ValueArray);
    ValueArray vl = (ValueArray)v;
    v = DKV.get(kr);
    assert (v instanceof ValueArray);
    ValueArray vr = (ValueArray)v;
    // now check that the arrays are compatible. 
    // for us it means the number of columns and their sizes are the same, because
    // we do not yet support a column selector
    assert (vl.num_cols() == vr.num_cols());
    assert (vl.num_rows() == vr.num_rows());
    // now we are happy, it seems we can add the two vectors together
    
    // shortcut let me just being stupid
    assert (vl.num_cols() == 1);
    assert (vl.num_rows() == 1);
    
    double l = vl.datad(0,0);
    double r = vr.datad(0,0);
    double result = l + r;
    
    Key key2 = ValueArray.make_chunkkey(res,0);
    byte[] bits = new byte[8];
    UDP.set8d(bits,0,result);
    Value val = new Value(key2,bits);
    DKV.put(key2,val);

    // The metadata
    C._min = C._max = result;
    ValueArray ary = ValueArray.make(res,Value.ICE,res,Double.toString(result),1,8,CC);
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
