package water.exec;
import java.io.IOException;
import water.*;
import water.parser.ParseDataset;
import java.util.Arrays;
import water.exec.RLikeParser.Token;


// -----------------------------------------------


/**
 * Eval expressions.  The namespace is the Keys in H2O.
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
  
}

class KeyLiteral extends Expr {
  private final Key key_;
  public KeyLiteral(String id) {
    key_ = Key.make(id);
  }
  
  public Key eval() {
    return key_;
  }
  
  public Key eval(Key res) {
    throw new RuntimeException("Key literal cannot be evaluated to a different key");
  }
}


class FloatLiteral extends Expr {
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
  public FloatLiteral( double d ) { _d=d; }

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

class BinaryOperator extends Expr {

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
  private final Token.Type type_;
  
  
  public BinaryOperator(Token.Type type, Expr left, Expr right) {
    left_ = left;
    right_ = right;
    type_ = type;
    
  }
  
  @Override public Key eval(Key res) {
    // get the keys and the values    
    Key kl = left_.eval();
    Key kr = right_.eval();
    ValueArray vl = (ValueArray) DKV.get(kl);
    ValueArray vr = (ValueArray) DKV.get(kr);
    // check that the data types are correct
    assert (vl.num_cols() == vr.num_cols());
    assert (vl.num_rows() == vr.num_rows());
    // TODO simplification, we only assume single columns
    assert (vl.num_cols() == 1);
    ValueArray result = ValueArray.make(res,Value.ICE,res,"temp result",vl.num_rows(),8,CC);
    DKV.put(res,result);
    MRVectorBinaryOperator op;
    switch (type_) {
      case ttOpAdd:
        op = new AddOperator(kl,kr,res,0,0);
        break;
      case ttOpSub:
        op = new SubOperator(kl,kr,res,0,0);
        break;
      case ttOpMul:
        op = new MulOperator(kl,kr,res,0,0);
        break;
      case ttOpDiv:
        op = new DivOperator(kl,kr,res,0,0);
        break;
      default:
        throw new RuntimeException("Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
    op.invoke(res);
    C._min = op.min_;
    C._max = op.max_;
    result = ValueArray.make(res,Value.ICE,res,"temp result",vl.num_rows(),8,CC);
    DKV.put(res,result); // reinsert with min / max
    return res;
  }
  
}


