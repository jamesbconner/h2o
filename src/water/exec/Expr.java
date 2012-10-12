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
  public static class Result {
    public final Key _key;
    private int _refCount;
    private boolean _copied;
    
    private Result(Key k, int refCount) {
      _key = k;
      _refCount = refCount;
    }
    
    public static Result temporary(Key k) {
      return new Result(k, 1);
    }
    
    public static Result temporary() {
      return new Result(Key.make(),1);
    }
    
    public static Result permanent(Key k) {
      return new Result(k, -1);
    }
    
    public void dispose() {
      --_refCount;
      if (_refCount == 0) 
        if (_copied)
          DKV.remove(_key); // remove only the array header
        else   
          UKV.remove(_key);
    }
    
    public boolean isTemporary() {
      return _refCount>=0; 
    }
    
    public boolean canShallowCopy() {
      return false; // shallow copy does not seem to be possible at the moment - arraylets are fixed to their key
      //return isTemporary() && (_copied == false);
    }
    
  }

  
  public abstract Result eval();
  
  
  public static void assign(final Key to, Result what) {
    if (what.canShallowCopy()) {
      assert (false); // we do not support shallow copy now (TODO)
      ValueArray v = (ValueArray) DKV.get(what._key);
      byte[] bits = v.get();
      ValueArray r = new ValueArray(to,MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
      DKV.put(to,r);
      what._copied = true; // TODO do we need to sync this? 
    } else {
      ValueArray v = (ValueArray) DKV.get(what._key);
      byte[] bits = v.get();
      ValueArray r = new ValueArray(to,MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
      DKV.put(to,r);
      MRTask copyTask = new MRTask() {
        @Override
        public void map(Key key) {
          byte[] bits = DKV.get(key).get();
          long offset = ValueArray.getOffset(key);
          Key k = ValueArray.make_chunkkey(to, offset);
          Value v = new Value(k,MemoryManager.arrayCopyOfRange(bits, 0, bits.length));
          DKV.put(k,v);
        }
        @Override public void reduce(DRemoteTask drt) {
          // pass
        }
      } ;
      copyTask.invoke(what._key);
    }
  }
  
}

class KeyLiteral extends Expr {
  private final Key key_;
  public KeyLiteral(String id) {
    key_ = Key.make(id);
  }
  
  @Override public Result eval() {
    return Result.permanent(key_);
  }
}


class AssignmentOperator extends Expr {

  private final Key _lhs;
  private final Expr _rhs;
  
  public AssignmentOperator(Key lhs, Expr rhs) {
    _lhs = lhs;
    _rhs = rhs;
  }
  
  @Override public Result eval() {
    Result rhs = _rhs.eval();
    Expr.assign(_lhs,rhs);
    rhs.dispose();
    return Result.permanent(_lhs);
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

  @Override public Result eval() {
    Result res = Result.temporary();
    // The 1 tiny arraylet
    Key key2 = ValueArray.make_chunkkey(res._key,0);
    byte[] bits = new byte[8];
    UDP.set8d(bits,0,_d);
    Value val = new Value(key2,bits);
    DKV.put(key2,val);

    // The metadata
    C._min = C._max = _d;
    ValueArray ary = ValueArray.make(res._key,Value.ICE,res._key,Double.toString(_d),1,8,CC);
    DKV.put(res._key,ary);
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
  
  @Override public Result eval() {
    Result res = Result.temporary();
    // get the keys and the values    
    Result kl = left_.eval();
    Result kr = right_.eval();
    ValueArray vl = (ValueArray) DKV.get(kl._key);
    ValueArray vr = (ValueArray) DKV.get(kr._key);
    // check that the data types are correct
    assert (vl.num_cols() == vr.num_cols());
    assert (vl.num_rows() == vr.num_rows());
    // TODO simplification, we only assume single columns
    assert (vl.num_cols() == 1);
    ValueArray result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vl.num_rows(),8,CC);
    DKV.put(res._key,result);
    MRVectorBinaryOperator op;
    switch (type_) {
      case ttOpAdd:
        op = new AddOperator(kl._key,kr._key,res._key,0,0);
        break;
      case ttOpSub:
        op = new SubOperator(kl._key,kr._key,res._key,0,0);
        break;
      case ttOpMul:
        op = new MulOperator(kl._key,kr._key,res._key,0,0);
        break;
      case ttOpDiv:
        op = new DivOperator(kl._key,kr._key,res._key,0,0);
        break;
      default:
        throw new RuntimeException("Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
    op.invoke(res._key);
    C._min = op.min_;
    C._max = op.max_;
    result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vl.num_rows(),8,CC);
    DKV.put(res._key,result); // reinsert with min / max
    kl.dispose();
    kr.dispose();
    return res;
  }
  
}


