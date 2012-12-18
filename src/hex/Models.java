//package hex;
//
//import hex.HexDataFrame.Sampling;
//import init.H2OSerializable;
//
//import java.util.Arrays;
//
//import water.*;
//
//public abstract class Models {
//
//  public interface ModelBuilder {
//    public HexModel trainOn(ValueArray data, int[] colIds, Sampling s);
//  }
//
//  public static abstract class ModelValidation implements Cloneable, H2OSerializable {
//    public abstract void add(double yr, double ym);
//    public abstract void add(ModelValidation other);
//    public abstract double err();
//    public abstract long n();
//
//    /**
//     * Combines result during cross validation.
//     *
//     * It should update mean and variance of error and any other value of
//     * interest used in this validation
//     *
//     * @param other
//     */
//    public abstract void aggregate(ModelValidation other);
//
//    public abstract ModelValidation clone();
//  }
//
//  public interface ClassifierValidation {
//    public abstract int classes();
//    public abstract long cm(int i, int j);
//  }
//
//  public interface BinaryClassifierValidation extends ClassifierValidation {
//    public abstract double fp();
//    public abstract double fpVar();
//    public abstract double fn();
//    public abstract double fnVar();
//    public abstract double tp();
//    public abstract double tpVar();
//    public abstract double tn();
//    public abstract double tnVar();
//  }
//
//  public static class ModelTask extends RowVecTask {
//    boolean                   _validate     = true;
//    boolean                   _reduce;
//    boolean                   _storeResults;
//    int                       _rpc;
//    double                    _ymu;
//    Key                       _resultChunk0;
//    protected HexModel        _m;
//    protected ModelValidation _val;
//    // transients
//    transient byte[]          _data;
//    transient int             _off;
//
//    public ModelTask() {
//    }
//
//    public ModelTask(HexModel m, int[] colIds, Sampling s, double[][] pVals,
//        double ymu) {
//      super(colIds, s, m.skipIncompleteLines(), pVals);
//      _ymu = ymu;
//      _m = m;
//    }
//
//    @Override
//    public void map(Key key) {
//      if( _validate ) _val = _m.makeValidation();
//      if( _resultChunk0 != null )
//        _data = MemoryManager.allocateMemory(_rpc << 3);
//      super.map(key);
//      if( _resultChunk0 != null ) {
//        Key k = ValueArray.getChunk(_resultChunk0, ValueArray.getOffset(key));
//        DKV.put(k, new Value(k, _data));
//      }
//      _reduce = true;
//    }
//
//    @Override
//    void processRow(double[] x) {
//      double ym = _m.getYm(x);
//      if( _validate ) _val.add(_m.getYr(x), ym);
//      if( _storeResults ) {
//        UDP.set8d(_data, _off, ym);
//        _off += 8;
//      }
//    }
//
//    @Override
//    public void reduce(DRemoteTask drt) {
//      _reduce = true;
//      if( _validate ) {
//        ModelTask other = (ModelTask) drt;
//        if( _val == null ) _val = other._val;
//        else _val.add(other._val);
//      }
//    }
//  }
//
//  public static abstract class HexModel implements H2OSerializable {
//    public transient String[] _warnings;   // warning messages from model
//                                            // building
//    transient String[]        _columnNames;
//    transient int[]           _colIds;
//
//    double[][]                _pvals;      // data preprocessing values
//    long                      _n;
//    protected double          _ymu;
//
//    public long n() {
//      return _n;
//    }
//
//    public HexModel() {
//    }
//
//    public HexModel(int [] colIds, double[][] pVals) {
//      this(null,colIds,pVals);
//    }
//    public HexModel(String [] columnNames, double[][] pVals) {
//      this(columnNames,null,pVals);
//    }
//    public HexModel(String[] columnNames, int [] colIds, double[][] pVals) {
//      _colIds = colIds;
//      _columnNames = columnNames;
//      _pvals = pVals;
//    }
//
//    public abstract boolean skipIncompleteLines();
//
//    abstract public double getYm(double[] x);
//
//    public double getYr(double[] x) {
//      return x[x.length - 1];
//    }
//
//    public void addWarning(String s){
//      int n = 0;
//      if(_warnings != null){
//        n = _warnings.length;
//        _warnings = Arrays.copyOf(_warnings, n+1);
//      } else
//        _warnings = new String [1];
//      _warnings[n] = s;
//    }
//
//    abstract ModelValidation makeValidation();
//
//    public Key applyOn(Key k) {
//      throw new UnsupportedOperationException();
//    }
//
//    public void setParameters(String[] args) {
//    }
//
//    public String[][] parameterRange() {
//      throw new UnsupportedOperationException();
//    }
//
//    public boolean parameterRangeSupported() {
//      return false;
//    }
//
//    public ModelValidation validateOn(Key k, Sampling s) {
//      ValueArray ary = (ValueArray) DKV.get(k);
//      // get colIds
//      int[] colIds = _colIds;
//      if(_columnNames != null){
//        colIds = new int[_columnNames.length];
//        L0: for( int i = 0; i < _columnNames.length; ++i ) {
//          for( int j = 0; j < ary.num_cols(); ++j ) {
//            if( _columnNames[i].equalsIgnoreCase(ary.col_name(j)) ) {
//              colIds[i] = j;
//              continue L0;
//            }
//          }
//          if(_colIds == null)throw new Error("Missing column " + _columnNames[i] + " in dataset " + ary._key + ", no previous oclumn ids recorded.");
//          System.out.println("[Model] missing column " + _columnNames[i] + " in dataset " + ary._key + ", using column ids used for model building.");
//          colIds = _colIds;
//          break;
//        }
//      }
//      // get preprocessing flags
//      ModelTask tsk = new ModelTask(this, colIds, s, _pvals, _ymu);
//      tsk.invoke(k);
//      return tsk._val;
//    }
//  }
//
//  public static ModelValidation[] crossValidate(ModelBuilder bldr, int fold,
//      ValueArray data, int[] colIds, int n) {
//    n = Math.min(n, fold);
//    if( fold <= 1 )
//      return new ModelValidation[] { bldr.trainOn(data, colIds, null)
//          .validateOn(data._key, null) };
//    ModelValidation[] res = new ModelValidation[n + 1];
//    Sampling s = new Sampling(0, fold, false);
//    res[0] = bldr.trainOn(data, colIds, s)
//        .validateOn(data._key, s.complement());
//    res[1] = res[0].clone();
//    for( int i = 2; i <= n; ++i ) {
//      s = new Sampling(i - 1, fold, false);
//      res[i] = bldr.trainOn(data, colIds, s).validateOn(data._key,
//          s.complement());
//      res[0].aggregate(res[i]);
//    }
//    return res;
//  }
//}
