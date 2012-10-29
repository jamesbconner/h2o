package hex;

import hex.RowVecTask.Sampling;

import java.io.*;

import water.*;

public abstract class Models {

  public interface ModelBuilder {
    public NewModel trainOn(ValueArray data, int[] colIds, Sampling s);
  }

  public interface ModelValidation extends Cloneable {
    public void add(double yr, double ym);

    public void add(ModelValidation other);

    public double err();

    public long n();

    /**
     * Combines result during cross validation.
     *
     * It should update mean and variance of error and any other value of
     * interest used in this validation
     *
     * @param other
     */
    public abstract void aggregate(ModelValidation other);

    public ModelValidation clone();

    public int wire_len();

    public void write(Stream s);

    public void read(Stream s);

    public void write(DataOutputStream os) throws IOException;

    public void read(DataInputStream is) throws IOException;
  }

  public interface ClassifierValidation extends ModelValidation {
    public int classes();

    public long cm(int i, int j);
  }

  public interface BinaryClassifierValidation extends ClassifierValidation {
    public double fp();
    public double fpVar();
    public double fn();
    public double fnVar();
    public double tp();
    public double tpVar();
    public double tn();
    public double tnVar();
  }

  public static class ModelTask extends RowVecTask {
    boolean                   _validate     = true;
    boolean                   _reduce;
    boolean                   _storeResults;
    int                       _rpc;
    double                    _ymu;
    Key                       _resultChunk0;
    protected NewModel        _m;
    protected ModelValidation _val;
    // transients
    transient byte[]          _data;
    transient int             _off;

    public ModelTask() {
    }

    public ModelTask(NewModel m, int[] colIds, Sampling s, double[][] pVals,
        double ymu) {
      super(colIds, s, m.skipIncompleteLines(), pVals);
      _ymu = ymu;
      _m = m;
    }

    @Override
    public int wire_len() {
      return super.wire_len() + ((_reduce)?
          2 + (_validate ? _val.wire_len() : 0):
            14 + (_storeResults?_resultChunk0.wire_len():0) + _m.wire_len());
    }

    public void write(DataOutputStream os) throws IOException {
      super.write(os);
      os.writeBoolean(_validate);
      os.writeBoolean(_reduce);
      if( _reduce ) {
        if( _validate ) {
          byte[] cn = _val.getClass().getName().getBytes();
          os.write(cn.length);
          os.write(cn);
          _val.write(os);
        }
      } else {
        os.writeBoolean(_storeResults);
        os.writeInt(_rpc);
        os.writeDouble(_ymu);
        byte[] cn = _m.getClass().getName().getBytes();
        os.write(cn.length);
        os.write(cn);
        _m.write(os);
        if( _storeResults ) _resultChunk0.write(os);
      }
    }

    public void read(DataInputStream is) throws IOException {
      super.read(is);
      try {
        _validate = is.readBoolean();
        _reduce = is.readBoolean();
        if( _reduce ) {
          if( _validate ) {
            int cnLen = is.readInt();
            byte[] cn = new byte[cnLen];
            is.readFully(cn);
            _val = (ModelValidation) Class.forName(new String(cn)).newInstance();
            _val.read(is);
          }
        } else {
          _storeResults = is.readBoolean();
          _rpc = is.readInt();
          _ymu = is.readDouble();
          int cnLen = is.readInt();
          byte[] cn = new byte[cnLen];
          is.readFully(cn);
          _m = (NewModel) Class.forName(new String(cn)).newInstance();
          _m.read(is);
          if( _storeResults ) _resultChunk0 = Key.read(is);
        }
      } catch( Exception e ) {
        throw new Error(e);
      }
    }

    public void write(Stream s) {
      super.write(s);
      s.set1(_validate?1:0);
      s.set1(_reduce?1:0);
      if( _reduce ) {
        if( _validate ) {
          s.setAry1(_val.getClass().getName().getBytes());
          _val.write(s);
        }
      } else {
        s.set1(_storeResults?1:0);
        s.set4(_rpc);
        s.set8d(_ymu);
        s.setAry1(_m.getClass().getName().getBytes());
        _m.write(s);
        if( _storeResults ) _resultChunk0.write(s);
      }
    }

    public void read(Stream s) {
      super.read(s);
      try{
      _validate = (s.get1() != 0);
      _reduce = (s.get1() != 0);
      if( _reduce ) {
        if( _validate ) {
          _val = (ModelValidation)Class.forName(new String(s.getAry1())).newInstance();
          _val.read(s);
        }
      } else {
        _storeResults = (s.get1() != 0);
        _rpc = s.get4();
        _ymu = s.get8d();
        _m = (NewModel)Class.forName(new String(s.getAry1())).newInstance();
        _m.read(s);
        if( _storeResults )_resultChunk0 = Key.read(s);
      }
      }catch(Exception e){
        throw new Error(e);
      }
    }

    @Override
    public void map(Key key) {
      if( _validate ) _val = _m.makeValidation();
      if( _resultChunk0 != null )
        _data = MemoryManager.allocateMemory(_rpc << 3);
      super.map(key);
      if( _resultChunk0 != null ) {
        Key k = ValueArray.getChunk(_resultChunk0, ValueArray.getOffset(key));
        DKV.put(k, new Value(k, _data));
      }
      _reduce = true;
    }

    @Override
    void processRow(double[] x) {
      double ym = _m.getYm(x);
      if( _validate ) _val.add(_m.getYr(x), ym);
      if( _storeResults ) {
        UDP.set8d(_data, _off, ym);
        _off += 8;
      }
    }

    @Override
    public void reduce(DRemoteTask drt) {
      if( _validate ) {
        ModelTask other = (ModelTask) drt;
        if( _val == null ) _val = other._val;
        else if(other._val != null) _val.add(other._val);
      }
    }
  }

  public static abstract class NewModel extends RemoteTask {
    public transient String[] _warnings;   // warning messages from model
                                            // building
    transient String[]        _columnNames;
    transient int[]           _colIds;

    double[][]                _pvals;      // data preprocessing values
    long                      _n;
    protected double          _ymu;

    public long n() {
      return _n;
    }

    public NewModel() {
    }

    public NewModel(int [] colIds, double[][] pVals) {
      this(null,colIds,pVals);
    }
    public NewModel(String [] columnNames, double[][] pVals) {
      this(columnNames,null,pVals);
    }
    public NewModel(String[] columnNames, int [] colIds, double[][] pVals) {
      _colIds = colIds;
      _columnNames = columnNames;
      _pvals = pVals;
    }

    public abstract boolean skipIncompleteLines();

    abstract public double getYm(double[] x);

    public double getYr(double[] x) {
      return x[x.length - 1];
    }

    abstract ModelValidation makeValidation();

    public Key applyOn(Key k) {
      throw new UnsupportedOperationException();
    }

    public void setParameters(String[] args) {
    }

    public String[][] parameterRange() {
      throw new UnsupportedOperationException();
    }

    public boolean parameterRangeSupported() {
      return false;
    }

    public ModelValidation validateOn(Key k, Sampling s) {
      ValueArray ary = (ValueArray) DKV.get(k);
      // get colIds
      int[] colIds = _colIds;
      if(_columnNames != null){
        colIds = new int[_columnNames.length];
        L0: for( int i = 0; i < _columnNames.length; ++i ) {
          for( int j = 0; j < ary.num_cols(); ++j ) {
            if( _columnNames[i].equalsIgnoreCase(ary.col_name(j)) ) {
              colIds[i] = j;
              continue L0;
            }
          }
          if(_colIds == null)throw new Error("Missing column " + _columnNames[i] + " in dataset " + ary._key + ", no previous oclumn ids recorded.");
          System.out.println("[Model] missing column " + _columnNames[i] + " in dataset " + ary._key + ", using column ids used for model building.");
          colIds = _colIds;
          break;
        }
      }
      // get preprocessing flags
      ModelTask tsk = new ModelTask(this, colIds, s, _pvals, _ymu);
      tsk.invoke(k);
      return tsk._val;
    }

    @Override
    public void invoke(Key k) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void compute() {
      throw new UnsupportedOperationException();
    }

  }

  public static ModelValidation[] crossValidate(ModelBuilder bldr, int fold,
      ValueArray data, int[] colIds, int n) {
    n = Math.min(n, fold);
    if( fold <= 1 )
      return new ModelValidation[] { bldr.trainOn(data, colIds, null)
          .validateOn(data._key, null) };
    ModelValidation[] res = new ModelValidation[n + 1];
    Sampling s = new Sampling(0, fold, false);
    res[0] = bldr.trainOn(data, colIds, s)
        .validateOn(data._key, s.complement());
    res[1] = res[0].clone();
    for( int i = 2; i <= n; ++i ) {
      s = new Sampling(i - 1, fold, false);
      res[i] = bldr.trainOn(data, colIds, s).validateOn(data._key,
          s.complement());
      res[0].aggregate(res[i]);
    }
    return res;
  }
}
