package hex;

import hex.DLSM.DLSM_SingularMatrixException;
import hex.DLSM.LSMTask;
import hex.DLSM.LSM_Params;
import hex.DLSM.Norm;
import hex.Models.BinaryClassifierValidation;
import hex.RowVecTask.DataPreprocessing;
import hex.RowVecTask.Sampling;
import init.H2OSerializable;

import java.io.*;
import java.util.Arrays;
import java.util.Map.Entry;

import water.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


/**
 * Distributed General Linear Model solver.
 *
 * Implemented as Iterative Reweighted LSM fitting problem. Calls DLSM iteratively until solution is found.
 *
 * Goal is to have distributed version of glm from R with few enhancements (L1 and L2 norm).
 *
 * Current Limitations:
 *   * only gaussian, binomial and poisson family supported at the moment.
 *   * limitations of underlying DLSM apply here as well
 *
 * Implemented by extending LSMTask (by IRLSMTask) to transform response variable and to apply weights in flight).
 *
 * @author tomasnykodym
 *
 */
public class DGLM implements Models.ModelBuilder {

  public static enum Link {
    identity(0),
    logit(0),
    log(0),
//    probit(0),
//    cauchit(0),
//    cloglog(0),
//    sqrt(0),
    inverse(1.0),
//    oneOverMu2(0);
    ;
    public final double defaultBeta;

    Link(double b){defaultBeta = b;}
  }

  // supported families
  public static enum Family {
    gaussian(Link.identity),
    binomial(Link.logit),
    poisson(Link.log),
    gamma(Link.inverse);
    ;
    public final Link defaultLink;

    Family(Link l){defaultLink = l;}
  }



  public DGLM(GLM_Params glmPArams, LSM_Params lsmParams) {
    _glmParams = glmPArams;
    _lsmParams = lsmParams;
  }
  /**
   * Per family variance computation
   *
   * @param family
   * @param mu
   * @return
   */
  public static double variance(Family family, double mu){
    switch(family){
    case gaussian:
      return 1;
    case binomial:
      assert 0 <= mu && mu <= 1:"unexpected mu:" + mu;
      return mu*(1-mu);
    case poisson:
      return mu;
    case gamma:
      return mu*mu;
    default:
      throw new Error("unknown family Id " + family);
    }
  }


  // helper function
  static double y_log_y(double y, double mu){
    mu = Math.max(Double.MIN_NORMAL, mu);
    return (y != 0) ? (y * Math.log(y/mu)) : 0;
  }

  /**
   * Per family deviance computation.
   *
   * @param family
   * @param yr
   * @param ym
   * @return
   */
  public static double deviance(Family family, double yr, double ym){
    switch(family){
    case gaussian:
      return (yr - ym)*(yr - ym);
    case binomial:
      return 2*((y_log_y(yr, ym)) + y_log_y(1-yr, 1-ym));
      //return -2*(yr * ym - Math.log(1 + Math.exp(ym)));
    case poisson:
      //ym = Math.exp(ym);
      if(yr == 0)return 2*ym;
      return 2*((yr * Math.log(yr/ym)) - (yr - ym));
    case gamma:
      if(yr == 0)return -2;
      return -2*(Math.log(yr/ym) - (yr - ym)/ym);
    default:
      throw new Error("unknown family Id " + family);
    }
  }

  /**
   * Link function computation.
   *
   * @param linkFunction
   * @param x
   * @return
   */
  public static double link(Link linkFunction, double x){
   switch(linkFunction){
     case identity:
       return x;
     case logit:
       assert 0 <= x && x <= 1;
       return Math.log(x/(1 - x));
     case log:
       return Math.log(x);
//     case inverse:
//       return 1/x;
     default:
       throw new Error("unsupported link function id  " + linkFunction);
   }
  }

  /**
   * Link function inverse computation.
   *
   * @param linkFunction
   * @param x
   * @return
   */
  public static double linkInv(Link linkFunction, double x){
    switch(linkFunction){
      case identity:
        return x;
      case logit:
        return 1.0 / (Math.exp(-x) + 1.0);
      case log:
        return Math.exp(x);
      case inverse:
        return 1/x;
      default:
        throw new Error("unexpected link function id  " + linkFunction);
    }
   }

  /**
   * Link function derivative computation.
   *
   * @param linkFunction
   * @param x
   * @return
   */
  public static double linkDeriv(Link linkFunction, double x){
    switch(linkFunction){
      case identity:
        return 1;
      case logit:
        if( x == 1 || x == 0 ) return MAX_SQRT;
        return 1 / (x * (1 - x));
      case log:
        return (x == 0)?MAX_SQRT:1/x;
      case inverse:
        return -1/(x*x);
      default:
        throw new Error("unexpected link function id  " + linkFunction);
    }
   }


  /**
   * Additional per-family args to glm.
   * @author tomasnykodym
   *
   */
  public static abstract class FamilyArgs implements H2OSerializable {
    public JsonObject toJson(){
      return new JsonObject();
    }
    public int wire_len(){return 0;}
    public void write(Stream s){}
    public void write(DataOutputStream dos){}
    public void read(Stream s){}
    public void read(DataInputStream dis){}
  }

  /**
   *  Args for binomial family
   * @author tomasnykodym
   *
   */
  public static class BinomialArgs extends FamilyArgs {
    double _threshold = 0.5; // decision threshold for classification/validation
    double _case = 1.0; // value to be mapped to 1 (en eeverything else to 0).
    double [] _wt = new double[]{1.0,1.0};

    public BinomialArgs(double t, double c, double [] wt){
      _threshold = t;
      _case = c;
      _wt = wt;
    }
    @Override
    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("threshold", _threshold);
      res.addProperty("case", _case);
      res.addProperty("weights", Arrays.toString(_wt));
      return res;
    }
  }

  /**
   * Paramters for GLM.
   * @author tomasnykodym
   *
   */
  public static class GLM_Params implements H2OSerializable {
    public static final int DEFAULT_MAX_ITER = 50;
    public double _betaEps = 1e-3; // precision level for beta
    public Family _family = Family.gaussian;
    public Link _link = Link.identity;
    public DataPreprocessing _preprocessing = DataPreprocessing.AUTO;
    public int _maxIter = DEFAULT_MAX_ITER;
    public FamilyArgs _fargs;
    public double [] _weights;

    public GLM_Params(){}
    public GLM_Params(Family f, FamilyArgs fargs, Link l, int maxIter, double beps, DataPreprocessing p){
      _family = f;
      _fargs = fargs;
      _link = l;
      _maxIter = maxIter;
      _betaEps = beps;
      _preprocessing = p;
    }
    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("betaEps", _betaEps);
      res.addProperty("family", _family.toString());
      res.addProperty("link", _link.toString());
      res.addProperty("prepropcessing", _preprocessing.toString());
      res.addProperty("maxIter", _maxIter);
      if(_fargs != null){
        JsonObject fargsJson = _fargs.toJson();
        for(Entry<String,JsonElement> e:fargsJson.entrySet())
          res.addProperty(e.getKey(), e.getValue().toString());
      }
      return res;
    }

    public int wire_len(){
      return 8 + 4 + 4 + 4 + 4 + _fargs.wire_len() + UDP.wire_len(_weights);
    }

    public void write(Stream s){
     s.set8d(_betaEps);
     s.set4(_family.ordinal());
     s.set4(_link.ordinal());
     s.set4(_preprocessing.ordinal());
     s.set4(_maxIter);
     _fargs.write(s);
     s.setAry8d(_weights);
    }

    public void write(DataOutputStream dos) throws IOException{
      dos.writeDouble(_betaEps);
      dos.writeInt(_family.ordinal());
      dos.writeInt(_link.ordinal());
      dos.writeInt(_preprocessing.ordinal());
      dos.writeInt(_maxIter);
      _fargs.write(dos);
      TCPReceiverThread.writeAry(dos, _weights);
     }

    public void read(Stream s){
      _betaEps = s.get8d();
      _family = Family.values()[s.get4()];
      _link = Link.values()[s.get4()];
      _preprocessing = DataPreprocessing.values()[s.get4()];
      _maxIter = s.get4();
      _fargs = (FamilyArgs)SerializationUtils.readObject(s);
      s.setAry8d(_weights);
     }

     public void read(DataInputStream dis) throws IOException{
       _betaEps = dis.readDouble();
       _family = Family.values()[dis.readInt()];
       _link = Link.values()[dis.readInt()];
       _preprocessing = DataPreprocessing.values()[dis.readInt()];
       _maxIter = dis.readInt();
       _fargs = (FamilyArgs)SerializationUtils.readObject(dis);
       _weights = TCPReceiverThread.readDoubleAry(dis);
      }
  }

  public static class GLSMException extends RuntimeException {
    public GLSMException(String msg) {
      super(msg);
    }
  }

  private static final double MAX_SQRT = Math.sqrt(Double.MAX_VALUE);

  Sampling         _sampling;


  public static final LSM_Params defaultLSMParams = new LSM_Params();

  GLM_Params _glmParams;
  LSM_Params _lsmParams;


/**
 * Solve glm problem by iterative reweighted least square method.
 * Repeatedly solves LSM problem with weights given by previous iteration until
 * a fixed point is reached.
 */
  public GLMModel trainOn(ValueArray ary, int[] colIds, Sampling s) {
    if(_lsmParams == null)_lsmParams = defaultLSMParams;
    DataPreprocessing dp = _glmParams._preprocessing;
    if(_glmParams._preprocessing == DataPreprocessing.AUTO){
      // default is to standardize data if using penalty function and do nothing if not
      if(_lsmParams.n != Norm.NONE) dp = DataPreprocessing.STANDARDIZE;
      else dp = DataPreprocessing.NONE;
    }
    double[][] pVals = RowVecTask.getDataPreprocessingForColumns(dp, ary, colIds);
    if(pVals != null){
      // do not preprocess y!!!
      pVals[pVals.length-1][0] = 0;
      pVals[pVals.length-1][1] = 0;
    }
    String [] colNames = new String [colIds.length];
    for(int i = 0; i < colIds.length; ++i){
      colNames[i] = ary.col_name(colIds[i]);
      if(colNames[i] == null){
        colNames = null;
        break;
      }
    }
    //GLM_Model m = (glmParams.family == Family.binomial)? new BinomialModel((BinomialArgs)fargs):new GLM_Model();//colIds, beta, p, gp, lp)
    GLMModel m = (_glmParams._family == Family.binomial)?new GLMBinomialModel(colNames, colIds, pVals,null,0,_glmParams):new GLMModel(colNames,colIds, pVals, null,0, _glmParams);
    if(_glmParams._family == Family.gaussian){
      LSMTask tsk = new LSMTask(colIds, s, colIds.length - 1,  _lsmParams.constant, pVals);
      tsk.invoke(ary._key);
      m._n = tsk._n;
      try {
        m._beta = DLSM.solveLSM(tsk._xx, tsk._xy, _lsmParams);
      }catch (DLSM_SingularMatrixException e){
        int n = 0;
        if(m._warnings != null){
          n = m._warnings.length;
          m._warnings = Arrays.copyOf(m._warnings, n + 1);
        } else
          m._warnings = new String[1];
        m._warnings[n] = "Failed to compute without normalization due to singular gram matrix. Rerun with L2 regularization and lambda = 1e-5";
        m._beta = e.res;
      }
      ++m._iterations;
      return m;
    }
    double [] beta = new double [colIds.length];
    Arrays.fill(beta, _glmParams._link.defaultBeta);
    double diff = 0;
    long N = 0;
    m._ymu = ary.col_mean(colIds[colIds.length-1]);
    try{
      for(int i = 0; i != _glmParams._maxIter; ++i) {
        ++m._iterations;
        //System.out.println("iteration: " + i + ", beta = " + Arrays.toString(beta));
        IRLSMTask tsk;
        switch(_glmParams._family){
        case binomial:
          BinomialArgs bargs = (BinomialArgs)_glmParams._fargs;
          tsk = new BinomialTask(colIds, s, _lsmParams.constant, beta, pVals, _glmParams._link,bargs);
          tsk.invoke(ary._key);
          m._ymu = ((BinomialTask)tsk)._caseCount/(double)tsk._n;
          break;
        default:
          tsk = new IRLSMTask(colIds, s, _lsmParams.constant, beta, pVals, _glmParams._family, _glmParams._link);
          tsk.invoke(ary._key);

        }
        diff = 0;
        N = tsk._n;
        try {
          tsk._beta = DLSM.solveLSM(tsk._xx, tsk._xy, _lsmParams);
        }catch (DLSM_SingularMatrixException e){
          int n = 0;
          if(m._warnings != null){
            n = m._warnings.length;
            m._warnings = Arrays.copyOf(m._warnings, n + 1);
          } else
            m._warnings = new String[1];
          m._warnings[n] = "Using L2 regularization due to singular gram matrix.";
          tsk._beta = e.res;
        }
        if( beta != null ) for( int j = 0; j < beta.length; ++j )
          diff = Math.max(diff, Math.abs(beta[j] - tsk._beta[j]));
        else diff = Double.MAX_VALUE;
        beta = tsk._beta;
        if(diff < _glmParams._betaEps)break;
      }
    } catch (Exception e) {
      if(beta == null)throw new GLSMException("Failed to compute the data: " + e.getMessage());;
      int n = 0;
      if(m._warnings != null){
        n = m._warnings.length;
        m._warnings = Arrays.copyOf(m._warnings, n+1);
      } else
        m._warnings = new String[1];
      m._warnings[n] = "Failed to converge due to NaNs";
    }
    if(diff >= _glmParams._betaEps)m.addWarning("Reached max # iterations: " + _glmParams._maxIter);
    m._beta = beta;
    m._n = N;
    return m;
  }

  /**
   * Task computing one round of logistic regression by iterative least square
   * method. Given beta_k, computes beta_(k+1). Works by transforming input
   * vector by link function and applying weights equal to inverse of variance
   * and  passing the transformed input to LSM.
   *
   * @author tomasnykodym
   *
   */
  public static class IRLSMTask extends LSMTask {
    double[] _beta;
    double   _w = 1.0;
    double   _origConstant;
    int      _f;
    int      _l;

    public IRLSMTask() {
    } // Empty constructor for the serializers

    public IRLSMTask(int[] colIds, Sampling s, int constant, double[] beta,
        double[][] pVals, Family f, Link l) {
      super(colIds, s, colIds.length - 1, constant, pVals);
      _beta = beta;
      _f = f.ordinal();
      _l = l.ordinal();
    }

    @Override
    public void preMap(int xlen, int nrows) {
      super.preMap(xlen, nrows);
      _origConstant = _constant;
    }

    /**
     * Applies the link function on the input and calls
     * underlying LSM.
     *
     * Two steps are performed here:
     * 1) y is replaced by z, which is obtained by
     * Taylor expansion at the point of last estimate of y (x'*beta)
     * 2) Weight is applied to both x and y. Weight is the square root of inverse of variance of y at this
     * data point according to our model
     *
     */
    @Override
    public void processRow(double[] x) {
      Family f = Family.values()[_f];
      Link l = Link.values()[_l];
      double y = x[x.length-1];
      // transform input to the GLR according to Olga's slides
      // (glm lecture, page 12)
      // Step 1, compute the estimate of y according to previous model (old
      // beta)
      double gmu = 0.0;
      for( int i = 0; i < x.length - 1; ++i ) {
        gmu += x[i] * _beta[i];
      }
      // add the constant (constant/Intercept is not included in the x vector,
      // have to add it separately)
      gmu += _origConstant * _beta[x.length - 1];
      // get the inverse to get estimate of p(Y=1|X) according to previous model
      double mu = linkInv(l,gmu);
      double dgmu = linkDeriv(l,mu);
      x[x.length - 1] = gmu + (y - mu) * dgmu; // z = y approx by Taylor
                                               // expansion at the point of our
                                               // estimate (mu), done to avoid
                                               // log(0),log(1)
      // Step 2
      double vary = variance(f,mu); // variance of y according to our model

      // compute the weights (inverse of variance of z)
      double var = dgmu * dgmu * vary;
      // Apply the weight. We want each data point to have weight of inverse of
      // the variance of y at this point.
      // Since we compute x'x, we take sqrt(w) and apply it to both x and y
      // (we also compute X*y)
      double w = Math.sqrt(1 / var)*_w;
      for( int i = 0; i < x.length; ++i )
        x[i] *= w;
      _constant = _origConstant * w;
      super.processRow(x);
    }
  }

  /**
   * Specialization of IRLSM for binomial family. Values 0/1 are enforced.(_case = 1, everything else = 0)
   */
  public static class BinomialTask extends IRLSMTask {
    double _case; // in
    long _caseCount; // out
    double [] _wt;

    public BinomialTask(int [] colIds, Sampling s, int constant, double [] beta, double[][] pVals, Link l,BinomialArgs bargs){
      super(colIds,s,constant, beta, pVals,  Family.binomial,l);
      _case = bargs._case;
      _wt = bargs._wt;
    }

    @Override
    public void processRow(double [] x){
      if(x[x.length-1] == _case){
        x[x.length-1] = 1.0;
        _w = _wt[1];
        ++_caseCount;
      } else {
        x[x.length-1] = 0.0;
        _w = _wt[0];
      }

      super.processRow(x);
    }
  }


  public static class GLMModel extends Models.NewModel {
    double [] _beta;
    int [] _colIds;
    public int _iterations;
    GLM_Params _glmParams;

    public GLMModel(){}

    public GLMModel(String [] columnNames, int [] colIds, double [][] pVals) {
      this(columnNames, colIds, pVals, null, 0.0, new GLM_Params());
    }

    public GLMModel(String [] columnNames,int [] colIds, double[][] pVals, double [] b, double ymu, GLM_Params glmParams) {
      super(columnNames,colIds, pVals);
      _beta = b;
      _glmParams = glmParams;
      _ymu = ymu;
    }

    public double [] beta(){return _beta;}
    @Override
    public boolean skipIncompleteLines() {
      return true;
    }

    public double getMu(double[] x) {
      double ym = 0;
      for( int i = 0; i < (_beta.length-1); ++i )
        ym += x[i] * _beta[i];
      ym += _beta[_beta.length - 1];
      return ym;
    }

    public double getYm(double[] x) {
      return linkInv(_glmParams._link,getMu(x));
    }

    @Override
    Models.ModelValidation makeValidation() {
      return new GLMValidation(_ymu, _glmParams);
    }

  }
  public static class GLMValidation extends Models.ModelValidation {
    double _nullDev;
    double _resDev;
    double _err;
    double _errVar;
    transient double _ymu;
    GLM_Params _glmParams;
    long _n;
    int _t = 1;

    public GLMValidation(double ymu, GLM_Params glmParams){
      _ymu = ymu;
      _glmParams = glmParams;
    }

    public GLMValidation(GLMValidation other){
      _nullDev = other._nullDev;
      _resDev = other._resDev;
      _err = other._err;
      _errVar = other._errVar;
      _ymu = other._ymu;
      _glmParams = other._glmParams;
      _n = other._n;
      _t = other._t;
    }

    @Override
    public void add(double yr, double ym) {
      _nullDev += deviance(_glmParams._family, yr, _ymu);
      _resDev += deviance(_glmParams._family, yr, ym);
      _err += (yr-ym)*(yr-ym);
      ++_n;
    }

    @Override
    public void add(Models.ModelValidation other) {
      GLMValidation v = (GLMValidation)other;
      _nullDev += v._nullDev;
      _resDev += v._resDev;
      _n += v._n;
      _err += v._err;
    }

    @Override
    public void aggregate(Models.ModelValidation mv) {
      GLMValidation other = (GLMValidation)mv;
      // recursive avg formula
      _n += other._n;
      ++_t;
      _err = (_t - 1.0) / _t * _err + 1.0 / _t * other._err;
      // recursive variance formula
      double newVar = (other._err - _err);
      _errVar = ((_t - 1.0) / _t) * _errVar + (1.0 / (_t - 1)) * newVar
          * newVar;
    }

    @Override
    public double err() {
      return _err;
    }

    public double nullDeviance(){return _nullDev;}
    public double resDeviance(){return _resDev;}

    @Override
    public Models.ModelValidation clone() {
      return new GLMValidation(this);
    }

    @Override
    public long n() {
      return _n;
    }

  }


  public static class GLMBinomialModel extends GLMModel {

    @Override
    public double getYr(double[] x) {
      return (x[x.length-1] == ((BinomialArgs)_glmParams._fargs)._case)?1.0:0.0;
    }

    public GLMBinomialModel(){}
    public GLMBinomialModel(String [] columNames, int [] colIds, double [][] pVals){
      super(columNames, colIds, pVals);
    }

    public GLMBinomialModel(String [] columnNames, int [] colIds, double[][] pVals, double [] b, double ymu, GLM_Params glmParams){
      super(columnNames, colIds, pVals,b,ymu, glmParams);
    }



    @Override
    Models.ModelValidation makeValidation() {
      return new GLMBinomialValidation(_ymu,_glmParams);
    }
  }

  public static class GLMBinomialValidation extends GLMValidation implements BinaryClassifierValidation, H2OSerializable {
    public long [][] _cm;
    double _fpMean;
    double _fpVar;
    double _fnMean;
    double _fnVar;
    double _tnMean;
    double _tnVar;
    double _tpMean;
    double _tpVar;
    boolean _aggregate;

    public GLMBinomialValidation(double ymu, GLM_Params glmParams){
      super(ymu,glmParams);
      _cm = new long[2][2];
    }

    public GLMBinomialValidation(GLMBinomialValidation other){
      super(other);
      _cm = other._cm.clone();
    }

    @Override
    public void add(double yr, double ym) {
      assert !_aggregate;
      super.add(yr,ym);
      int m = (ym > ((BinomialArgs)_glmParams._fargs)._threshold)?1:0;
      int r = (int)yr;
      assert r == yr;
      ++_cm[m][r];
    }

    @Override
    public void aggregate(Models.ModelValidation other) {
      super.aggregate(other);
      GLMBinomialValidation v = (GLMBinomialValidation)other;
      _fpMean = (_t - 1.0) / _t * fp() + 1.0 / _t * v.fp();
      // recursive variance formula
      double newVar = (v.fp() - fp());
      _fpVar = ((_t - 1.0) / _t) * _fpVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _tpMean = (_t - 1.0) / _t * tp() + 1.0 / _t * tp();
      // recursive variance formula
      newVar = (v.tp() - tp());
      _tpVar = ((_t - 1.0) / _t) * _tpVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _tnMean = (_t - 1.0) / _t * tn() + 1.0 / _t * v.tn();
      // recursive variance formula
      newVar = (v.tn() - tn());
      _tnVar = ((_t - 1.0) / _t) * _tnVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _fnMean = (_t - 1.0) / _t * fn() + 1.0 / _t * v.fn();
      // recursive variance formula
      newVar = (v.fn() - fn());
      _fnVar = ((_t - 1.0) / _t) * _fnVar + (1.0 / (_t - 1)) * newVar
          * newVar;
      _aggregate = true;

    }

    public long cm(int i, int j){
      return _cm[i][j];
    }

    @Override
    public void add(Models.ModelValidation other) {
      super.add(other);
      GLMBinomialValidation bv = (GLMBinomialValidation)other;
      for(int i = 0; i < _cm.length; ++i)
        for(int j = 0; j < _cm.length; ++j)
          _cm[i][j] += bv._cm[i][j];
      _err = (_cm[0][1] + _cm[1][0])/(double)_n;
    }

    @Override
    public double err() {
      if(_n == 0)return 0;
      return  (_cm[0][1] + _cm[1][0])/(double)_n;
    }

    public double fp(){
      return (_aggregate?_fpMean:(double)_cm[1][0]/(double)_n);
    }
    public double fpVar(){
      return _fpVar;
    }

    public double fn(){
      return _aggregate?_fnMean:(double)_cm[0][1]/(double)_n;
    }

    public double fnVar(){
      return _fnVar;
    }

    public double tp(){
      return _aggregate?_tpMean:(double)_cm[1][1]/(double)_n;
    }

    public double tpVar(){
      return _tpVar;
    }

    public double tn(){
      return _aggregate?_tnMean:(double)_cm[0][0]/(double)_n;
    }
    public double tnVar(){
      return _tnVar;
    }

    @Override
    public int classes() {
      return 2;
    }
    @Override
    public GLMBinomialValidation clone() {
      return new GLMBinomialValidation(this);
    }
  }
}
