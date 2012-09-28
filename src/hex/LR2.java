package hex;
import water.*;

/**
 * @author cliffc@0xdata.com
 */
public abstract class LR2 {

  static public String run( ValueArray ary, int colA, int colB ) {
    StringBuilder sb = new StringBuilder();

    sb.append("Linear Regression of ").append(ary._key).append(" between ").
      append(colA).append(" and ").append(colB);

    // Pass 1: compute sums & sums-of-squares
    final long start = System.currentTimeMillis();
    LR_Task1 lr1 = new LR_Task1();
    lr1._arykey = ary._key;
    lr1._colA = colA;
    lr1._colB = colB;
    lr1.invoke(ary._key);
    long pass1 = System.currentTimeMillis();
    sb.append("<p>Pass 1 in ").append(pass1-start).append("msec");
    final long n = lr1._rows;

    // Pass 2: Compute squared errors
    LR_Task2 lr2 = new LR_Task2();
    lr2._arykey = ary._key;
    lr2._Xbar = lr1._sumX / n;
    lr2._Ybar = lr1._sumY / n;
    lr2.invoke(ary._key);
    long pass2 = System.currentTimeMillis();
    sb.append("<p>Pass 2 in ").append(pass2-pass1).append("msec");

    // Compute the regression
    LR_Task3 lr3 = new LR_Task3();
    lr3._arykey = ary._key;
    lr3._beta1 = lr2._XYbar / lr2._XXbar;
    lr3._beta0 = lr2._Ybar - lr3._beta1 * lr2._Xbar;
    lr3._Ybar  = lr2._Ybar;
    // print results
    sb.append("<p>y = ").append(lr3._beta1).append(" * x + ").append(lr3._beta0);

    // Pass 3: analyze results
    lr3.invoke(ary._key);
    long pass3 = System.currentTimeMillis();
    sb.append("<p>Pass 3 in ").append(pass3-pass2).append("msec");

    long df = n - 2;
    double R2    = lr3._ssr / lr2._YYbar;
    double svar  = lr3._rss / df;
    double svar1 = svar / lr2._XXbar;
    double svar0 = svar/n + lr2._Xbar*lr2._Xbar*svar1;
    sb.append("<p>R^2                 = ").append(R2);
    sb.append("<p>std error of beta_1 = ").append(Math.sqrt(svar1));
    sb.append("<p>std error of beta_0 = ").append(Math.sqrt(svar0));
    svar0 = svar * lr1._sumX2 / (n * lr2._XXbar);
    sb.append("<p>std error of beta_0 = ").append(Math.sqrt(svar0));

    sb.append("<p>SSTO = ").append(lr2._YYbar);
    sb.append("<p>SSE  = ").append(lr3._rss);
    sb.append("<p>SSR  = ").append(lr3._ssr);

    return sb.toString();
  }

  public static class LR_Task1 extends MRTask {
    Key _arykey;                // Main ValueArray key
    int _colA, _colB;           // Which columns to work on
    long _rows;                 // Rows used
    double _sumX,_sumY,_sumX2;  // Sum of X's, Y's, X^2's

    public void map( Key key ) {
      assert key.home();
      // Get the root ValueArray for the metadata
      ValueArray ary = (ValueArray)DKV.get(_arykey);
      // Get the raw bits to work on
      byte[] bits = DKV.get(key).get();
      // Split out all the loop-invariant meta-data offset into
      int rowsize = ary.row_size();
      int rows = bits.length/rowsize;
      int colA_off  = ary.col_off  (_colA);
      int colB_off  = ary.col_off  (_colB);
      int colA_size = ary.col_size (_colA);
      int colB_size = ary.col_size (_colB);
      int colA_base = ary.col_base (_colA);
      int colB_base = ary.col_base (_colB);
      int colA_scale= ary.col_scale(_colA);
      int colB_scale= ary.col_scale(_colB);

      if( ary.col_badat(_colA) == 0 && ary.col_badat(_colB) == 0 ) {
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
          double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
          _sumX += X;
          _sumY += Y;
          _sumX2+= X*X;
        }
        _rows = rows;
      } else {
        int bad = 0;
        for( int i=0; i<rows; i++ ) {
          if( ary.valid(bits,i,rowsize,colA_off,colA_size) &&
              ary.valid(bits,i,rowsize,colB_off,colB_size) ) {
            double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
            double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
            _sumX += X;
            _sumY += Y;
            _sumX2+= X*X;
          } else {
            bad++;
          }
        }
        _rows = rows-bad;
      }
    }

    public void reduce( DRemoteTask rt ) {
      LR_Task1 lr1 = (LR_Task1)rt;
      _sumX += lr1._sumX ;
      _sumY += lr1._sumY ;
      _sumX2+= lr1._sumX2;
      _rows += lr1._rows ;
    }
  }


  public static class LR_Task2 extends MRTask {
    Key _arykey;                // Main ValueArray key
    int _colA, _colB;           // Which columns to work on
    double _Xbar, _Ybar, _XXbar, _YYbar, _XYbar;

    public void map( Key key ) {
      assert key.home();
      // Get the root ValueArray for the metadata
      ValueArray ary = (ValueArray)DKV.get(_arykey);
      // Get the raw bits to work on
      byte[] bits = DKV.get(key).get();
      // Split out all the loop-invariant meta-data offset into
      int rowsize = ary.row_size();
      int rows = bits.length/rowsize;
      int colA_off  = ary.col_off  (_colA);
      int colB_off  = ary.col_off  (_colB);
      int colA_size = ary.col_size (_colA);
      int colB_size = ary.col_size (_colB);
      int colA_base = ary.col_base (_colA);
      int colB_base = ary.col_base (_colB);
      int colA_scale= ary.col_scale(_colA);
      int colB_scale= ary.col_scale(_colB);
      final double Xbar = _Xbar;
      final double Ybar = _Ybar;

      if( ary.col_badat(_colA) == 0 && ary.col_badat(_colB) == 0 ) {
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
          double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
          double Xa = (X-Xbar);
          double Ya = (Y-Ybar);
          _XXbar += Xa*Xa;
          _YYbar += Ya*Ya;
          _XYbar += Xa*Ya;
        }
      } else {
        for( int i=0; i<rows; i++ ) {
          if( ary.valid(bits,i,rowsize,colA_off,colA_size) &&
              ary.valid(bits,i,rowsize,colB_off,colB_size) ) {
            double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
            double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
            double Xa = (X-Xbar);
            double Ya = (Y-Ybar);
            _XXbar += Xa*Xa;
            _YYbar += Ya*Ya;
            _XYbar += Xa*Ya;
          }
        }
      }
    }

    public void reduce( DRemoteTask rt ) {
      LR_Task2 lr2 = (LR_Task2)rt;
      _XXbar += lr2._XXbar;
      _YYbar += lr2._YYbar;
      _XYbar += lr2._XYbar;
    }
  }


  public static class LR_Task3 extends MRTask {
    Key _arykey;                // Main ValueArray key
    int _colA, _colB;           // Which columns to work on
    double _Ybar;
    double _beta0, _beta1;
    double _rss, _ssr;

    public void map( Key key ) {
      assert key.home();
      // Get the root ValueArray for the metadata
      ValueArray ary = (ValueArray)DKV.get(_arykey);
      // Get the raw bits to work on
      byte[] bits = DKV.get(key).get();
      // Split out all the loop-invariant meta-data offset into
      int rowsize = ary.row_size();
      int rows = bits.length/rowsize;
      int colA_off  = ary.col_off  (_colA);
      int colB_off  = ary.col_off  (_colB);
      int colA_size = ary.col_size (_colA);
      int colB_size = ary.col_size (_colB);
      int colA_base = ary.col_base (_colA);
      int colB_base = ary.col_base (_colB);
      int colA_scale= ary.col_scale(_colA);
      int colB_scale= ary.col_scale(_colB);
      final double beta0 = _beta0;
      final double beta1 = _beta1;
      final double Ybar  = _Ybar ;
      if( ary.col_badat(_colA) == 0 && ary.col_badat(_colB) == 0 ) {
        for( int i=0; i<rows; i++ ) {
          double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
          double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
          final double fit = beta1*X + beta0;
          final double rs = fit-Y;
          _rss += rs*rs;
          final double sr = fit-Ybar;
          _ssr += sr*sr;
        }
      } else {
        for( int i=0; i<rows; i++ ) {
          if( ary.valid(bits,i,rowsize,colA_off,colA_size) &&
              ary.valid(bits,i,rowsize,colB_off,colB_size) ) {
            double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
            double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
            final double fit = beta1*X + beta0;
            final double rs = fit-Y;
            _rss += rs*rs;
            final double sr = fit-Ybar;
            _ssr += sr*sr;
          }
        }
      }
    }

    public void reduce( DRemoteTask rt ) {
      LR_Task3 lr3 = (LR_Task3)rt;
      _rss += lr3._rss;
      _ssr += lr3._ssr;
    }
  }
}
