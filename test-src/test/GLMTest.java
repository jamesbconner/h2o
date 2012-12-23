package test;
import static org.junit.Assert.assertEquals;
import com.google.gson.*;
import hex.LinearRegression;
import hex.GLMSolver;
import hex.LSMSolver;
import java.util.Map;
import org.junit.*;
import water.*;
import water.parser.ParseDataset;

// A series of tests designed to validate GLM's *statistical results* and not,
// i.e. correct behavior when handed bad/broken/null arguments (although those
// tests are also good).  

public class GLMTest extends KeyUtil {

  // --------
  // Build a ValueArray from a collection of normal arrays
  ValueArray va_maker( Key key, Object... arys ) {

    // Gather basic column info, 1 column per array
    ValueArray.Column cols[] = new ValueArray.Column[arys.length];
    char off = 0;
    int numrows = -1;
    for( int i=0; i<arys.length; i++ ) {
      ValueArray.Column col = cols[i] = new ValueArray.Column();
      col._name = Integer.toString(i);
      col._off = off;
      col._scale = 1;
      col._min = Double.MAX_VALUE;
      col._max = Double.MIN_VALUE;
      col._mean = 0.0;
      Object ary = arys[i];
      if( ary instanceof byte[] ) {
        col._size = 1;
        col._n = ((byte[])ary).length;
      } else if( ary instanceof float[] ) {
        col._size = -4;
        col._n = ((float[])ary).length;
      } else {
        throw H2O.unimpl();
      }
      off += Math.abs(col._size);
      if( numrows == -1 ) numrows = (int)col._n;
      else assert numrows == col._n;
    }
    int rowsize = off;

    // Compact data into VA format, and compute min/max/mean
    AutoBuffer ab = new AutoBuffer(numrows*rowsize);
    for( int i=0; i<numrows; i++ ) {
      for( int j=0; j<arys.length; j++ ) {
        ValueArray.Column col = cols[j];
        double d;  float f;  byte b;
        switch( col._size ) {
        case  1: ab.put1 (b = ((byte [])arys[j])[i]);  d = b;  break;
        case -4: ab.put4f(f = ((float[])arys[j])[i]);  d = f;  break;
        default: throw H2O.unimpl();
        }
        if( d > col._max ) col._max = d;
        if( d < col._min ) col._min = d;
        col._mean += d;
      }
    }
    // Sum to mean
    for( ValueArray.Column col : cols )
      col._mean /= col._n;

    // Write out data & keys
    ValueArray ary = new ValueArray(key,numrows,rowsize,cols);
    Key ckey0 = ary.getChunkKey(0);
    UKV.put(ckey0,new Value(ckey0,ab.bufClose()));
    UKV.put( key ,ary.value());
    return ary;
  }


  // ---
  @Test public void testLinearRegression() {
    Key datakey = Key.make("datakey");
    try {
      // Make some data to test with
      ValueArray va = 
        va_maker(datakey,
                 new byte []{  0 ,  1 ,  2 ,  3 ,  4 ,  5 ,  6 ,  7 ,  8 ,  9 },
                 new float[]{0.0f,0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f});
      
      // Compute LinearRegression between columns 0 & 1
      JsonObject lr = LinearRegression.run(va,0,1);
      assertEquals( 0.0, lr.get("Beta0"   ).getAsDouble(), 0.000001);
      assertEquals( 0.1, lr.get("Beta1"   ).getAsDouble(), 0.000001);
      assertEquals( 1.0, lr.get("RSquared").getAsDouble(), 0.000001);

      // Now a Gaussian GLM model for the same thing
      GLMSolver.GLMParams glmp = new GLMSolver.GLMParams();
      glmp._f = GLMSolver.Family.gaussian;
      glmp._l = glmp._f.defaultLink;
      glmp._familyArgs = glmp._f.defaultArgs;
      glmp._betaEps = 0.00001;
      glmp._maxIter = 10;
      glmp._expandCat = false;
      LSMSolver lsms = LSMSolver.makeSolver(); // Default normalization of NONE
      // Solver
      GLMSolver glms = new GLMSolver(lsms,glmp);
      // Columns to solve over; last column is the result column
      int[] cols = new int[]{0,1};

      // Solve it!
      GLMSolver.GLMModel m = glms.computeGLM(va, cols, null);
      JsonObject glm = m.toJson();

      JsonObject coefs = glm.get("coefficients").getAsJsonObject();
      assertEquals( 0.0, coefs.get("Intercept").getAsDouble(), 0.000001);
      assertEquals( 0.1, coefs.get("0")        .getAsDouble(), 0.000001);

    } finally {
      UKV.remove(datakey);
    }
  }
}
