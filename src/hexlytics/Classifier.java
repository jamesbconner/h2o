package hexlytics;

import java.io.Serializable;

/** A classifier can simply decide on the class of the data row that is given to
 * it in the classify() method. The class is returned as an integer starting
 * from 0. 
 *
 * @author peta
 */
public interface Classifier extends Serializable {
   /** Returns the number of classes for this classifier. */
  int numClasses();
  int navigate(double[]_);
  int classOf();
  int column();
  double value();
  
  /** Creates the constant classifier that will always return the given result.  */
  static public class Const implements Classifier {
    private static final long serialVersionUID = -3705740604051055127L;
    final int result;
    public Const(int r)         { result = r; }
    public int numClasses()     { return 1; }  
    public int navigate(double[]_)       { throw new Error("NIY"); }
    public int classOf() { return result; }
    public int column() { throw new Error("no column"); }
    public double value() { throw new Error("no value"); }
  }
  
  static final class Binary implements Classifier {
    private static final long serialVersionUID = 6496848674571619538L;
    public final int column;
    public final double value;        
    Binary(int c, double v) { column = c; value = v; }    
    public int navigate(double[]v)   { return v[column]<=value?0:1; }
    public int numClasses()  { return 2; }
    public int classOf() { throw new Error("Unsupported"); }
    public int column() { return column; }
    public double value() { return value; }
  }
}

