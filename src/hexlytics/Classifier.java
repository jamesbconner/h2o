package hexlytics;

public abstract class Classifier {
  public abstract int numClasses();
  public int navigate(double[]_)  { throw new Error("NIY"); }
  public int column()             { throw new Error("no column"); }
  public double value()           { throw new Error("no value"); }
 
  static final class Binary extends Classifier {
    private static final long serialVersionUID = 6496848674571619538L;
    public final int column;
    public final double value;        
    Binary(int c, double v)       { column = c; value = v; }    
    public int navigate(double[]v){ return v[column]<=value?0:1; }
    public int numClasses()       { return 2; }
    public int column()           { return column; }
    public double value()         { return value; }
    public String toString()      { return "Binary("+column+","+Utils.p2d(value)+")"; }
  }
}

