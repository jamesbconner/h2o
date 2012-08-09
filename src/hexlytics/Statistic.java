package hexlytics;


import hexlytics.Data.Int;

/** A statistic that is capable of storing itself into the long[] arrays
 * conveniently. These long arrays are used for fast and memory efficient
 * retrieval of the statistic data by the distributed tree builder. 
 * 
 * TODO This is not thread safe yet! Locks should be implemented. 
 *
 * @author peta
 */
public abstract class Statistic {
   
  /** Produces the classifier from the statistic. If the statistic has seen only
   * rows of one type, the ConstClassifier should be returned.    */ 
  public abstract Classifier classifier();  

  public static Statistic make(String name, Data d) {
    if (name.equals("Numeric")) return new Numeric(d);
    else throw new Error("Unsupported stat " + name);
  }
}

class Numeric extends Statistic {
  
  private final Data data;  // data
  private final Column[] columns_;  //columns for which the averages are computed
  double[] v_;
  /** Hold information about a split. */
  static class Split {
    final int column; final double value, fitness;    
    Split(int column, double splitValue, double fitness) {
      this.column = column;  this.value = splitValue; this.fitness = fitness;
    }    
    boolean betterThan(Split other) { return other==null || fitness > other.fitness;  }
  }

 
  /** Computer  the statistic on each column; holds the distribution of the values 
   * using weights. This class is called from the main statistic for each column 
   * the statistic cares about. */
  class Column {
    int column; // column
    double[][] dists; // 2 x numClasses    
    Column(int c) { column = c; dists = new double[2][data.classes()];  }    
    void add(int class_,double weight) { dists[1][class_] += weight; }    
    
    /** Calculates the best split on given column and returns its information.
     * In order to do this it must sort the rows for the given node and then
     * walk again over them in order (that is out of their natural order) to
     * determine the best splitpoint.   */
    Split split() {
      double fit = Utils.entropyOverColumns(dists); //compute fitness with no prediction
      Data sd = data.sort(column);
      double last = sd.getD(column,sd.rows()-1);
      double currSplit =  sd.getD(column,0);
      if (last == currSplit) return null;
      // now try all the possible splits
      double bestFit = -Double.MAX_VALUE;
      double split = 0, gain = 0;
      for (int i =0; i< sd.rows(); i++) {
        double s = sd.getD(column,i);
        if (s > currSplit) {
          gain = Utils.entropyCondOverRows(dists); // fitness gain
          double newFit = fit - gain; // fitness gain
          if (newFit > bestFit) {
            bestFit = newFit;
            split = (s + currSplit) / 2;
          }
        }
        currSplit = s;
        dists[0][data.classOf()] += data.weight();
        dists[1][data.classOf()] -= data.weight();
      }     
      return new Split(column,split,bestFit);
    }    
  }
  

  /** Creates the classifier. If the node has seen only single data class, a
   * const classifier is used. Otherwise all columns are queried to find the
   * best split. If the chosen selected columns is not able to differentiate
   * between the observations, a Const node will be returned with a majority
   * vote for the class.
   */
  @Override public Classifier classifier() {
    // check if we have only one class
    int cls = 0, cnt = 0;
    double[] vs = columns_[0].dists[1];
    for (int i = 0; i<vs.length;++i)
      if (vs[i]!=0) {
        cnt++; 
        cls = i;
      }
    if (cnt==1) return new Classifier.Const(cls);   
    Split best = null;
    for (Column c: columns_) {
      Split s = c.split();
      if (s.betterThan(best)) best = s;
    }
    //for all chosen columns, all observations have the same values and 
    //no split was selected. In this case we give up and create a Const node. 
    if (best==null) {
      double max = 0; int index= 0;   vs = columns_[0].dists[1];
      for (int j = 0; j<vs.length;++j) if (vs[j]>max) {index=j; max=vs[j];}
      return new Classifier.Const(index);            
    }
    return new Classifier.Binary(best.column,best.value); 
  }

  public Numeric(Data data) {
    columns_ = new Column[data.features()];
    this.data = data;    
    A: for(int i=0;i<data.features();) {
      columns_[i]=new Column(data.random_.nextInt(data.columns())); // TODO: Fix to avoid throwing away columns
      for(int j=0;j<i;j++) if (columns_[i].column==columns_[j].column) continue A;  
      i++;
    }
    v_=new double[data.columns()];
    data.seek(0);
    for(Int it: data)
      for (Column c : columns_) {
//        System.out.println(data.classOf());
        data.classOf();
        c.add(data.classOf(),data.weight());
      }
  }
 }
