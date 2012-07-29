package test.analytics;


import analytics.Classifier;
import analytics.DataAdapter;
import analytics.Statistic;


class DummyStatistic extends Statistic {

  int size;
  int classifyTo = -1;
  
  
  public void addDataPoint(DataAdapter row, long[] data, int offset) {
    
  }

  public int dataSize() {
    return size;
  }

  public Classifier createClassifier(long[] data, int offset) {
    if (classifyTo<0)
      return new Classifier.Const(-classifyTo);
    else return new DummyClassifier(classifyTo);
  }

  public double fitness(long[] data, int offset) {
    return 0;
  }
  
  public DummyStatistic() {
    size = 8;
    classifyTo = -1;
  }
  
  public DummyStatistic(int size, int classifyTo) {
    this.size = size;
    this.classifyTo = classifyTo;
  }
  
}




class DummyDataSet extends DataAdapter {

 
  public void seekToRow(int index) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public int numRows() {
    return 0;
  }

  public int numColumns() {
    return 10;
  }

  public boolean isInt(int index) {
    // TODO Auto-generated method stub
    return false;
  }

  public int toInt(int index) {
    // TODO Auto-generated method stub
    return 0;
  }

  public double toDouble(int index) {
    // TODO Auto-generated method stub
    return 0;
  }

  public Object originals(int index) {
    // TODO Auto-generated method stub
    return null;
  }

  public int numClasses() {
    // TODO Auto-generated method stub
    return 0;
  }

  public int dataClass() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  
}

class DummyClassifier implements Classifier {

  int nc;
  int c;
  
  public int classify(DataAdapter row) {
    c =  (c+1) % nc;
    return c;
  }

  public int numClasses() {
    return nc;
  }
  
  public DummyClassifier(int nc) {
    this.nc = nc;
    c = -1;
  }
  
  
}



/**
 *
 * @author peta
 */
public class Mockups {
  
}
