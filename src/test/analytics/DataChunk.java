package test.analytics;



public class DataChunk {
  
  
  static final int SIZE = 2 * 1000; //  * 1000;
  
  // Adds getters for the data as abstract methods
  abstract class Column {
    abstract float toFloat(int index);
    abstract int toInt(int index);
  }

  class IntColumn extends Column{
    int[] data;
    IntColumn(){data=new int[SIZE];}
  
    float toFloat(int index) {
      return data[index];
    }
    
    int toInt(int index) {
      return data[index];
    }
  
  }
  class FloatColumn extends Column {
    float[] data;
    FloatColumn(){data=new float[SIZE];}
    float toFloat(int index) {
      return data[index];
    }
    
    int toInt(int index) {
      return (int)data[index];
    }
  
  }
  class ByteColumn extends Column {
    byte[] data;
    ByteColumn(){data=new byte[SIZE];}

    float toFloat(int index) {
      return data[index];
    }
    
    int toInt(int index) {
      return data[index];
    }
 }
  
  Column[] cols = new Column[4]; 
  int numCols;
  
  private void grow() {
    if(numCols==cols.length){ 
      Column[] newcols = new Column[cols.length*2];
      for(int i=0;i<cols.length;i++)newcols[i]=cols[i];
      cols=newcols;
    }
  }
  void addIntCol() { grow(); cols[numCols++]= new IntColumn(); }
  void addFloatCol() { grow(); cols[numCols++]= new FloatColumn(); }
  void addByteCol() { grow(); cols[numCols++]= new ByteColumn(); }
  
  void setI(int val, int row, int col) {((IntColumn)cols[col]).data[row]=val;}
  void setF(float val, int row, int col) { ((FloatColumn)cols[col]).data[row]=val; }
  void setB(byte val, int row, int col) { ((ByteColumn)cols[col]).data[row]=val; }

  
  // We can't use the old system as we must support reading any value from any column and 
  // do the conversion ourselves to be compatible with data adapters interface
  int getI(int row, int col) { 
    return cols[col].toInt(row);
  }
  float getF(int row, int col) {
    return cols[col].toFloat(row);
  }
  byte getB(byte row, int col) {
    return (byte)cols[col].toInt(row);
  }

}
