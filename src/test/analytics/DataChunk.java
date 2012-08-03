package test.analytics;



public class DataChunk {
  
  static final int SIZE = 100 * 1000;
  class Column {}

  class IntColumn extends Column{  int[] data;  IntColumn(){data=new int[SIZE];} }
  class FloatColumn extends Column { float[] data; FloatColumn(){data=new float[SIZE];} }
  class ByteColumn extends Column { byte[] data; ByteColumn(){data=new byte[SIZE];}}
  
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
  
  void setI(int val, int row, int col) { ((IntColumn)cols[col]).data[row]=val; }
  void setF(float val, int row, int col) { ((FloatColumn)cols[col]).data[row]=val; }
  void setB(byte val, int row, int col) { ((ByteColumn)cols[col]).data[row]=val; }

  int getI(int row, int col) { return ((IntColumn)cols[col]).data[row]; }
  float getF(int row, int col) { return ((FloatColumn)cols[col]).data[row]; }
  byte getB(byte row, int col) { return ((ByteColumn)cols[col]).data[row]; }
  
}
