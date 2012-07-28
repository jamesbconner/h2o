package test.analytics;



public class DataChunck {
  
  class Column {}

  class IntColumn extends Column{  int[] data;  IntColumn(){data=new int[1000];} }
  class FloatColumn extends Column { float[] data; FloatColumn(){data=new float[1000];} }
  class ByteColumn extends Column { byte[] data; ByteColumn(){data=new byte[1000];}}
  
  Column[] cols = new Column[100]; //fix
  int numCols;
  
  void addIntCol() { cols[numCols++]= new IntColumn(); }
  void addFloatCol() { cols[numCols++]= new FloatColumn(); }
  void addByteCol() { cols[numCols++]= new ByteColumn(); }
  
  void setI(int val, int row, int col) { ((IntColumn)cols[col]).data[row]=val; }
  void setF(float val, int row, int col) { ((FloatColumn)cols[col]).data[row]=val; }
  void setB(byte val, int row, int col) { ((ByteColumn)cols[col]).data[row]=val; }

  int getI(int row, int col) { return ((IntColumn)cols[col]).data[row]; }
  float getF(int row, int col) { return ((FloatColumn)cols[col]).data[row]; }
  byte getB(byte row, int col) { return ((ByteColumn)cols[col]).data[row]; }
  
}
