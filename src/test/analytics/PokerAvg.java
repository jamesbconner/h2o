package test.analytics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.RemoteTask;
import water.UDP;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.DProcessCSVTask;

public class PokerAvg extends DProcessCSVTask<int[]>{
  
  /**
   * 
   */
  private static final long serialVersionUID = 2113818373535413958L;
  
  long [] _values = new long[11];
  
  int _N;
  
  public int N() {return _N;}
  static CSVParserSetup _setup;
  
  static {
    _setup = new CSVParserSetup();
    _setup._parseColumnNames = false;
  }
  
  public PokerAvg() throws NoSuchFieldException, SecurityException{
    super(new int[11], null, _setup);
  }

  
  public double [] getAvg(){
    double [] result = new double[_values.length];
    System.out.println("N = " + _N);
    double d = 1.0/(double)_N;
    for(int i = 0; i < _values.length; ++i)
      result[i] = d * (double)_values[i];
    return result;
  }
  @Override
  protected void processRecord(int [] csvRecord) {    
    for(int i = 0; i < csvRecord.length;++i){
      _values[i] += csvRecord[i];      
    }
    ++_N;    
  }

  @Override
  public void reduce(RemoteTask drt) {
    PokerAvg other = (PokerAvg)drt;
    for(int i = 0; i < _values.length;++i)
      _values[i] += other._values[i];
    _N += other._N;
    System.out.println("reduce: " + _N + " records processed");
  }

  @Override
  protected int wire_len() {
    return 4 + (_values.length << 3);
  }

  @Override
  protected int write(byte[] buf, int off) {
    UDP.set4(buf, off, _N);
    off += 4;
    for(int i = 0; i < _values.length; ++i){
      UDP.set8(buf, off, _values[i]);
      off += 8;
    }
    return wire_len();
  }

  @Override
  protected void write(DataOutputStream dos) {
    try {
      dos.writeInt(_N);
      for(int i = 0; i < _values.length; ++i){      
        dos.writeLong(_values[i]);            
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new Error(e);
    }
  }

  @Override
  protected void read(byte[] buf, int off) {
    _N = UDP.get4(buf, off);
    off += 4;
    for(int i = 0; i < _values.length; ++i){
      _values[i] = UDP.get8(buf, off);
      off += 8;
    }    
  }

  @Override
  protected void read(DataInputStream dis) {
    try {
      _N = dis.readInt();
      for(int i = 0; i < _values.length; ++i){      
        _values[i] = dis.readLong();           
      }   
    } catch (IOException e) {
      e.printStackTrace();
      throw new Error(e);
    }
  }          
}
