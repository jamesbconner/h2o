package test.analytics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.DRemoteTask;
import water.UDP;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.DProcessCSVTask;
import water.csv.ValueCSVRecords;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

/**
 * Example of using distributed processing of csv data.
 * 
 * Executeds distributing csv parse and computation of average values of the poker dataset.
 *  
 * @author tomas
 */
@RTSerializer(PokerAvg.Serializer.class)
public class PokerAvg extends DProcessCSVTask<int[]>{
  public static class Serializer extends RemoteTaskSerializer<PokerAvg> {
    @Override
    public int wire_len(PokerAvg p) {
      return 4 + (p._values.length * 8);
    }

    @Override
    public int write(PokerAvg p, byte[] buf, int off) {
      UDP.set4(buf, off, p._N);
      off += 4;
      for(int i = 0; i < p._values.length; ++i){
        UDP.set8(buf, off, p._values[i]);
        off += 8;    
      }
      return off;
    }

    @Override
    public void write(PokerAvg p, DataOutputStream dos) throws IOException {
      dos.writeInt(p._N);
      for(int i = 0; i < p._values.length; ++i){      
        dos.writeLong(p._values[i]);            
      }
    }

    @Override
    public PokerAvg read(byte[] buf, int off) {
      PokerAvg p = new PokerAvg();
      p._N = UDP.get4(buf, off);    
      off += 4;    
      
      for(int i = 0; i < p._values.length; ++i){
        p._values[i] = UDP.get8(buf, off);
        off += 8;
      }    
      return p;
    }

    @Override
    public PokerAvg read(DataInputStream dis) throws IOException {
      PokerAvg p = new PokerAvg();
      p._N = dis.readInt();
      for(int i = 0; i < p._values.length; ++i){      
        p._values[i] = dis.readLong();           
      }   
      return p;
    }          
  }
  
  private static final long serialVersionUID = 2113818373535413958L;
  static CSVParserSetup _setup;
  static {
    _setup = new CSVParserSetup();
    _setup._parseColumnNames = false;        
  }
  
  final long [] _values;
  
  int _N;
  
  public int N() {return _N;}
  
  public PokerAvg() {
    super(new int[11], null, _setup);
    _values = new long[11];
  }
  
  public PokerAvg(PokerAvg other) {
    super(new int[11], null, _setup);
    _values = other._values.clone();
    _N = other._N;
  }
  
  public double [] getAvg(){
    double [] result = new double[_values.length];
    double d = 1.0/(double)_N;
    for(int i = 0; i < _values.length; ++i)
      result[i] = d * (double)_values[i];
    return result;
  }
  
  protected void processRecords(ValueCSVRecords<int[]> records){
    for(int [] rec:records){
      for(int i = 0; i < rec.length; ++i) {
        _values[i] += rec[i];
      }
      ++_N;
    }
  }
    

  @Override
  public void reduce(DRemoteTask drt) {
    PokerAvg other = (PokerAvg)drt;
    for(int i = 0; i < _values.length;++i)
      _values[i] += other._values[i];
    _N += other._N;
  }

  @Override public PokerAvg clone() {
    return new PokerAvg(this);
  }
}
