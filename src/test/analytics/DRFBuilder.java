package test.analytics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import water.DKV;
import water.DRemoteTask;
import water.H2O;
import water.Key;
import water.RemoteTask;
import water.UDP;
import water.Value;
import water.ValueArray;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;
import analytics.RF;

/**
 * Example of using distributed processing of csv data.
 * 
 * Executeds distributing csv parse and computation of average values of the
 * poker dataset.
 * 
 * @author tomas
 * 
 */
public class DRFBuilder extends DRemoteTask {

  /**
   * 
   */
  private static final long serialVersionUID = 2113818373535413958L;

  int[] _csvRecord = new int[11];

  static CSVParserSetup _setup;

  static {
    _setup = new CSVParserSetup();
    _setup._parseColumnNames = false;
    _setup._skipFirstRecord = true;
  }

  RF _rf;
  
  public RF getRf() {return _rf;}
  byte[] _serializedRf;
  
  Key _k;

  @Override
  public void map(Key key) {
    System.out.println("running map on " + H2O.SELF);
    try {
      Value v = DKV.get(key);
      if (v == null)
        throw new Error("did not find key " + key);
      byte[] mem = v.mem();
      int filenameLen = UDP.get4(mem, 0);
      String filename = new String(mem, 4, filenameLen);
      int fileOffset = UDP.get4(mem, 4 + filenameLen);
      int dataLength = UDP.get4(mem, 4 + filenameLen + 4);
      int ntrees = UDP.get4(mem, 4 + filenameLen + 4 + 4);
      File f = new File(filename);
      ValueCSVRecords<int[]> records = new ValueCSVRecords<int[]>(
          new ValueCSVRecords.StreamDataProvider(1 << ValueArray.LOG_CHK,
              new FileInputStream(f), fileOffset, dataLength), _csvRecord,
          null, _setup);
      PokerChunkedAdapter adapter = new PokerChunkedAdapter();
      int nrows = 0;
      for (int[] rec : records) {        
        adapter.addRow(rec);
        ++nrows;
      }      
      System.out.println("parsed " + nrows + "rows");
      _rf = new RF(adapter, ntrees);
      _rf.compute();
      System.out.println(H2O.SELF + " done: " + _rf);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  @Override
  public void reduce(DRemoteTask drt) {
    System.out.println("running reduce on " + H2O.SELF);
    DRFBuilder other = (DRFBuilder) drt;
    try {
      if (_rf != null && other._serializedRf != null) {
        _rf.combine(other._serializedRf);
        other._serializedRf = null;
      } else {
        other._rf.combine(_serializedRf);
        _rf = other._rf;
        _serializedRf = null;
      }
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  @Override
  protected int wire_len() {
    try {
      return (_rf == null) ? 0 : _rf.trees().length;
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  @Override
  protected int write(byte[] buf, int off) {
    if (_rf == null)
      return off;
    try {
      System.arraycopy(_rf.trees(), 0, buf, off, _rf.trees().length);
      return off + _rf.trees().length;
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  @Override
  protected void write(DataOutputStream dos) {
    if (_rf == null)
      return;
    try {
      assert _rf.trees() != null;
      dos.writeInt(_rf.trees().length);
      dos.write(_rf.trees());
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  @Override
  protected void read(byte[] buf, int off) {
    _serializedRf = Arrays.copyOfRange(buf, off, buf.length);
  }

  @SuppressWarnings("unused")
  @Override
  protected void read(DataInputStream dis) {    
    try {
      int len = dis.readInt();      
      _serializedRf = new byte[len];
      dis.readFully(_serializedRf);             
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  @Override
  public Object clone() {
    try {
      Object o = super.clone();
      DRFBuilder other = (DRFBuilder) o;
      _csvRecord = other._csvRecord.clone();
      return other;
    } catch (Exception e) {
      throw new Error(e);
    }
  }
}
