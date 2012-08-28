package test.analytics;

import java.io.*;
import java.util.Arrays;

import water.*;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;
import analytics.RF;

/**
 * Example of using distributed processing of csv data.
 * 
 * Executeds distributing csv parse and computation of average values of the
 * poker dataset.
 * 
 * @author tomas
 */
@RTSerializer(DRFBuilder.Serializer.class)
public class DRFBuilder extends DRemoteTask implements Cloneable {
  public static class Serializer extends RemoteTaskSerializer<DRFBuilder> {
    @Override
    public int wire_len(DRFBuilder b) {
      try {
        return (b._rf == null) ? 0 : b._rf.trees().length;
      } catch (IOException e) {
        throw new Error(e);
      }
    }

    @Override
    public int write(DRFBuilder b, byte[] buf, int off) {
      if (b._rf == null) return off;
      try {
        System.arraycopy(b._rf.trees(), 0, buf, off, b._rf.trees().length);
        return off + b._rf.trees().length;
      } catch (IOException e) {
        throw new Error(e);
      }
    }

    @Override
    public void write(DRFBuilder b, DataOutputStream dos) {
      if (b._rf == null)
        return;
      try {
        assert b._rf.trees() != null;
        dos.writeInt(b._rf.trees().length);
        dos.write(b._rf.trees());
      } catch (IOException e) {
        throw new Error(e);
      }
    }

    @Override
    public DRFBuilder read(byte[] buf, int off) {
      DRFBuilder b = new DRFBuilder();
      b._serializedRf = Arrays.copyOfRange(buf, off, buf.length);
      return b;
    }

    @Override
    public DRFBuilder read(DataInputStream dis) throws IOException {    
        DRFBuilder b = new DRFBuilder();
        int len = dis.readInt();      
        b._serializedRf = new byte[len];
        dis.readFully(b._serializedRf);             
        return b;
    }
  }

  private static final long serialVersionUID = 2113818373535413958L;
  static CSVParserSetup _setup;
  static {
    _setup = new CSVParserSetup();
    _setup._parseColumnNames = false;
    _setup._skipFirstRecord = true;
  }

  int[] _csvRecord = new int[11];
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
  public DRFBuilder clone() throws CloneNotSupportedException {
    DRFBuilder other = (DRFBuilder) super.clone();
    _csvRecord = other._csvRecord.clone();
    return other;
  }
}
