package water.hdfs;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import water.H2O;
import water.Key;
import water.UDP;

/**
 *
 * @author peta
 */
public class BlockInfo {
  
  public static final int SIZE = 73;
  
  public final long bid;
  public final long length;
  public final long genStamp;
  public final InetAddress[] replicas;
  
  
  public static long readBid(byte[] buffer, int offset) {
    return UDP.get8(buffer,offset+65);
  }
  
  public static long readLength(byte[] buffer, int offset) {
    return UDP.get8(buffer, offset);
  }
  
  public static long readGenStamp(byte[] buffer, int offset) {
    return UDP.get8(buffer,offset+8);
  }
  
  public BlockInfo(long bid, long length, long genStamp) {
    this.bid = bid;
    this.length = length;
    this.genStamp = genStamp;
    replicas = null;
  }
  
  public BlockInfo(DataInputStream is) throws IOException {
    bid = is.readLong();
    length = is.readLong();
    // PETA
    genStamp = 0;
//    genStamp = (ValueINode._hdfs_imgVersion <=-14) ? is.readLong() : 0 ;
    replicas = null;
  }
  
  public BlockInfo(byte[] buffer, int offset) {
    length = readLength(buffer,offset);
    genStamp = readGenStamp(buffer,offset);
    throw H2O.unimpl();
    //replicas = Key.readReplicaRecord(buffer, offset+16);
    //bid = readBid(buffer,offset);
  }
  
  public void store(byte[] buffer, int offset) {
    throw H2O.unimpl();
    //byte[] r = (replicas==null) ? Key.makeReplicaRecord(new InetAddress[0]) : Key.makeReplicaRecord(replicas);
    //assert (buffer.length>=offset+72);
    //UDP.set8(buffer,offset,length);
    //UDP.set8(buffer,offset+8,genStamp);
    //System.arraycopy(r,0,buffer,offset+16,r.length);
    //UDP.set8(buffer,offset+65,bid);
  }
  
}
