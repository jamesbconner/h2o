/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.hdfs;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

/** Hadoop secret manager. 
 * 
 * Does not do that much now, only stores the data for now. 
 *
 * @author peta
 */
public class SecretManager {

  int _id;
  ArrayList<DelegationKey> _keys = new ArrayList();
  int _delegationTokenSequenceNumber;
  ArrayList<DelegationToken> _tokens = new ArrayList();

  public SecretManager(DataInputStream in) throws IOException {
    _id = in.readInt();
    int numKeys = in.readInt();
    while (numKeys-->0)
      _keys.add(new DelegationKey(in));
    _delegationTokenSequenceNumber = in.readInt();
    int numTokens = in.readInt();
    while (numTokens-->0)
      _tokens.add(new DelegationToken(in));
  }

  /** Delegation key for the manager. 
    * 
    * At the moment a silly object.
    */
  public static class DelegationKey {
    public int id;
    public long expiryDate;
    public byte[] bytes;
    public DelegationKey(DataInputStream in) throws IOException {
      id = in.readInt();
      expiryDate = in.readLong();
      int numBytes = in.readInt();
      if (numBytes == -1) {
        bytes = null;
      } else {
        bytes = new byte[numBytes];
        in.read(bytes, 0, numBytes);
      }
    }
  }

  public static class DelegationToken {
    public byte version;
    public String owner;
    public String renewer;
    public String realUser;
    public long issueDate;
    public long maxDate;
    public int sequenceNumber;
    public int masterKeyId;
    public long expiryTime;
    public DelegationToken(DataInputStream in) throws IOException {
      version = in.readByte();
      if (version!=0)
        throw new IOException("Incompatible delegation token version in the fsimage.");
      owner = Utils.readVString(in);
      renewer = Utils.readVString(in);
      realUser = Utils.readVString(in);
      issueDate = in.readLong();
      maxDate = in.readLong();
      sequenceNumber = in.readInt();
      masterKeyId = in.readInt();
      expiryTime = in.readLong();
    }

  }
    
  
}
