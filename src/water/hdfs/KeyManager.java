/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.hdfs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import water.H2ONode;
import water.Key;
import water.UDP;

/**
 *
 * @author peta
 */
public class KeyManager {
  
  public static final byte INTERNAL_KEY = 0x0;
  
  /** Creates an internal block key for given block. 
   * 
   * The internal key has the following structure:
   * 
   * offset size value
   * 0      1    HDFS_INTERNAL_BLOCK
   * 1      1/2  One replica following
   * 2      4/16 IPv4/IPv6 of the home node (HDFS namenode)
   * 6/18   8    block id
   * 
   * @param bid
   * @return 
   */
  public static Key createInternalBlockKey(long bid) {
    byte[] kb = new byte[8];
    UDP.set8(kb,0,bid);
    // Returns the key with replication factor indicating that the key should
    // not go to other node. 
    return Key.make(kb, INTERNAL_KEY,Key.HDFS_INTERNAL_BLOCK);
  }

  
  
  /** Creates an system block key for given block. 
   * 
   * The internal key has the following structure:
   * 
   * offset size value
   * 0      1    HDFS_INTERNAL_BLOCK
   * 1      1    0 (no home information)
   * 2      8    block id
   * 
   * @param bid
   * @return 
   */
  public static Key createBlockKeyInfoKey(long bid) {
    byte[] kb = new byte[8];
    UDP.set8(kb,0,bid);
    // Returns the key with replication factor indicating that the key should
    // not go to other node. 
    return Key.make(kb, INTERNAL_KEY,Key.HDFS_BLOCK_INFO);
  }
  
  /** Creates  inode key for a specified filename.
   * 
   * The inode keys are homed on the namenode
   * 
   * offset size value
   * 0      1    HDFS_INTERNAL_INODE
   * 1      1/2  One replica following
   * 2      4/16 IPv4/IPv6 of the home node (HDFS namenode)
   * 6/18   ?    Hash of the full name
   * 
   * @param name
   * @return 
   */
  public static Key createINodeKey(String name, H2ONode home) {
    byte[] kb = hashString(name);
    return Key.make(kb,/* Key.DEFAULT_DESIRED_REPLICA_FACTOR */ INTERNAL_KEY, Key.HDFS_INODE,home);
  }
  
  /** Returns the block number associated with the key. 
   * 
   * Throws an exception if the key does not hold an HDFS block key. 
   * 
   * @param key
   * @return 
   */
  public static long getBlockFromKey(Key key) {
    switch (key._kb[0]) {
      case Key.HDFS_INTERNAL_BLOCK:
      case Key.HDFS_BLOCK_INFO:
        return UDP.get8(key._kb,key._kb.length-8);
      default:
        throw new Error("Invalid key index "+(int)(key._kb[0]));
    }
  }
  
  // helpers -------------------------------------------------------------------
  
  private static byte[] hashString(String what) {
    try {
      return MessageDigest.getInstance("MD5").digest(what.getBytes());
    } catch (NoSuchAlgorithmException e) {
      // pass
      return null;
    }
  }
}
