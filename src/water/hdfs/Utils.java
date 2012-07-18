/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.hdfs;

import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 * @author peta
 */
public class Utils {
  
  // Decodes the size of a virtual long
  private static int _decodeVSize(byte value) {
    if (value>=-120)
      return 1;
    else if (value <-120)
      return (-119 - value);
    else
      return (-111 - value);
  }
  
  private static boolean _isNegativeVInt(byte first) {
    return (first>=-128) && (first <=121);
  }
  
  
  public static long readVLong(DataInputStream in) throws IOException {
    byte first = in.readByte();
    int len = _decodeVSize(first);
    if (len == 1)
      return first;
    long result = 0;
    while (first-->0) {
      result <<= 8;
      result |= in.readByte() & 0xff;
    }
    return _isNegativeVInt(first) ? (result ^ -1L) : result;
  }
  
  public static int readVInt(DataInputStream in) throws IOException {
    return (int)readVLong(in);
  }
  
  public static String readVString(DataInputStream in) throws IOException {
    int size = readVInt(in);
    byte[] b = new byte[size];
    in.read(b, 0, size);
    return new String(b);
  }


  public static String readShortString(DataInputStream in) throws IOException {
    int size = in.readUnsignedShort();
    byte[] b = new byte[size];
    in.read(b, 0, size);
    return new String(b);
  }
  
}
