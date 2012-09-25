package water.serialization;

import java.io.*;
import java.lang.reflect.Method;
import java.util.Map;

import water.Key;
import water.Stream;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

public abstract class RTSerGenHelpers {
  static final Map<Class<?>, String> SUFFIX = Maps.newHashMap();
  static {
    SUFFIX.put(int.class,    "Int");
    SUFFIX.put(byte.class,   "Byte");
    SUFFIX.put(long.class,   "Long");
    SUFFIX.put(double.class, "Double");
    SUFFIX.put(int[].class,  "IntArray");
    SUFFIX.put(byte[].class, "ByteArray");
    SUFFIX.put(Key.class,    "Key");
  }

  static Method len(Class<?> c) {
    try {
      String name = "len" + SUFFIX.get(c);
      return RTSerGenHelpers.class.getMethod(name, c);
    } catch( Throwable e ) {
      throw Throwables.propagate(e);
    }
  }
  static Method read(Class<?> stream, Class<?> arg) {
    try {
      String name = "read" + SUFFIX.get(arg);
      return RTSerGenHelpers.class.getMethod(name, stream);
    } catch( Throwable e ) {
      throw Throwables.propagate(e);
    }
  }
  static Method write(Class<?> stream, Class<?> arg) {
    try {
      String name = "write" + SUFFIX.get(arg);
      return RTSerGenHelpers.class.getMethod(name, stream, arg);
    } catch( Throwable e ) {
      throw Throwables.propagate(e);
    }
  }


  public static int lenIntArray(int[] ints) {
    return 4 + 4*(ints == null ? 0 : ints.length);
  }
  public static void writeIntArray(Stream s, int[] ints) {
    if(ints == null) s.set4(-1);
    else {
      s.set4(ints.length);
      for( int i : ints ) s.set4(i);
    }
  }
  public static void writeIntArray(DataOutputStream s, int[] ints) throws IOException {
    if(ints == null) s.writeInt(-1);
    else {
      s.writeInt(ints.length);
      for( int i : ints ) s.writeInt(i);
    }
  }
  public static int[] readIntArray(Stream s) {
    int len = s.get4();
    if(len < 0) return null;
    int[] ints = new int[len];
    for(int i = 0; i < len; ++i) ints[i] = s.get4();
    return ints;
  }
  public static int[] readIntArray(DataInputStream s) throws IOException {
    int len = s.readInt();
    if(len < 0) return null;
    int[] ints = new int[len];
    for(int i = 0; i < len; ++i) ints[i] = s.readInt();
    return ints;
  }


  public static int lenByteArray(byte[] b) {
    return 4 + 4*(b == null ? 0 : b.length);
  }
  public static void writeByteArray(Stream s, byte[] bytes) {
    if(bytes == null) s.set4(-1);
    else s.setLen4Bytes(bytes);
  }
  public static byte[] readByteArray(Stream s) {
    int len = s.get4();
    if(len < 0) return null;
    byte[] b = new byte[len];
    s.getBytes(b, len);
    return b;
  }
  public static void writeByteArray(DataOutputStream s, byte[] bytes) throws IOException {
    if(bytes == null) s.writeInt(-1);
    else {
      s.writeInt(bytes.length);
      s.write(bytes);
    }
  }
  public static byte[] readByteArray(DataInputStream s) throws IOException {
    int len = s.readInt();
    if(len < 0) return null;
    byte[] bytes = new byte[len];
    s.readFully(bytes);
    return bytes;
  }

  public static int  lenByte  (                    byte b)                    { return 1;            }
  public static byte readByte (Stream s                  )                    { return s.get1();     }
  public static byte readByte (DataInputStream s         ) throws IOException { return s.readByte(); }
  public static void writeByte(Stream s,           byte b)                    { s.set1(b);           }
  public static void writeByte(DataOutputStream s, byte b) throws IOException { s.writeByte(b);      }

  public static int  lenInt  (                    int i)                      { return 4;           }
  public static int  readInt (Stream s                 )                      { return s.get4();    }
  public static int  readInt (DataInputStream s        ) throws IOException   { return s.readInt(); }
  public static void writeInt(Stream s,           int i)                      { s.set4(i);          }
  public static void writeInt(DataOutputStream s, int i) throws IOException   { s.writeInt(i);      }

  public static int  lenLong  (                    long i)                    { return 8;            }
  public static long readLong (Stream s                  )                    { return s.get8();     }
  public static long readLong (DataInputStream s         ) throws IOException { return s.readLong(); }
  public static void writeLong(Stream s,           long i)                    { s.set8(i);           }
  public static void writeLong(DataOutputStream s, long i) throws IOException { s.writeLong(i);      }

  public static int  lenKey  (                    Key k)                    { return k.wire_len(); }
  public static Key  readKey (Stream s                 )                    { return Key.read(s);  }
  public static Key  readKey (DataInputStream s        ) throws IOException { return Key.read(s);  }
  public static void writeKey(Stream s,           Key k)                    { k.write(s);          }
  public static void writeKey(DataOutputStream s, Key k) throws IOException { k.write(s);          }

  public static int    lenDouble   (                    double i)                    { return 8;              }
  public static double readDouble(Stream s                      )                    { return s.get8d();      }
  public static double readDouble(DataInputStream s             ) throws IOException { return s.readDouble(); }
  public static void   writeDouble (Stream s,           double i)                    { s.set8d(i);            }
  public static void   writeDouble (DataOutputStream s, double i) throws IOException { s.writeDouble(i);      }
}
