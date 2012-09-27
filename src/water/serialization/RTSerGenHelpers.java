package water.serialization;

import java.io.*;
import java.lang.reflect.Method;
import java.util.Map;

import water.Key;
import water.Stream;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

public abstract class RTSerGenHelpers {
  public static final Map<Class<?>, String> SUFFIX = Maps.newHashMap();
  static {
    SUFFIX.put(byte.class,   "Byte");
    SUFFIX.put(short.class,  "Short");
    SUFFIX.put(int.class,    "Int");
    SUFFIX.put(long.class,   "Long");
    SUFFIX.put(float.class,  "Float");
    SUFFIX.put(double.class, "Double");
    SUFFIX.put(Key.class,    "Key");

    SUFFIX.put(byte[].class,   "ByteArray");
    SUFFIX.put(short[].class,  "ShortArray");
    SUFFIX.put(int[].class,    "IntArray");
    SUFFIX.put(long[].class,   "LongArray");
    SUFFIX.put(float[].class,  "FloatArray");
    SUFFIX.put(double[].class, "DoubleArray");

    SUFFIX.put(long[][].class,   "LongArrayArray");
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


  public static int lenShortArray(short[] shorts) {
    return 4 + 2*(shorts == null ? 0 : shorts.length);
  }
  public static void writeShortArray(Stream s, short[] shorts) {
    if(shorts == null) s.set4(-1);
    else {
      s.set4(shorts.length);
      for( short i : shorts ) s.set2(i);
    }
  }
  public static void writeShortArray(DataOutputStream s, short[] shorts) throws IOException {
    if(shorts == null) s.writeInt(-1);
    else {
      s.writeInt(shorts.length);
      for( short i : shorts ) s.writeShort(i);
    }
  }
  public static short[] readShortArray(Stream s) {
    int len = s.get4();
    if(len < 0) return null;
    short[] shorts = new short[len];
    for(int i = 0; i < len; ++i) shorts[i] = (short) s.get2();
    return shorts;
  }
  public static short[] readShortArray(DataInputStream s) throws IOException {
    int len = s.readInt();
    if(len < 0) return null;
    short[] shorts = new short[len];
    for(int i = 0; i < len; ++i) shorts[i] = s.readShort();
    return shorts;
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


  public static int lenLongArray(long[] longs) {
    return 4 + 8*(longs == null ? 0 : longs.length);
  }
  public static void writeLongArray(Stream s, long[] longs) {
    if(longs == null) s.set4(-1);
    else {
      s.set4(longs.length);
      for( long i : longs ) s.set8(i);
    }
  }
  public static void writeLongArray(DataOutputStream s, long[] longs) throws IOException {
    if(longs == null) s.writeInt(-1);
    else {
      s.writeInt(longs.length);
      for( long i : longs ) s.writeLong(i);
    }
  }
  public static long[] readLongArray(Stream s) {
    int len = s.get4();
    if(len < 0) return null;
    long[] longs = new long[len];
    for(int i = 0; i < len; ++i) longs[i] = s.get8();
    return longs;
  }
  public static long[] readLongArray(DataInputStream s) throws IOException {
    int len = s.readInt();
    if(len < 0) return null;
    long[] longs = new long[len];
    for(int i = 0; i < len; ++i) longs[i] = s.readLong();
    return longs;
  }


  public static int lenFloatArray(float[] floats) {
    return 4 + 4*(floats == null ? 0 : floats.length);
  }
  public static void writeFloatArray(Stream s, float[] floats) {
    if(floats == null) s.set4(-1);
    else {
      s.set4(floats.length);
      for( float i : floats ) s.set4f(i);
    }
  }
  public static void writeFloatArray(DataOutputStream s, float[] floats) throws IOException {
    if(floats == null) s.writeInt(-1);
    else {
      s.writeInt(floats.length);
      for( float i : floats ) s.writeFloat(i);
    }
  }
  public static float[] readFloatArray(Stream s) {
    int len = s.get4();
    if(len < 0) return null;
    float[] floats = new float[len];
    for(int i = 0; i < len; ++i) floats[i] = s.get4f();
    return floats;
  }
  public static float[] readFloatArray(DataInputStream s) throws IOException {
    int len = s.readInt();
    if(len < 0) return null;
    float[] floats = new float[len];
    for(int i = 0; i < len; ++i) floats[i] = s.readFloat();
    return floats;
  }


  public static int lenDoubleArray(double[] doubles) {
    return 4 + 8*(doubles == null ? 0 : doubles.length);
  }
  public static void writeDoubleArray(Stream s, double[] doubles) {
    if(doubles == null) s.set4(-1);
    else {
      s.set4(doubles.length);
      for( double i : doubles ) s.set8d(i);
    }
  }
  public static void writeDoubleArray(DataOutputStream s, double[] doubles) throws IOException {
    if(doubles == null) s.writeInt(-1);
    else {
      s.writeInt(doubles.length);
      for( double i : doubles ) s.writeDouble(i);
    }
  }
  public static double[] readDoubleArray(Stream s) {
    int len = s.get4();
    if(len < 0) return null;
    double[] doubles = new double[len];
    for(int i = 0; i < len; ++i) doubles[i] = s.get8d();
    return doubles;
  }
  public static double[] readDoubleArray(DataInputStream s) throws IOException {
    int len = s.readInt();
    if(len < 0) return null;
    double[] doubles = new double[len];
    for(int i = 0; i < len; ++i) doubles[i] = s.readDouble();
    return doubles;
  }

  public static int lenLongArrayArray(long[][] longs) {
    int len = 4;
    if( longs != null ) for( long[] l : longs ) len += lenLongArray(l);
    return len;
  }
  public static void writeLongArrayArray(Stream s, long[][] longs) {
    if(longs == null) s.set4(-1);
    else {
      s.set4(longs.length);
      for( long[] l : longs ) writeLongArray(s, l);
    }
  }
  public static void writeLongArrayArray(DataOutputStream s, long[][] longs) throws IOException {
    if(longs == null) s.writeInt(-1);
    else {
      s.writeInt(longs.length);
      for( long[] l : longs ) writeLongArray(s, l);
    }
  }
  public static long[][] readLongArrayArray(Stream s) {
    int len = s.get4();
    if(len < 0) return null;
    long[][] longs = new long[len][];
    for(int i = 0; i < len; ++i) longs[i] = readLongArray(s);
    return longs;
  }
  public static long[][] readLongArrayArray(DataInputStream s) throws IOException {
    int len = s.readInt();
    if(len < 0) return null;
    long[][] longs = new long[len][];
    for(int i = 0; i < len; ++i) longs[i] = readLongArray(s);
    return longs;
  }



  public static int lenKey(Key k) { return 1 + (k == null ? 0 : k.wire_len()); }
  public static Key readKey(Stream s) {
    if( s.get1() == 0) return null;
    return Key.read(s);
  }
  public static Key readKey(DataInputStream s) throws IOException {
    if( s.readByte() == 0) return null;
    return Key.read(s);
  }
  public static void writeKey(Stream s, Key k) {
    if( k == null ) s.set1(0);
    else {
      s.set1(1);
      k.write(s);
    }
  }
  public static void writeKey(DataOutputStream s, Key k) throws IOException {
    if( k == null ) s.writeByte(0);
    else {
      s.writeByte(1);
      k.write(s);
    }
  }

  public static int    lenByte    (                    byte b  )                    { return 1;                 }
  public static byte   readByte   (Stream s                    )                    { return s.get1();          }
  public static byte   readByte   (DataInputStream s           ) throws IOException { return s.readByte();      }
  public static void   writeByte  (Stream s,           byte b  )                    { s.set1(b);                }
  public static void   writeByte  (DataOutputStream s, byte b  ) throws IOException { s.writeByte(b);           }

  public static int    lenShort   (                    short i )                    { return 2;                 }
  public static short  readShort  (Stream s                    )                    { return (short) s.get2();  }
  public static short  readShort  (DataInputStream s           ) throws IOException { return s.readShort();     }
  public static void   writeShort (Stream s,           short i )                    { s.set2(i);                }
  public static void   writeShort (DataOutputStream s, short i ) throws IOException { s.writeShort(i);          }

  public static int    lenInt     (                    int i   )                    { return 4;                 }
  public static int    readInt    (Stream s                    )                    { return s.get4();          }
  public static int    readInt    (DataInputStream s           ) throws IOException { return s.readInt();       }
  public static void   writeInt   (Stream s,           int i   )                    { s.set4(i);                }
  public static void   writeInt   (DataOutputStream s, int i   ) throws IOException { s.writeInt(i);            }

  public static int    lenLong    (                    long i  )                    { return 8;                 }
  public static long   readLong   (Stream s                    )                    { return s.get8();          }
  public static long   readLong   (DataInputStream s           ) throws IOException { return s.readLong();      }
  public static void   writeLong  (Stream s,           long i  )                    { s.set8(i);                }
  public static void   writeLong  (DataOutputStream s, long i  ) throws IOException { s.writeLong(i);           }

  public static int    lenFloat   (                    float i )                    { return 4;                 }
  public static float  readFloat  (Stream s                    )                    { return s.get4f();         }
  public static float  readFloat  (DataInputStream s           ) throws IOException { return s.readFloat();     }
  public static void   writeFloat (Stream s,           float i )                    { s.set4f(i);               }
  public static void   writeFloat (DataOutputStream s, float i ) throws IOException { s.writeFloat(i);          }

  public static int    lenDouble  (                    double i)                    { return 8;                 }
  public static double readDouble (Stream s                    )                    { return s.get8d();         }
  public static double readDouble (DataInputStream s           ) throws IOException { return s.readDouble();    }
  public static void   writeDouble(Stream s,           double i)                    { s.set8d(i);               }
  public static void   writeDouble(DataOutputStream s, double i) throws IOException { s.writeDouble(i);         }
}
