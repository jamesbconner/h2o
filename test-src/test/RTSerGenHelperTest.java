package test;

import java.io.*;
import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.serialization.RTSerGenHelpers;

public class RTSerGenHelperTest {
  @Test public void testCoverage() throws Exception {
    for(String s : RTSerGenHelpers.SUFFIX.values()) {
      Method m = RTSerGenHelperTest.class.getMethod("test" + s);
      Assert.assertNotNull(m);
    }
  }

  private ByteArrayOutputStream _bos;
  private Stream _stream = new Stream();

  private DataOutputStream dos() {
    _bos = new ByteArrayOutputStream();
    return new DataOutputStream(_bos);
  }

  private DataInputStream dis() {
    return new DataInputStream(new ByteArrayInputStream(_bos.toByteArray()));
  }

  private Stream stream() {
    return _stream = new Stream(_stream._buf);
  }

  @Test public void testByte() throws Exception {
    byte[] tests = { 0, 4, -1, 127, -128 };
    byte got;
    for( byte exp : tests) {
      RTSerGenHelpers.writeByte(stream(), exp);
      got = RTSerGenHelpers.readByte(stream());
      Assert.assertEquals(exp, got);

      RTSerGenHelpers.writeByte(dos(), exp);
      got = RTSerGenHelpers.readByte(dis());
      Assert.assertEquals(exp, got);
    }
  }

  @Test public void testShort() throws Exception {
    short[] tests = { 0, 4, -1, 127, -128 };
    short got;
    for( short exp : tests) {
      RTSerGenHelpers.writeShort(stream(), exp);
      got = RTSerGenHelpers.readShort(stream());
      Assert.assertEquals(exp, got);

      RTSerGenHelpers.writeShort(dos(), exp);
      got = RTSerGenHelpers.readShort(dis());
      Assert.assertEquals(exp, got);
    }
  }

  @Test public void testInt() throws Exception {
    int[] tests = { 0, 4, Integer.MAX_VALUE, Integer.MIN_VALUE, -1 };
    int got;
    for( int exp : tests) {
      RTSerGenHelpers.writeInt(stream(), exp);
      got = RTSerGenHelpers.readInt(stream());
      Assert.assertEquals(exp, got);

      RTSerGenHelpers.writeInt(dos(), exp);
      got = RTSerGenHelpers.readInt(dis());
      Assert.assertEquals(exp, got);
    }
  }

  @Test public void testLong() throws Exception {
    long[] tests = { 0, 4, Integer.MAX_VALUE, Integer.MIN_VALUE, -1,
        Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - Integer.MAX_VALUE
    };
    long got;
    for( long exp : tests) {
      RTSerGenHelpers.writeLong(stream(), exp);
      got = RTSerGenHelpers.readLong(stream());
      Assert.assertEquals(exp, got);

      RTSerGenHelpers.writeLong(dos(), exp);
      got = RTSerGenHelpers.readLong(dis());
      Assert.assertEquals(exp, got);
    }
  }

  @Test public void testFloat() throws Exception {
    float[] tests = { 0, 4, Integer.MAX_VALUE, Integer.MIN_VALUE, -1,
        Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - Integer.MAX_VALUE,
        Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY
    };
    float got;
    for( float exp : tests) {
      RTSerGenHelpers.writeFloat(stream(), exp);
      got = RTSerGenHelpers.readFloat(stream());
      Assert.assertEquals(exp, got, 0.0);

      RTSerGenHelpers.writeFloat(dos(), exp);
      got = RTSerGenHelpers.readFloat(dis());
      Assert.assertEquals(exp, got, 0.0);
    }
  }

  @Test public void testDouble() throws Exception {
    double[] tests = { 0, 4, Integer.MAX_VALUE, Integer.MIN_VALUE, -1,
        Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - Integer.MAX_VALUE,
        Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY
    };
    double got;
    for( double exp : tests) {
      RTSerGenHelpers.writeDouble(stream(), exp);
      got = RTSerGenHelpers.readDouble(stream());
      Assert.assertEquals(exp, got, 0.0);

      RTSerGenHelpers.writeDouble(dos(), exp);
      got = RTSerGenHelpers.readDouble(dis());
      Assert.assertEquals(exp, got, 0.0);
    }
  }

  @Test public void testKey() throws Exception {
    H2O.main(new String[0]);
    Key[] tests = { Key.make(), Key.make("monkey"), Key.make("ninja"), null };
    Key got;
    for( Key exp : tests) {
      RTSerGenHelpers.writeKey(stream(), exp);
      got = RTSerGenHelpers.readKey(stream());
      Assert.assertEquals(exp, got);

      RTSerGenHelpers.writeKey(dos(), exp);
      got = RTSerGenHelpers.readKey(dis());
      Assert.assertEquals(exp, got);
    }
  }

  @Test public void testByteArray() throws Exception {
    byte[][] tests = {
        { 0, 1, 2 },
        { },
        null,
        { 6, -1, 19, -49 }
    };
    byte[] got;

    for( byte[] exp : tests) {
      RTSerGenHelpers.writeByteArray(stream(), exp);
      got = RTSerGenHelpers.readByteArray(stream());
      Assert.assertArrayEquals(exp, got);

      RTSerGenHelpers.writeByteArray(dos(), exp);
      got = RTSerGenHelpers.readByteArray(dis());
      Assert.assertArrayEquals(exp, got);
    }
  }

  @Test public void testShortArray() throws Exception {
    short[][] tests = {
        { 0, 1, 2 },
        { },
        null,
        { 6, -1, 19, -49, Short.MAX_VALUE }
    };
    short[] got;

    for( short[] exp : tests) {
      RTSerGenHelpers.writeShortArray(stream(), exp);
      got = RTSerGenHelpers.readShortArray(stream());
      Assert.assertArrayEquals(exp, got);

      RTSerGenHelpers.writeShortArray(dos(), exp);
      got = RTSerGenHelpers.readShortArray(dis());
      Assert.assertArrayEquals(exp, got);
    }
  }

  @Test public void testIntArray() throws Exception {
    int[][] tests = new int[][] {
        { 0, 1, 2 },
        { },
        null,
        { 6, Integer.MAX_VALUE, -1, 19, -49 }
    };
    int[] got;

    for( int[] exp : tests) {
      RTSerGenHelpers.writeIntArray(stream(), exp);
      got = RTSerGenHelpers.readIntArray(stream());
      Assert.assertArrayEquals(exp, got);

      RTSerGenHelpers.writeIntArray(dos(), exp);
      got = RTSerGenHelpers.readIntArray(dis());
      Assert.assertArrayEquals(exp, got);
    }
  }

  @Test public void testLongArray() throws Exception {
    long[][] tests = {
        { 0, 1, 2 },
        { },
        null,
        { 6, -1, 19, -49 },
        { Long.MAX_VALUE, Long.MIN_VALUE}
    };
    long[] got;

    for( long[] exp : tests) {
      RTSerGenHelpers.writeLongArray(stream(), exp);
      got = RTSerGenHelpers.readLongArray(stream());
      Assert.assertArrayEquals(exp, got);

      RTSerGenHelpers.writeLongArray(dos(), exp);
      got = RTSerGenHelpers.readLongArray(dis());
      Assert.assertArrayEquals(exp, got);
    }
  }

  @Test public void testFloatArray() throws Exception {
    float[][] tests = {
        { 0, 1, 2 },
        { },
        null,
        { 6, -1, 19, -49 },
        { Float.MAX_VALUE, Float.MIN_VALUE}
    };
    float[] got;

    for( float[] exp : tests) {
      RTSerGenHelpers.writeFloatArray(stream(), exp);
      got = RTSerGenHelpers.readFloatArray(stream());
      Assert.assertArrayEquals(exp, got, 0.0f);

      RTSerGenHelpers.writeFloatArray(dos(), exp);
      got = RTSerGenHelpers.readFloatArray(dis());
      Assert.assertArrayEquals(exp, got, 0.0f);
    }
  }

  @Test public void testDoubleArray() throws Exception {
    double[][] tests = {
        { 0, 1, 2 },
        { },
        null,
        { 6, -1, 19, -49 },
        { Double.MAX_VALUE, Double.MIN_VALUE}
    };
    double[] got;

    for( double[] exp : tests) {
      RTSerGenHelpers.writeDoubleArray(stream(), exp);
      got = RTSerGenHelpers.readDoubleArray(stream());
      Assert.assertArrayEquals(exp, got, 0.0);

      RTSerGenHelpers.writeDoubleArray(dos(), exp);
      got = RTSerGenHelpers.readDoubleArray(dis());
      Assert.assertArrayEquals(exp, got, 0.0);
    }
  }
}
