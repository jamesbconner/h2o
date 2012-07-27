/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test.analytics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import analytics.Classifier;
import analytics.DataAdapter;
import analytics.Statistic;

/**
 *
 * @author peta
 */
public class StatisticTest extends Statistic {

  @Override public int dataSize() {
    // pass
    return 0;
  }

  public void addDataPoint(DataAdapter row, long[] data, int offset) {
    // pass
  }
  
  @Test public void writeLongTest() {
    long[] data = new long[2];
    assertEquals(0,data[0]);
    assertEquals(0,data[1]);
    data[0] = 67;
    data[1] = -2738;
    assertEquals(67,readLong(data,0));
    assertEquals(-2738,readLong(data,8));
    writeLong(12345,data,0);
    assertEquals(12345,readLong(data,0));
    writeLong(67890,data,8);
    assertEquals(12345,readLong(data,0));
    assertEquals(67890,readLong(data,8));
  }
  
  @Test public void writeDoubleTest() {
    long[] data = new long[2];
    assertEquals(0,data[0]);
    assertEquals(0,data[1]);
    data[0] = Double.doubleToLongBits(2.6d);
    data[1] = Double.doubleToLongBits(-1.5d);
    assertEquals(2.6d,readDouble(data,0),0.0);
    assertEquals(-1.5d,readDouble(data,8),0.0);
    writeDouble(1.5d,data,0);
    assertEquals(1.5d,readDouble(data,0),0.0);
    writeDouble(-2.5d,data,8);
    assertEquals(1.5d,readDouble(data,0),0.0);
    assertEquals(-2.5d,readDouble(data,8),0.0);
  }
  
  @Test public void writeIntTest() {
    long[] data = new long[2];
    data[0] = 0x1234567809abcdefl;
    long x = data[0] & 0xffffffffl;
    assertEquals(0x12345678,readInteger(data,0));
    assertEquals(0x9abcdef,readInteger(data,4));
    writeInteger(1234,data,0);
    writeInteger(5678,data,4);
    writeInteger(4321,data,8);
    writeInteger(-8765,data,12);
    assertEquals(1234,readInteger(data,0));
    assertEquals(5678,readInteger(data,4));
    assertEquals(4321,readInteger(data,8));
    assertEquals(-8765,readInteger(data,12));
  }
  
  @Test public void addLongTest() {
    long[] data = new long[1];
    data[0] = 67;
    addLong(3,data,0);
    assertEquals(70,readLong(data,0));
  }
  
  @Test public void addDoubleTest() {
    long[] data = new long[1];
    writeDouble(3.5,data,0);
    addDouble(2.5,data,0);
    assertEquals(6.0,readDouble(data,0),0.0);
  }
  
  @Test public void addIntegerTest() {
    long[] data = new long[1];
    writeInteger(6,data,0);
    writeInteger(31,data,4);
    addInteger(4,data,0);
    addInteger(-1,data,4);
    assertEquals(10,readInteger(data,0));
    assertEquals(30,readInteger(data,4));
  }

  public Classifier createClassifier(long[] data, int offset) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  public double fitness(long[] data, int offset) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  
}
