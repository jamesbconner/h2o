package water;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


/**
 * A memory-efficient version of vector clocks
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public final class VectorClock {

  // Classic Vector Clocks, with a few extensions.

  // A cookie for the Vector Clock formed by a vector of all the clocks from
  // all the H2ONodes we have ever seen.  The actual clocks are stored in the
  // H2ONode structures.
  static public final VectorClock NOW = new VectorClock();

  // Main extension is a "weak clock" - just orders operations on *this* Node
  // efficiently, and is used to order weak put/gets.  A "weak clock" has only
  // the clock for this Node and has a zero vector for other Nodes.  The "weak
  // clock" for Node2/clock=17 is {0,0,17,0,0...}.  It is represented by the
  // hashCode for my InetAddress and my local clock in a 8-byte long.
  //
  // The combination of a VectorClock structure and the "weak clock" long
  // represents the full VectorClock, and is inlined into Value objects.
  long weak_long() {
    assert_now();
    return (((long)H2O.SELF._key._inet.hashCode())<<32) | 
      ((long)H2O.SELF._clock.get() & 0xFFFFFFFFL);
  }
  public VectorClock weak_vc() {
    assert_now();
    return NOW;
  }
  static final int LOCALHOST = /*127.0.0.1*/((127<<24)+(0<<16)+(0<<8)+(1<<0));
  public static long weak_jvmboot_time() {
    return ((long)LOCALHOST<<32) | 0/*==(Value.ON_LOCAL_DISK.vcl())*/;
  }
  static long before_time() {
    return ((long)LOCALHOST<<32) | (0xFFFFFFFFL);
  }

  // Morally a 2-input 'equals' call
  public boolean same_clock( long weak1, VectorClock vc2, long weak2 ) {
    if( this == vc2 ) return weak1 == weak2; // Identical timestamps?
    throw new Error("unimplemented");
  }

  // Happens-Before!!!

  // Is {this,weak} < {vc2,weak} ?
  boolean happens_before( long weak1, VectorClock vc2, long weak2 ) {
    VectorClock vc1 = this;
    vc1.assert_now();
    vc2.assert_now();

    // Both are "now" clocks, plus perhaps a weak order.  "Now" clocks are kept
    // in the H2ONode structure.  Weak-clocks are essentially all-zero clocks
    // except for the one named Node, and are used to weakly order e.g. PUT
    // operations FROM THE SAME NODE.
    if( weak1 == weak2 ) return false; // Tied, not before
    // We pull only the weak order bits from the "now" clock & compare.
    int adr1 = (int)(weak1>>32);
    int clk1 = (int)(weak1>> 0);
    int adr2 = (int)(weak2>>32);
    int clk2 = (int)(weak2>> 0);
    // If tied on Node, we only need to compare the clocks from that Node
    if( adr1 == adr2 ) return clk1 < clk2; // Only order is from same Node?
    if( adr1 == LOCALHOST ) return true ;  // Adr1 is LOCALHOST/0, from JVM-boot?
    if( adr2 == LOCALHOST ) return false;  // Adr2 is LOCALHOST/0, from JVM-boot?
    // If one or the other is weak and the other is NOW, just check the most
    // recent Node clock.
    throw new Error("unimplemented");
    //if( adr1 != 0 && weak2 == 0 )
    //  return clk1 < H2ONode.intern(adr1)._clock.get();
    //if( weak1 == 0 && adr2 != 0 )
    //  return H2ONode.intern(adr2)._clock.get() < clk2;
    // Weak bits are from different clocks, making them NOT ordered
    //return false;
  }

  boolean happens_before( Value val ) { return happens_before(weak_long(),val.vc(),val.vcl());  }

  // Wireline version of vector clock: done to avoid actually making a VC for
  // common/cheap cases.
  public boolean happens_before( long weak1, byte[] buf, int off ) {
    long weak2 = read8(buf,off);
    if( (weak1>>32) == 0 ) throw new Error("unimplemented");
    if( (weak2>>32) == 0 ) throw new Error("unimplemented");
    // This is an 8-byte weak clock
    return happens_before(weak1,NOW,weak2);
  }
  public boolean same_clock( long weak1, byte[] buf, int off ) {
    long weak2 = read8(buf,off);
    if( (weak1>>32) == 0 ) throw new Error("unimplemented");
    if( (weak2>>32) == 0 ) throw new Error("unimplemented");
    // This is an 8-byte weak clock
    return same_clock(weak1,NOW,weak2);
  }

  // Apply this VectorClock to max across vectors
  public void apply_max_vector_clock( long weak ) { 
    assert_now();
    int adr1 = (int)(weak>>32);
    int clk1 = (int)(weak>> 0);
    throw new Error("unimplemented");
    //H2ONode h2o = H2ONode.intern(adr1);
    //int clk = h2o._clock.get();
    //while( clk1 > clk && !h2o._clock.compareAndSet(clk,clk1) )
    //  clk = h2o._clock.get();
  }

  // Reset clock for Node h2o to zero.
  public long reset_weak( H2ONode h2o, long weak ) {
    assert_now();
    int adr1 = (int)(weak>>32);
    if( adr1 != h2o.ip4() ) return weak;
    return ((long)adr1)<<32;
  }
  // Reset clock for Node h2o to zero.
  public VectorClock reset( H2ONode h2o ) {
    assert_now();
    return this;
  }

  // Dense line-wire format
  int wire_len( long weak, H2ONode h2o ) {
    assert_now();
    assert (weak>>32) != 0;     // First 4 bytes not-nulls a "weak clock"
    return 8;
  }

  int write(long weak, byte[] buf, int off, H2ONode h2o ) {
    assert_now();
    assert (weak>>32) != 0;     // First 4 bytes not-nulls a "weak clock"
    return off+UDP.set8(buf,off,weak);
  }
  void write(long weak, DataOutputStream dos, H2ONode h2o ) throws IOException {
    assert_now();
    assert (weak>>32) != 0;     // First 4 bytes not-nulls a "weak clock"
    dos.writeLong(weak);
  }

  // Read dense wire-line protocol
  static long read8( byte[] buf, int off ) {
    return UDP.get8(buf,off);  
  }
  static long read8( DataInputStream dis ) throws IOException {
    return dis.readLong(); 
  }
  static VectorClock read( long weak, byte[] buf, int off, H2ONode h2o ) {
    if( (weak>>32) == 0 ) throw new Error("unimplemented");
    return NOW;
  }
  static VectorClock read( long weak, DataInputStream dis, H2ONode h2o ) {
    if( (weak>>32) == 0 ) throw new Error("unimplemented");
    return NOW;
  }
 
  String toString( long weak_clock ) {
    assert_now();
    StringBuilder sb = new StringBuilder("{");
    sb.append((weak_clock>>56)&0xFF).append(".");
    sb.append((weak_clock>>48)&0xFF).append(".");
    sb.append((weak_clock>>40)&0xFF).append(".");
    sb.append((weak_clock>>32)&0xFF).append(":");
    sb.append((int)weak_clock).append("}");
    return sb.toString();
  }
  
  private void assert_now() {  if( this != NOW ) throw new Error("unimplemented");  }

}

