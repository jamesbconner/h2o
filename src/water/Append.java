package water;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

/**
 * Atomic Append of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

@RTSerializer(Append.Serializer.class)
public class Append extends Atomic {
  byte[] _bits;
  public Append( byte[] bits ) { _bits=bits; }
  public Append( Key key ) {    // Append a key
    this( new byte[key.wire_len()] );
    key.write(_bits,0);
  }
  public static class Serializer extends RemoteTaskSerializer<Append> {
    @Override public int wire_len(Append a) { return 4+a._bits.length; }
    @Override public int write( Append a, byte[] buf, int off ) {
      off += UDP.set4(buf,off,a._bits.length);
      System.arraycopy(a._bits,0,buf,off,a._bits.length);  off += a._bits.length;
      return off;
    }
    @Override public void write( Append a, DataOutputStream dos ) throws IOException { dos.write(a._bits); }
    @Override public Append read( byte[] buf, int off ) {
      byte[] bits = new byte[UDP.get4(buf,(off+=4)-4)];
      System.arraycopy(buf,off,bits,0,bits.length);
      return new Append(bits);
    }
    @Override public Append read( DataInputStream dis ) throws IOException {
      byte[] bits = new byte[dis.readInt()];
      dis.readFully(bits);
      return new Append(bits);
    }
  }

  // Just append the bits
  @Override public byte[] atomic( byte[] bits1 ) {
    if( bits1 == null ) return _bits;
    byte[] bits2 = Arrays.copyOf(bits1,bits1.length+_bits.length);
    System.arraycopy(_bits,0,bits2,bits1.length,_bits.length);
    return bits2;
  }
}
