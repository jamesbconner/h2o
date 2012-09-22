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
//@RTSerializer(AppendKey.Serializer.class)
public class AppendKey extends Atomic {
  private final byte[] _bits;
  private AppendKey( byte[] bits ) { _bits = bits; }
  public AppendKey( Key key ) {    // Append a key
    _bits = new byte[key.wire_len()];
    key.write(_bits,0);
  }
  public static class Serializer extends RemoteTaskSerializer<AppendKey> {
    @Override public int wire_len(AppendKey a) { return 4+a._bits.length; }

    @Override
    public void write(AppendKey task, Stream s) {
      s.setLen4Bytes(task._bits);
    }

    @Override
    public AppendKey read(Stream s) {
      return new AppendKey(s.getLen4Bytes());
    }

    @Override public int write( AppendKey a, byte[] buf, int off ) { throw new RuntimeException("Should not be called"); }
    @Override public AppendKey read(         byte[] buf, int off ) { throw new RuntimeException("Should not be called"); }

    @Override public void write( AppendKey a, DataOutputStream dos ) throws IOException {
      dos.write(a._bits);
    }
    @Override public AppendKey read( DataInputStream dis ) throws IOException {
      byte[] bits = new byte[dis.readInt()];
      dis.readFully(bits);
      return new AppendKey(bits);
    }
  }

  // Just append the bits
  @Override public byte[] atomic( byte[] bits1 ) {
    if( bits1 == null ) bits1 = new byte[4]; // Include the key count of zero
    byte[] bits2 = Arrays.copyOf(bits1,bits1.length+_bits.length);
    System.arraycopy(_bits,0,bits2,bits1.length,_bits.length);
    UDP.set4(bits2,0,UDP.get4(bits1,0)+1); // Increment key count
    return bits2;
  }
}
