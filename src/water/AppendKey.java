package water;

import java.util.Arrays;

/**
 * Atomic Append of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class AppendKey extends Atomic {
  private final byte[] _bits;
  public AppendKey( byte[] bits ) { _bits = bits; } // required ctor for serialization

  public AppendKey( Key key ) {    // Append a key
    _bits = new byte[key.wire_len()];
    key.write(_bits,0);
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
