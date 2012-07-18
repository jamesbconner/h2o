package water;
import java.net.DatagramPacket;

/**
 * A UDP Haz Keys Ack packet: inform another Node that I haz these keys...
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPHazKeyAck extends UDP {

  // Handle an incoming HazKeyAck packet.  This is in response to a HazKeys we
  // sent the remote, meaning WE have this key on our disk already before we
  // sent the HazKeys.  The remote is telling us that he has this key on his disk.
  void call(DatagramPacket pack, H2ONode h2o) {
    if( h2o == null ) return;   // Node died before I could get a key from him?
    byte[] buf = pack.getData();
    int off = 1;
    Key key = Key.read(buf,off);  off += key.wire_len();
    off++;                      // Also skip HazKey disk byte
    Value val = H2O.raw_get(key);
    if( val == null ) return;   // Key deleted already?
    Value sval = Value.read(buf, off, key, h2o);
    if( !val.same_clock(sval)) return; // Wrong Value
    key = val._key == null ? H2O.getk(key) : val._key; // Intern key
    key.set_disk_replica(h2o); // We know it is on the remote disk
  }

  // Define the packet for announcing Keys are available for replication
  static void build_and_send( H2ONode h2o, byte[] buf, Key key ) {
    int off=0;
    buf[off++] = (byte)UDP.udp.hazkeyack.ordinal();
    off = key.write(buf,off);
    MultiCast.singlecast(h2o,buf,off);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  // byte 1 is key type; byte 2 is rf, next 2 bytes are key len, then the key bytes
  static final char cs[] = new char[16];
  public String print16( long lo, long hi ) {
    byte rf = (byte)(lo>>8);
    int klen = (int)((lo>>16)&0xFFFF);
    for( int i=4; i<8; i++ )
      cs[i-4  ] = (char)((lo>>(i<<3))&0xff);
    for( int i=0; i<8; i++ )
      cs[i-4+8] = (char)((hi>>(i<<3))&0xff);
    int len = Math.min(12,klen);
    return "key["+klen+"]="+new String(cs,0,len)+((len==klen)?"":"...");
  }
}

