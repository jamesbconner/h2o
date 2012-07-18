package water;
import java.net.DatagramPacket;

/**
 * A UDP Haz Keys packet: inform another Node that I haz these keys...
 * and that I think he should replicate them, and he should tell me
 * when that replication is done.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPHazKeys extends UDP {

  void sendHazKeyAck(byte[] buf, int off, H2ONode h2o) {
    // We have it on disk, and he does not know it...
    // so send the packet right back unchanged.
    buf[0] = (byte)UDP.udp.hazkeyack.ordinal();
    // Send everything back
    MultiCast.singlecast(h2o,buf,off);
  }
  
  // Handle an incoming HazKey packet.  This means that the remote has this key
  // on his disk, but he is not aware if we have it on our disk.
  void call(DatagramPacket pack, H2ONode h2o) {
    if( h2o == null ) return;   // Node died before I could get a key from him?
    byte[] buf = pack.getData();
    int off = 1;
    Key key = Key.read(buf,off);
    off += key.wire_len();
    Value old = H2O.raw_get(key); // Get our local copy
    if( old != null )             // Grab the interned key, if any
      key = (old._key==null) ? H2O.getk(key) : old._key;

    // Read the remote-has-it-on-disk bit
    if( buf[off++] != 0 )         // Sender has it on disk?
      key.set_disk_replica(h2o); // We know it is on the remote disk
    Value val = Value.read(buf, off, key, h2o);
    off += val.wire_len(0,h2o);
    // If we have a the same copy already, no need to pull it over but we might
    // want to tell him that we're replicating it successfully.
    if( old != null && old.same_clock(val) ) {
      // If we have it on disk, then return this Key to him unchanged, which
      // will tell him WE have this Key on our disk.
      if( old.is_local_persist() ) { // Is it locally persisted?
        sendHazKeyAck(buf, off, h2o);
        return;
      }
      // Do I have ALL of the Key?  If not I'll need to fetch the rest of it
      if( old.is_mem_local() )
        return;               // Have all of Key, just not disk persistent yet
    } else if( old==null || old.happens_before(val) ) {
      // if we are on HDFS and the value we are getting is already persisted,
      // we can shortcut in certain cases. The rules are:
      // 1) the hazkey value must be persisted
      // 2) we must not have it, or we must not be loaded
      // 3) In other cases, we do the standard procedure
      
      
      // new implementation - works on HDFS *and* on S3
      if (((val.persistenceBackend().type()==Persistence.Type.HDFS) || (val.persistenceBackend().type()==Persistence.Type.HDFS))
              && (val.persistenceState() == Value.PERSISTED)
              && ((old == null) || (old.is_sentinel()))) {
        // If HDFS has the key, then HDFS has it replicated also.
        // So we basically believe
        if (key.is_disk_replica(h2o)) // If remotely replicated then...
          val.setPersistenceState(Value.PERSISTED); // ...it counts as LOCALLY replicated also
        H2O.put_if_later(key,val);
        sendHazKeyAck(buf,off,h2o);
        return;
      }

      /*
      if( (val.type()=='H') && (val.persistenceState() == Value.PERSISTED) && ((old==null) || (!old.is_loaded()))) {
        // If HDFS has the key, then HDFS has it replicated also.
        // So we basically believe
        if (key.is_disk_replica(h2o)) // If remotely replicated then...
          val.setPersistenceState(Value.PERSISTED); // ...it counts as LOCALLY replicated also
        // PETA
        Log.die("IMPLEMENT ME");
    //    H2O.put_if_later(key,val.is_deleted() ? val : ValueHdfs.ON_HADOOP);
        sendHazKeyAck(buf,off,h2o);
        return; 
      } */
      // Fall thru: he has a more recent copy
    } else {
      // Actually, we have a more recent copy.  We need to HazKey this guy
      // right back with our new & improved value
      key.clr_disk_replica(h2o); //
      build_and_send(h2o,key,old);
      return;                 // Actually, we have a more recent copy
    }
    // if the value is deleted, we do not need to get it, we already know
    if (val instanceof ValueDeleted) {
      val.setPersistenceState(Value.NOT_STARTED);
      H2O.put_if_later(key,val);
      sendHazKeyAck(buf,off,h2o);
      return;
    }
    // Unless the Cloud changed recently, the other guy already figured out I
    // really should have this key.  Get it all.  Note that I do not wait on
    // the response - I just start the process of key-fetch and as a side
    // effect I record the returned key
    TaskGetKey.make(h2o,key,Integer.MAX_VALUE);
  }

  // Define the packet for announcing Keys are available for replication.
  // Pre-built buf for fast/bulk version.
  static void build_and_send( H2ONode h2o, byte[] buf, Key key, Value val ) {
    int off=0;
    buf[off++] = (byte)UDP.udp.hazkey.ordinal();
    off = key.write(buf,off);
    buf[off++] = (byte)(val.is_local_persist() ? 1 : 0);
    off = val.write(buf, off, 0, h2o);
    MultiCast.singlecast(h2o,buf,off);
  }
  // Build-your-own-buffer version
  static void build_and_send( H2ONode h2o, Key key, Value val ) {
    DatagramPacket pack = UDPReceiverThread.get_pack();
    byte[] buf = pack.getData();
    build_and_send(h2o,buf,key,val);
    UDPReceiverThread.free_pack(pack);
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

