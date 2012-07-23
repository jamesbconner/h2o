package water;

import java.util.HashMap;
import water.hdfs.PersistHdfs;

// The persistence object abstract class and handler.
//  @author peta, cliffc
public abstract class Persistence {

  // The main persistence interface is a single control-byte in the Value
  // object, and a series of abstract calls described below.  The single
  // byte contains the following bitfields:
  // 0-2 - 3 bits, one of 8 persistence backends, described in the enum below.
  // 3   - 1 bit, goal: to either persist this object(1), or to remove this object(0)
  // 4   - 1 bit, goal is met (or not).
  // 5-7 - 3 bits - private state per backend
  
  // Persistence types.
  public enum type {
    ICE (new PersistIce() ), // ICE backed (files & leveldb if enabled)
    HDFS(new PersistHdfs()); // HDFS backend to existing hadoop
    //S3  (new PersistS3()  ), // Amazon S3 cloud backend
    final Persistence _persist;
    type( Persistence persist ) { _persist = persist; }
    static public type[] TYPES = values();
  };

  static byte info(Value v) { return v._persistenceInfo; }
  static type ptype(Value v) { return type.TYPES[info(v)&0x7]; }
  static Persistence p(Value v) { return ptype(v)._persist; }
  static public String name(Value v) { return ptype(v).toString(); }

  static public String stateString( Value v ) {
    return
      (is_goal(v) ? "stor" : "remov") +
      (is     (v) ? "ed"   : "ing"  );
  }

  // Asks  if persistence goal is to either persist (or to remove).
  // True  if the last call was to start(),
  // False if the last call was to remove().
  static public boolean is_goal( Value v ) {
    return (info(v)&8)==0 ? false : true;
  }
  // Asks if persistence is completed (of either storing or deletion)
  static public boolean is( Value v ) {
    return (info(v)&16)==0 ? false : true;
  }

  // Atomically set the mask bits from the _persistenceInfo field
  // TRUE if we changed the bit, FALSE otherwise.
  static protected boolean set_info( Value v, int mask ) {
    int tmp = info(v)&0xFF;
    while(true) {               // Repeat till something changes
      if( (tmp|mask) == tmp ) return false;
      if( v.CAS_persist(tmp,tmp|mask) ) return true;
      tmp = info(v)&0xFF;
    } 
  }
  // Atomically clear the mask bits from the _persistenceInfo field
  // TRUE if we changed the bit, FALSE otherwise.
  static protected boolean clr_info( Value v, int mask ) {
    int tmp = info(v)&0xFF;
    while(true) {               // Repeat till something changes
      if( (tmp&~mask) == tmp ) return false;
      if( v.CAS_persist(tmp,tmp&~mask) ) return true;
      tmp = info(v)&0xFF;
    } 
  }

  // Note that at the moment, store & delete must be called single-threaded.

  // Start this Value persisting.  Ok to call repeatedly, or if the value is
  // already persisted.  Depending on how busy the disk is, and how big the
  // Value is, it might be a long time before the value is persisted.
  public abstract void store( Value v );
  // Remove any trace of this value from the persistence layer.  Called right
  // after the Value is itself deleted.  Depending on how busy the disk is, and
  // how big the Value is, it might be a long time before the disk space is
  // returned.
  public abstract void delete( Value v );
  // Load more of this Value from the persistence layer
  public abstract byte[] load( Value v, int len);
}
