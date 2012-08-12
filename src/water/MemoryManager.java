package water;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages memory assigned to key/value pairs.  All byte arrays used in
 * keys/values should be allocated through this class - otherwise we risking
 * running out of java memory, and throw unexpected OutOfMemory errors.  The
 * theory here is that *most* allocated bytes are allocated in large chunks by
 * allocating new Values - with large backing arrays.  If we intercept these
 * allocation points, we cover most Java allocations.  If such an allocation
 * might trigger an OOM error we first free up some other memory.
 * 
 * When allocating memory, MemoryManager makes sure current the k+v memory
 * footprint is below given threshold.  Otherwise, it tries to steal memory of
 * outdated value or "free" (delete values from memory - gc run required to
 * actually free the memory) enough space so that the memory can be allocated.
 * If there is not enough memory available, it blocks/ return null depending on
 * the used interface.
 * 
 * There's lots of room for improvement here.  For instance, its easy to leak
 * the memory of freed Values (i.e., the memory is really freed, but the USED
 * field does not track it - so the system believes it has less memory
 * available to it).  This could be fixed by periodically reading all the
 * active Values and setting USED accordingly.  We do not track length of keys,
 * nor any internally memory other than thread stacks.
 *
 * Memory 
 * @author tomas
 * @author cliffc
 */
public abstract class MemoryManager {

  // Max memory available to the Java heap
  static long MAXMEM = Runtime.getRuntime().maxMemory();
  // Estimate of memory in-use by Value objects
  static final AtomicLong USED = new AtomicLong(0);
  // Estimated bytes lost per K/V pair
  static final long PER_KEY = 100;
  // Estimated bytes lost per thread, 1Meg
  static final long PER_THREAD = 1L<<10;
  // Iterator over the H2O store
  private static SimpleValueIterator STORE_ITER = new SimpleValueIterator(H2O.STORE.kvs());
  // Number of threads blocked waiting for memory
  private static int NUM_BLOCKED = 0;

  // Memory margin: how much "slop" we'd like to keep in the Java heap
  // expressed as USED+B < MAXMEM*A/256
  static final int  A = 128;           // 128/256 = 1/2ths of heap
  static final long B = 10*(1L << 20); // 10Meg
  // Compute: USED+B < MAXMEM*A/256
  private static boolean can_allocate( long size ) { 
    return true;
    // Estimate in-use size
    //long mem_used = 
    //  USED.get()+               // Current bytes burned by Values
    //  size+                     // Plus this new allocation
    //  H2O.SELF.get_keys()*PER_KEY+            // Plus some per-key overhead
    //  H2O.SELF.get_thread_count()*PER_THREAD+ // Plus some per-thread overhead
    //  B;                                      // Plus some per-JVM overhead
    //long cutoff = (MAXMEM*A)>>8; // Cutoff: fraction of memory we can live with
    //return mem_used < cutoff;
  }

  // This is a simple iterator explicitly over the guts of a NonBlockingHashMap
  // - it understands the internal structures and exposes a simple & fast API.
  private final static class SimpleValueIterator {
    final Object [] _arr;
    final int _length;
    int _index;
    
    SimpleValueIterator(Object [] kvs) { this(kvs,-1); }
    SimpleValueIterator(Object [] kvs, int index) {
      _arr = kvs;
      _index = index;
      _length = (kvs.length - 2) >> 1;
      assert (_length & (_length-1))==0; // Assert length is a power of 2
    }
    
    Object next() {
      _index = (_index+1) & (_length-1);
      return _arr[(_index << 1) + 3];
    }
    
    @Override
    protected SimpleValueIterator clone() {
      Object[] kvs = H2O.STORE.kvs(); // Current STORE backing array
      // If we are still iterating over the same STORE backing array then
      // return the old index (so we proceed from where we last stopped) else
      // restart the iteration at the start of the new array.
      return new SimpleValueIterator(kvs, _arr==kvs ? _index : 0);
    }
  }

  // Eagerly free things as values persist, if we have blocked requests
  public static void notifyValuePersisted(Value v) {
    if( NUM_BLOCKED > 0 )
      v.CAS_mem(v.mem(),null);  // This is a 1-shot throw-away CAS
  }

  // Mark memory as freed; if there are queued-waiting threads then awaken
  // them, so they may try again.
  public static void freeMemory(byte [] mem) {
    if( mem == null ) return;   // Nothing freed
    assert USED.get() >= mem.length;
    USED.addAndGet(-mem.length); // Mark memory as freed
    if( NUM_BLOCKED > 0 &&       // wake up sleeping threads if any
        // But only if we have freed ourselves back to the margin-level
        can_allocate(0) )
      synchronized(MemoryManager.class) {
        MemoryManager.class.notify();
      }            
  }

  // allocates memory, will block until there is enough available memory
  public static byte[] allocateMemory(int size) {
    while( true ) {
      // Attempt to allocate
      byte [] mem = tryAllocateMemory(size);
      if( mem != null ) return mem; // Successful!

      // Failed: block until we think we can allocate
      synchronized(MemoryManager.class) {
        NUM_BLOCKED++;
        try { MemoryManager.class.wait(1000); }
        catch( InterruptedException ex ) { }
        --NUM_BLOCKED;
      }
    } // while(true)
  }

  // Try to allocate memory, return null if there is not enough free memory
  public static byte[] tryAllocateMemory(int size) {
    final int numkeys = H2O.STORE.size(); // Part of the memory footprint estimate
    SimpleValueIterator myIter = null;
    int i = 0;                  // Iterator index
    while( true ) {
      long oldVal = USED.get();
      if( can_allocate(size) && // Can allocate: will not overrun margin
          USED.compareAndSet(oldVal, oldVal+size) ) {
        byte[] mem = tryAllocateMemory2(size);
        if( mem != null ) return mem;
      }
      // Get an iterator over the entire local H2O store.
      // Start from the last iteration point.
      if( myIter == null ) myIter = STORE_ITER.clone();
      // If we have walked the entire H2O store at least once without finding
      // enough memory to free - then give it up.
      if( i++ >= myIter._length )
        return null;
      Object o = myIter.next();
      if( !(o instanceof Value)) // Ignore tombstone, etc
        continue;
      Value v = (Value)o;
      if( !v.is_persisted() )   // Is on disk?
        continue;               // Nope...
      byte[] mem = v.mem();
      if( mem == null ) continue; // Already freed
      System.out.println("Free'ing memory on key "+v._key+" because can_allocate has said No to "+size+" bytes");
      v.CAS_mem(mem,null);        // One-shot free attempt
    }
  }

  public static final byte[] tryAllocateMemory2(int size) {
    try {
      return new byte[size];
    } catch (OutOfMemoryError e) { // should not happen but just in case...
      System.err.println("OutOfMemoryError in tryAllocateMemory!!!");
      return null;
    }
  }

  public static String dbgStatus() {
    final Runtime run = Runtime.getRuntime();
    return
      "MAXMEM= " + (MAXMEM >> 20) + "M, "+
      "USED= " + (USED.get() >> 20) +"M, "+
      "NUM_BLOCKED= " + NUM_BLOCKED +", "+
      "freeMem= "+(run.freeMemory()>>20)+"M.";
  }
}
