package water;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.Notification;
import javax.management.NotificationEmitter;

/**
 * Manages memory assigned to key/value pairs. All byte arrays used in
 * keys/values should be allocated through this class - otherwise we risking
 * running out of java memory, and throw unexpected OutOfMemory errors. The
 * theory here is that *most* allocated bytes are allocated in large chunks by
 * allocating new Values - with large backing arrays. If we intercept these
 * allocation points, we cover most Java allocations. If such an allocation
 * might trigger an OOM error we first free up some other memory.
 * 
 * MemoryManager monitors used memory by hooking gc and monitoring the amount of
 * free memory after gc run. It adjusts the size of the cache according to
 * current situation so that the overall heap size stays within give boundaries.
 * Generally when heap usage becomes too high (more than 1/2 of the heap), it
 * starts cleaning values from K/V store. It will also persist some of the
 * non-persisted values during the process. If the memory usage becomes critical
 * (3/4 of the heap) new (managed) allocations will be block until either enough
 * memory is freed or there is more memory to free (in which case OOM is likely
 * to follow soon).
 * 
 * @author tomas
 * @author cliffc
 */
public abstract class MemoryManager {

  static volatile int CACHED = 0;
  static long MAXMEM = 0;
  // Number of threads blocked waiting for memory
  private static int NUM_BLOCKED = 0;

  // block new allocations if false
  private static boolean canAllocate = true;

  /**
   * MemCleaner is the class responsible for memory cleaning. It monitors
   * current memory situation via gc hook/ It is notified after every full-gc
   * run.
   * 
   * Cleaning is performed in a separate thread, so that the hook routine is
   * quick and does not spann across multiple gc runs. The gc-hook monitors the
   * amount of memory after gc run and sets the amount of memory to be cleaned.
   * It may also tell the MemoryManager to block further allocations by setting
   * MemoryManager.canAllocate variable to false.
   * 
   * @author tomas
   * 
   */
  private static class MemCleaner implements
      javax.management.NotificationListener, Runnable {    
    MemoryMXBean _allMemBean = ManagementFactory.getMemoryMXBean(); // general

    int _sCounter = 0; // contains count of successive "old" values
    int _uCounter = 0; // contains count of successive "young" values
        

    // current amount of memory to be freed
    // (set by gc callback, cleaner method decreases it as it frees memory)
    AtomicLong _memToFree = new AtomicLong(0);
    // expected recent use time, values not used in this interval will be
    // removed when low memory
    long _maxTime = 4000;
    long _previousT = _maxTime;
    // prefered size of the cache, cleaner will gradually
    // remove old cached values when above this threshold    
    final long _memHi;
    final long _memOpt;
    final long _memLo;
    // if overall heap memory goes over this limit,
    // stop allocating new value sand remove as much as
    // possible from the cache
    final long _memCritical;

    // if true non persisted old values will be persisted and freed
    boolean _doPersist = true;    

    MemCleaner() {
      MemoryManager.MAXMEM = Runtime.getRuntime().maxMemory();
      _memCritical = (MemoryManager.MAXMEM - (MemoryManager.MAXMEM >> 2) - (MemoryManager.MAXMEM >> 3));      
      _memHi = _memCritical;
      _memLo = MemoryManager.MAXMEM >> 3;
      _memOpt = _memLo + ((_memHi - _memLo) >> 1);
      
      int c = 0;
      for (MemoryPoolMXBean m : ManagementFactory.getMemoryPoolMXBeans()) {
        if (m.getType() != MemoryType.HEAP) // only interested in HEAP
          continue;

        if (m.isCollectionUsageThresholdSupported()) {
          // start cleaning cache when heap occupies _memHi or more
          if (m.isUsageThresholdSupported()) {
            // this should be true only for the Old pool at the moment
            // in any case, we monitor only pools which support usage threshold
            m.setUsageThreshold((MemoryManager.MAXMEM >> 2));
            ++c;
            long threshold = Math.min(MemoryManager.MAXMEM >> 2, m.getUsage()
                .getMax());
            m.setCollectionUsageThreshold(threshold);
            // register the callback
            NotificationEmitter emitter = (NotificationEmitter) _allMemBean;
            emitter.addNotificationListener(this, null, m);
          }
        }
      }
      // there should currently only be one pool supporting usage threshold
      // which we monitor
      assert c == 1;
    }

    static void sleep(long t) {
      try {
        Thread.sleep(t);
      } catch (InterruptedException e) {
      }
    }

    /**
     * Callback routine called by JVM after full gc run. Has two functions: 1)
     * sets the amount of memory to be cleaned from the cache 2) sets the
     * canAllocate flag to false if memory level is critical
     * 
     */
    public void handleNotification(Notification notification, Object handback) {
      String notifType = notification.getType();    
      if (notifType
          .equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
        // overall heap usage
        long heapUsage = _allMemBean.getHeapMemoryUsage().getUsed();        
                        
        if (heapUsage > _memCritical) { // memory level critical
          System.out.println("MEMORY LEVEL CRITICAL, stopping allocations");          
          _memToFree.set(Math.max(_memToFree.get(),CACHED - _memLo));       
          System.out.println("setting mem2Free to " + (_memToFree.get() >> 20) + "M in the callback");
          canAllocate = false; // stop new allocations until we free enough mem
        } else if(!canAllocate) {
          System.out.println("ALLOWING ALLOCATIONS from the callback");
          canAllocate = true;          
        }
      }
    }
//        } else if (mbean.isUsageThresholdSupported()
//            && mbean.isUsageThresholdExceeded()) {
//          minToFree = ((mbean.getUsage().getUsed() - mbean.getUsageThreshold()) >> 1);          
//          canAllocate = true;
//        }
     // }
//      if (minToFree > 0)
//        synchronized (this) {
//          _memToFree.set(minToFree);
//          notify();
//        }
//    }

    // test if value should be remove and update _maxTime estimate
    private boolean removeValue(long currentTime, Value v) {
      long deltaT = currentTime - v._lastAccessedTime;
      if (deltaT > _maxTime) {
        _uCounter = 0;
        // if we hit 10 old elements in a row, increase expected age
        if (++_sCounter == 10) {
          if (_previousT > _maxTime) {
            long x = _maxTime;
            _maxTime += ((_previousT - _maxTime) >> 1);
            _previousT = x;
          } else {
            _previousT = _maxTime;
            _maxTime = (_maxTime << 1) - (_maxTime >> 1);
          }
          _sCounter = 0;
        }
        if (!v.is_persisted() && _doPersist)
          v.store_persist();                
        return v.is_persisted();
      } else {        
        _sCounter = 0;
        // if we hit 10 young elements in a row, decrease expected age
        if (++_uCounter == 10) {
          if (_previousT < _maxTime) {
            long x = _maxTime;
            _maxTime -= ((_maxTime - _previousT) >> 1);
            _previousT = x;
          } else {
            _previousT = _maxTime;
            _maxTime = (_maxTime >> 1) + (_maxTime >> 2);
          }
          _uCounter = 0;
        }
      }
      return false;
    }

    /**
     * Memory cleaning routine. Runs in separate thread so that the callback can
     * get called with every full-gc run.
     * 
     */
    @Override
    public void run() {
      long lastMem2Free = 0;
      while (true) {        
        SimpleValueIterator STORE_ITER = new SimpleValueIterator(
            H2O.STORE.kvs(), (int) (Math.random() * H2O.store_size()));
        long cacheSz = 0;        
        int N = H2O.STORE.kvs().length;
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < N; ++i) {
          Object o = STORE_ITER.next();
          if (!(o instanceof Value)) // Ignore tombstone, etc
            continue;
          Value v = (Value) o;
          byte m[] = v.mem();
          if ((m != null) && (m.length > 0)) {
            // test if we should remove the value (it must old and persisted)
            // and if we have the need to fre more mem
            // the first test *should* always be performed as it also estimates
            // _maxTime
            if (removeValue(currentTime, v) && (_memToFree.get() > 0)) {              
              v.free_mem();
              long m2free = _memToFree.addAndGet(-m.length);
              if (!canAllocate && (m2free < ((_memHi - _memLo) >> 1))){     
                System.out.println("MEMORY BELOW CRITICAL, ALLOWING ALLOCATIONS from the cleaner");
                canAllocate = true;
              }
            } else {
              cacheSz += m.length;
            }
          }
        }            
        CACHED = (int) cacheSz;        
        if(CACHED < _memLo){ // we cleaned too much           
          _memToFree.set(0);
        } else if (CACHED > _memHi){ // we cleaned too little
          _memToFree.set((CACHED - _memHi) + ((_memHi - _memOpt) >> 1));
        } else { // optimal range
          _memToFree.set((CACHED - _memOpt) >> 1);
        }        
        if (!canAllocate && CACHED < _memLo) {          
          // memory was critical and there is nothing more to be freed, return
          // to normal anyways
          System.out.println("allowing mem allocations from the cleaner as there is not enough stuff cached");
          canAllocate = true;
          synchronized (MemoryManager.class) {
            if (NUM_BLOCKED > 0)
              MemoryManager.class.notifyAll();
          }
        }
        // nothing to remove (or there are no cached values), so do not loop
        // needlesly
        if((CACHED < _memLo) || (_memToFree.get() <= 0)) sleep(1000);        
      }
    }
  }

  static MemCleaner _cleaner;

  static {
    _cleaner = new MemCleaner();
    new Thread(_cleaner).start();
  }

  // This is a simple iterator explicitly over the guts of a NonBlockingHashMap
  // - it understands the internal structures and exposes a simple & fast API.
  private final static class SimpleValueIterator {
    final Object[] _arr;
    final int _length;
    int _index;

    SimpleValueIterator(Object[] kvs, int index) {
      _arr = kvs;
      _index = index;
      _length = (kvs.length - 2) >> 1;
      assert (_length & (_length - 1)) == 0; // Assert length is a power of 2
    }

    Object next() {
      _index = (_index + 1) & (_length - 1);
      return _arr[(_index << 1) + 3];
    }

    @Override
    protected SimpleValueIterator clone() {
      Object[] kvs = H2O.STORE.kvs(); // Current STORE backing array
      // If we are still iterating over the same STORE backing array then
      // return the old index (so we proceed from where we last stopped) else
      // restart the iteration at the start of the new array.
      return new SimpleValueIterator(kvs, _arr == kvs ? _index : 0);
    }
  }

  // allocates memory, will block until there is enough available memory
  public static byte[] allocateMemory(int size) {
    if (size < 256)
      return new byte[size];
    while (!canAllocate) {
      // Failed: block until we think we can allocate
      synchronized (MemoryManager.class) {
        NUM_BLOCKED++;
        try {
          MemoryManager.class.wait(1000);
        } catch (InterruptedException ex) {
        }
        --NUM_BLOCKED;
      }
    }
    return new byte[size];
  }

}
