package water;

import java.util.HashMap;

/** The persistence object abstract class and handler.
 * 
 * 
 * 
 *
 * @author peta
 */
public abstract class Persistence {
  
  // Interface -----------------------------------------------------------------
  
  /** Persistence types.
   * 
   * Each persistence backend that can be used should be put here. Each type
   * must have a unique number, they must increase by 1.
   */
  public enum Type {
    /// ICE backed (files & leveldb if enabled), implemented by PersistIce
    ICE(0),
    /// HDFS backend to existing hadoop, implemented by PersistHdfs
    HDFS(1),
    /// Amazon S3 cloud backend, implemented by PersistS3
    S3(2),
    /// Internal keys that never persist, implemented by PersistNone
    INTERNAL(3),
    // end sentinel
    _END(4)
    ;
    public final int id;
    Type(int id) {
      assert (id>=0) && (id<64); // otherwise we won't fit into the place with value
      this.id = id;
    }
  }
  
  /** Given a sentinel, loads the proper key bound value and returns its value
   * object. 
   * 
   * The persistence backend of the value must be the called backend. The
   * sentinel's data can be used to further setup the value properly (for ice
   * persistence for example the leveldb vs classic file is distinguished by
   * the sentinel data field of the sentinel).
   * 
   * @param k Key to which the value should be bound.
   * @param sentinel Sentinel describing the type and sentinel data for the new
   * value.
   * @return New value bound to the given key that is persisted and can be
   * loaded from the persistence backend when needed.
   */  
  public abstract Value load(Key k, Value sentinel);
  
  /** Reads the value from the persistence store.
   * 
   * Reads at least len value bytes from the persistence store and returns the
   * resulting byte. 
   * 
   * NOTE: Make sure to use the MemoryManager to allocate the array properly,
   * otherwise the memory backpressure would not work. 
   * 
   * @param k Key to load.
   * @param v Existing value associated with the key.
   * @param len Min number of bytes to read
   * @return Read bytes. 
   */
  public abstract byte[] get(Key k, Value v, int len);
  
  /** Stores the value to the persistence store. 
   * 
   * Stores the given value associated with the key to the persistence store. 
   * Prior to the previous versions does not actually delete the old value
   * because the old value might be associated with a different backend and is
   * thus deleted by the DKV itself. 
   * 
   * While no two threads can enter the store() method for a particular value
   * simultaneously, if the value is deleted while it is being persisted and its
   * memory must be freed up using the memory manager, the mem() access to the
   * value must be treated as if multi-threaded access is possible.
   * 
   * @param k Key to be stored.
   * @param v Value to be stored. 
   * @return True if successful, false otherwise.
   */
  public abstract boolean store(Key k, Value v);
  
  /** Deletes the value from the persistence store.
   * 
   * @param k Key to be deleted.
   * @param v Value to be deleted. 
   * @return True if successful. 
   */
  public abstract boolean delete(Key k, Value v);
  
  /** Returns the size of the value.
   * 
   * This method is used when creating a proper value from the sentinel when we
   * must determine the maximum size of the value. 
   * 
   * @param k Key of the value.
   * @param v The value (the sentinel of the value).
   * @return  The size of the value, or 0 on error. // PETA Maybe -1 will be better?
   */
  public abstract long size(Key k, Value v);
  
  /** Returns the name of the persistence backend.  */
  public abstract String name();
  
  /** Returns the type of the persistence backend. */
  public abstract Type type();
  
  /** Returns the sentinel data for given key and value. 
   * 
   * This can be used to encode additional persistence related information in
   * the sentinel values. 
   * 
   * @param k Key.
   * @param v Old value (not sentinel)
   * @return Returns the persistence data that should be stored in the sentinel.
   */
  public short sentinelData(Key k, Value v) {
    return 0;
  }
  
  // Persistence Backends ------------------------------------------------------
  
  // List of all persistence objects
  protected static Persistence[] _persistenceObjects = new Persistence[Type._END.id];
  
  /** Constructor that assigns the id to the persistence object. 
   * 
   * All persistence objects cannot be ycreated from outside, but their static
   * initializers should load their single instances calling this constructor
   * that also registers the persistence backend in the list of available
   * backends. 
   */
  protected Persistence(Type ptype) {
    assert (ptype.id>=0) && (ptype.id<_persistenceObjects.length) && (_persistenceObjects[ptype.id]==null);
    _persistenceObjects[ptype.id] = this;
  }

  /** Gets the persistence object associated with given index.
   * 
   * The index is the Persistence.Type value of the persistence backend. This
   * method is for internal use only, use the methods of Value class to get a
   * persistence backend of a particular value. 
   * 
   * @param index
   * @return 
   */
  protected static Persistence getPersistence(int index) {
    if ((index<0) || (index>=_persistenceObjects.length))
      return null;
    return _persistenceObjects[index];
  }

  // Sentinels -----------------------------------------------------------------
  
  // a map of sentinels available to this persistence backend, sentinels are
  // identifiable by their max size and type. Just all of it in a long
  private final HashMap<Long,Value> _sentinels = new HashMap();
  
  /** Returns a sentinel with given user data, type and persistency.
   * 
   * The persistence used is this. This is a non-public method to be only used
   * by persistence backends when they create sentinels during the startup.
   * 
   * @param max
   * @param type
   * @return 
   */
  protected Value getSentinel(short userData, byte type) {
    long idx = (userData << 8) | (0xff & type);
    if (!_sentinels.containsKey(idx)) {
      synchronized (_sentinels) {
        if (!_sentinels.containsKey(idx))
          _sentinels.put(idx, ValueSentinel.create(userData,this,type));
      }
    }
    return _sentinels.get(idx);
  }
}
