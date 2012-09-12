///////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2001, Eric D. Friedman All Rights Reserved.
// Copyright (c) 2009, Rob Eden All Rights Reserved.
// Copyright (c) 2009, Jeff Randall All Rights Reserved.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
package hexlytics.rf;

import java.util.Arrays;

/**
 * An open addressed Map implementation for short keys and float values.
 *
 * @author Eric D. Friedman
 * @author Rob Eden
 * @author Jeff Randall
 * @version $Id: _K__V_HashMap.template,v 1.1.2.16 2010/03/02 04:09:50 robeden Exp $
 */

public class H extends TFloatShortHash {
    static final long serialVersionUID = 1L;

    /** the values of the map */
    protected transient short[] _values;



    /**
     * Creates a new <code>TFloatShortHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public H( int initialCapacity ) {
        super( initialCapacity );
    }


    /**
     * initializes the hashtable to a prime capacity which is at least
     * <tt>initialCapacity + 1</tt>.
     *
     * @param initialCapacity an <code>int</code> value
     * @return the actual capacity chosen
     */
    protected int setUp( int initialCapacity ) {
        int capacity;

        capacity = super.setUp( initialCapacity );
        _values = new short[capacity];
        return capacity;
    }


    /**
     * rehashes the map to the new capacity.
     *
     * @param newCapacity an <code>int</code> value
     */
     /** {@inheritDoc} */
    protected void rehash( int newCapacity ) {
        int oldCapacity = _set.length;
        
        float oldKeys[] = _set;
        short oldVals[] = _values;
        byte oldStates[] = _states;

        _set = new float[newCapacity];
        _values = new short[newCapacity];
        _states = new byte[newCapacity];

        for ( int i = oldCapacity; i-- > 0; ) {
            if( oldStates[i] == FULL ) {
                float o = oldKeys[i];
                int index = insertKey( o );
                _values[index] = oldVals[i];
            }
        }
    }


    /** {@inheritDoc} */
    public short put( float key, short value ) {
        int index = insertKey( key );
        return doPut( key, value, index );
    }


    private short doPut( float key, short value, int index ) {
        short previous = no_entry_value;
        boolean isNewMapping = true;
        if ( index < 0 ) {
            index = -index -1;
            previous = _values[index];
            isNewMapping = false;
        }
        _values[index] = value;

        if (isNewMapping) {
            postInsertHook( consumeFreeSlot );
        }

        return previous;
    }

    /** {@inheritDoc} */
    public short get( float key ) {
        int index = index( key );
        return index < 0 ? no_entry_value : _values[index];
    }


    /** {@inheritDoc} */
    public boolean isEmpty() {
        return 0 == _size;
    }




    /** {@inheritDoc} */
    public float[] keys() {
        float[] keys = new float[size()];
        float[] k = _set;
        byte[] states = _states;

        for ( int i = k.length, j = 0; i-- > 0; ) {
          if ( states[i] == FULL ) {
            keys[j++] = k[i];
          }
        }
        return keys;
    }



    /** {@inheritDoc} */
    public short[] values() {
        short[] vals = new short[size()];
        short[] v = _values;
        byte[] states = _states;

        for ( int i = v.length, j = 0; i-- > 0; ) {
          if ( states[i] == FULL ) {
            vals[j++] = v[i];
          }
        }
        return vals;
    }

    /** {@inheritDoc} */
    public boolean containsValue( short val ) {
        byte[] states = _states;
        short[] vals = _values;

        for ( int i = vals.length; i-- > 0; ) {
            if ( states[i] == FULL && val == vals[i] ) {
                return true;
            }
        }
        return false;
    }


    /** {@inheritDoc} */
    public boolean containsKey( float key ) {
        return contains( key );
    }


} // TFloatShortHashMap


 
 abstract class TFloatShortHash extends TPrimitiveHash {
  static final long serialVersionUID = 1L;

  /** the set of floats */
  public transient float[] _set;


  /**
   * key that represents null
   *
   * NOTE: should not be modified after the Hash is created, but is
   *       not final because of Externalization
   *
   */
  protected float no_entry_key;


  /**
   * value that represents null
   *
   * NOTE: should not be modified after the Hash is created, but is
   *       not final because of Externalization
   *
   */
  protected short no_entry_value;

  protected boolean consumeFreeSlot;


  /**
   * Creates a new <code>T#E#Hash</code> instance whose capacity
   * is the next highest prime above <tt>initialCapacity + 1</tt>
   * unless that value is already prime.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public TFloatShortHash( int initialCapacity ) {
      super( initialCapacity );
      no_entry_key = ( float ) 0;
      no_entry_value = ( short ) 0;
  }


  /**
   * initializes the hashtable to a prime capacity which is at least
   * <tt>initialCapacity + 1</tt>.
   *
   * @param initialCapacity an <code>int</code> value
   * @return the actual capacity chosen
   */
  protected int setUp( int initialCapacity ) {
      int capacity;

      capacity = super.setUp( initialCapacity );
      _set = new float[capacity];
      return capacity;
  }


  /**
   * Searches the set for <tt>val</tt>
   *
   * @param val an <code>float</code> value
   * @return a <code>boolean</code> value
   */
  public boolean contains( float val ) {
      return index(val) >= 0;
  }
  


  /**
   * Locates the index of <tt>val</tt>.
   *
   * @param key an <code>float</code> value
   * @return the index of <tt>val</tt> or -1 if it isn't in the set.
   */
  protected int index( float key ) {
      int hash, probe, index, length;

      final byte[] states = _states;
      final float[] set = _set;
      length = states.length;
      hash = HashFunctions.hash( key ) & 0x7fffffff;
      index = hash % length;
      byte state = states[index];

      if (state == FREE)
          return -1;

      if (state == FULL && set[index] == key)
          return index;

      return indexRehashed(key, index, hash, state);
  }

  int indexRehashed(float key, int index, int hash, byte state) {
      // see Knuth, p. 529
      int length = _set.length;
      int probe = 1 + (hash % (length - 2));
      final int loopIndex = index;

      do {
          index -= probe;
          if (index < 0) {
              index += length;
          }
          state = _states[index];
          //
          if (state == FREE)
              return -1;

          //
          if (key == _set[index] && state != REMOVED)
              return index;
      } while (index != loopIndex);

      return -1;
  }


  /**
   * Locates the index at which <tt>val</tt> can be inserted.  if
   * there is already a value equal()ing <tt>val</tt> in the set,
   * returns that value as a negative integer.
   *
   * @param key an <code>float</code> value
   * @return an <code>int</code> value
   */
       protected int insertKey( float val ) {
           int hash, index;

           hash = HashFunctions.hash(val) & 0x7fffffff;
           index = hash % _states.length;
           byte state = _states[index];

           consumeFreeSlot = false;

           if (state == FREE) {
               consumeFreeSlot = true;
               insertKeyAt(index, val);

               return index;       // empty, all done
           }

           if (state == FULL && _set[index] == val) {
               return -index - 1;   // already stored
           }

           // already FULL or REMOVED, must probe
           return insertKeyRehash(val, index, hash, state);
       }

       int insertKeyRehash(float val, int index, int hash, byte state) {
           // compute the double hash
           final int length = _set.length;
           int probe = 1 + (hash % (length - 2));
           final int loopIndex = index;
           int firstRemoved = -1;

           /**
            * Look until FREE slot or we start to loop
            */
           do {
               // Identify first removed slot
               if (state == REMOVED && firstRemoved == -1)
                   firstRemoved = index;

               index -= probe;
               if (index < 0) {
                   index += length;
               }
               state = _states[index];

               // A FREE slot stops the search
               if (state == FREE) {
                   if (firstRemoved != -1) {
                       insertKeyAt(firstRemoved, val);
                       return firstRemoved;
                   } else {
                       consumeFreeSlot = true;
                       insertKeyAt(index, val);
                       return index;
                   }
               }

               if (state == FULL && _set[index] == val) {
                   return -index - 1;
               }

               // Detect loop
           } while (index != loopIndex);

           // We inspected all reachable slots and did not find a FREE one
           // If we found a REMOVED slot we return the first one found
           if (firstRemoved != -1) {
               insertKeyAt(firstRemoved, val);
               return firstRemoved;
           }

           // Can a resizing strategy be found that resizes the set?
           throw new IllegalStateException("No free or removed slots available. Key set full?!!");
       }

       void insertKeyAt(int index, float val) {
           _set[index] = val;  // insert value
           _states[index] = FULL;
       }

  protected int XinsertKey( float key ) {
      int hash, probe, index, length;

      final byte[] states = _states;
      final float[] set = _set;
      length = states.length;
      hash = HashFunctions.hash( key ) & 0x7fffffff;
      index = hash % length;
      byte state = states[index];

      consumeFreeSlot = false;

      if ( state == FREE ) {
          consumeFreeSlot = true;
          set[index] = key;
          states[index] = FULL;

          return index;       // empty, all done
      } else if ( state == FULL && set[index] == key ) {
          return -index -1;   // already stored
      } else {                // already FULL or REMOVED, must probe
          // compute the double hash
          probe = 1 + ( hash % ( length - 2 ) );

          // if the slot we landed on is FULL (but not removed), probe
          // until we find an empty slot, a REMOVED slot, or an element
          // equal to the one we are trying to insert.
          // finding an empty slot means that the value is not present
          // and that we should use that slot as the insertion point;
          // finding a REMOVED slot means that we need to keep searching,
          // however we want to remember the offset of that REMOVED slot
          // so we can reuse it in case a "new" insertion (i.e. not an update)
          // is possible.
          // finding a matching value means that we've found that our desired
          // key is already in the table

          if ( state != REMOVED ) {
              // starting at the natural offset, probe until we find an
              // offset that isn't full.
              do {
                  index -= probe;
                  if (index < 0) {
                      index += length;
                  }
                  state = states[index];
              } while ( state == FULL && set[index] != key );
          }

          // if the index we found was removed: continue probing until we
          // locate a free location or an element which equal()s the
          // one we have.
          if ( state == REMOVED) {
              int firstRemoved = index;
              while ( state != FREE && ( state == REMOVED || set[index] != key ) ) {
                  index -= probe;
                  if (index < 0) {
                      index += length;
                  }
                  state = states[index];
              }

              if (state == FULL) {
                  return -index -1;
              } else {
                  set[index] = key;
                  states[index] = FULL;

                  return firstRemoved;
              }
          }
          // if it's full, the key is already stored
          if (state == FULL) {
              return -index -1;
          } else {
              consumeFreeSlot = true;
              set[index] = key;
              states[index] = FULL;

              return index;
          }
      }
  }


} // TFloatShortHash


abstract  class TPrimitiveHash extends THash {
  
    public  byte[] _states;

    /* constants used for state flags */

    /** flag indicating that a slot in the hashtable is available */
    public static final byte FREE = 0;

    /** flag indicating that a slot in the hashtable is occupied */
    public static final byte FULL = 1;

    /**
     * flag indicating that the value of a slot in the hashtable
     * was deleted
     */
    public static final byte REMOVED = 2;

    /**
     * Creates a new <code>TPrimitiveHash</code> instance with a prime
     * capacity at or near the specified capacity and with the default
     * load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TPrimitiveHash( int initialCapacity ) {
        this( initialCapacity, DEFAULT_LOAD_FACTOR );
    }


    /**
     * Creates a new <code>TPrimitiveHash</code> instance with a prime
     * capacity at or near the minimum needed to hold
     * <tt>initialCapacity<tt> elements with load factor
     * <tt>loadFactor</tt> without triggering a rehash.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public TPrimitiveHash( int initialCapacity, float loadFactor ) {
        super();
        initialCapacity = Math.max( 1, initialCapacity );
        _loadFactor = loadFactor;
        setUp( HashFunctions.fastCeil( initialCapacity / loadFactor ) );
    }


    /**
     * Returns the capacity of the hash table.  This is the true
     * physical capacity, without adjusting for the load factor.
     *
     * @return the physical capacity of the hash table.
     */
    public int capacity() {
        return _states.length;
    }


    /**
     * initializes the hashtable to a prime capacity which is at least
     * <tt>initialCapacity + 1</tt>.
     *
     * @param initialCapacity an <code>int</code> value
     * @return the actual capacity chosen
     */
    protected int setUp( int initialCapacity ) {
        int capacity;

        capacity = super.setUp( initialCapacity );
        _states = new byte[capacity];
        return capacity;
    }
} // TPrimitiveHash


abstract  class THash {
    /** the load above which rehashing occurs. */
    protected static final float DEFAULT_LOAD_FACTOR = Constants.DEFAULT_LOAD_FACTOR;

    /**
     * the default initial capacity for the hash table.  This is one
     * less than a prime value because one is added to it when
     * searching for a prime capacity to account for the free slot
     * required by open addressing. Thus, the real default capacity is
     * 11.
     */
    protected static final int DEFAULT_CAPACITY = Constants.DEFAULT_CAPACITY;


    /** the current number of occupied slots in the hash. */
    protected transient int _size;

    /** the current number of free slots in the hash. */
    protected transient int _free;

    protected float _loadFactor;

    protected int _maxSize;

    public THash() {
        this( DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR );
    }

    /**
     * Creates a new <code>THash</code> instance with a prime capacity
     * at or near the minimum needed to hold <tt>initialCapacity</tt>
     * elements with load factor <tt>loadFactor</tt> without triggering
     * a rehash.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public THash( int initialCapacity, float loadFactor ) {
        super();
        _loadFactor = loadFactor;

        setUp( HashFunctions.fastCeil( initialCapacity / loadFactor ) );
    }


    /**
     * Tells whether this set is currently holding any elements.
     *
     * @return a <code>boolean</code> value
     */
    public boolean isEmpty() {
        return 0 == _size;
    }


    /**
     * Returns the number of distinct elements in this collection.
     *
     * @return an <code>int</code> value
     */
    public int size() {
        return _size;
    }


    /** @return the current physical capacity of the hash table. */
    abstract public int capacity();




    /**
     * initializes the hashtable to a prime capacity which is at least
     * <tt>initialCapacity + 1</tt>.
     *
     * @param initialCapacity an <code>int</code> value
     * @return the actual capacity chosen
     */
    protected int setUp( int initialCapacity ) {
        int capacity;

        capacity = PrimeFinder.nextPrime( initialCapacity );
        computeMaxSize( capacity );
        return capacity;
    }


    /**
     * Rehashes the set.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected abstract void rehash( int newCapacity );


    /**
     * Computes the values of maxSize. There will always be at least
     * one free slot required.
     *
     * @param capacity an <code>int</code> value
     */
    protected void computeMaxSize( int capacity ) {
        // need at least one free slot for open addressing
        _maxSize = Math.min( capacity - 1, (int) ( capacity * _loadFactor ) );
        _free = capacity - _size; // reset the free element count
    }


    

    /**
     * After an insert, this hook is called to adjust the size/free
     * values of the set and to perform rehashing if necessary.
     *
     * @param usedFreeSlot the slot
     */
    protected final void postInsertHook( boolean usedFreeSlot ) {
        if ( usedFreeSlot ) {
            _free--;
        }

        // rehash whenever we exhaust the available space in the table
        if ( ++_size > _maxSize || _free == 0 ) {
            // choose a new capacity suited to the new state of the table
            // if we've grown beyond our maximum size, double capacity;
            // if we've exhausted the free spots, rehash to the same capacity,
            // which will free up any stale removed slots for reuse.
            int newCapacity = _size > _maxSize ? PrimeFinder.nextPrime( capacity() << 1 ) : capacity();
            rehash( newCapacity );
            computeMaxSize( capacity() );
        }
    }


    protected int calculateGrownCapacity() {
        return capacity() << 1;
    }

}// THash

final class PrimeFinder {
  /**
   * The largest prime this class can generate; currently equal to
   * <tt>Integer.MAX_VALUE</tt>.
   */
  public static final int largestPrime = Integer.MAX_VALUE; //yes, it is prime.

  private static final int[] primeCapacities = {
      //chunk #0
      largestPrime,

      //chunk #1
      5,11,23,47,97,197,397,797,1597,3203,6421,12853,25717,51437,102877,205759,
      411527,823117,1646237,3292489,6584983,13169977,26339969,52679969,105359939,
      210719881,421439783,842879579,1685759167,

      //chunk #2
      433,877,1759,3527,7057,14143,28289,56591,113189,226379,452759,905551,1811107,
      3622219,7244441,14488931,28977863,57955739,115911563,231823147,463646329,927292699,
      1854585413,

      //chunk #3
      953,1907,3821,7643,15287,30577,61169,122347,244703,489407,978821,1957651,3915341,
      7830701,15661423,31322867,62645741,125291483,250582987,501165979,1002331963,
      2004663929,

      //chunk #4
      1039,2081,4177,8363,16729,33461,66923,133853,267713,535481,1070981,2141977,4283963,
      8567929,17135863,34271747,68543509,137087021,274174111,548348231,1096696463,

      //chunk #5
      31,67,137,277,557,1117,2237,4481,8963,17929,35863,71741,143483,286973,573953,
      1147921,2295859,4591721,9183457,18366923,36733847,73467739,146935499,293871013,
      587742049,1175484103,

      //chunk #6
      599,1201,2411,4831,9677,19373,38747,77509,155027,310081,620171,1240361,2480729,
      4961459,9922933,19845871,39691759,79383533,158767069,317534141,635068283,1270136683,

      //chunk #7
      311,631,1277,2557,5119,10243,20507,41017,82037,164089,328213,656429,1312867,
      2625761,5251529,10503061,21006137,42012281,84024581,168049163,336098327,672196673,
      1344393353,

      //chunk #8
      3,7,17,37,79,163,331,673,1361,2729,5471,10949,21911,43853,87719,175447,350899,
      701819,1403641,2807303,5614657,11229331,22458671,44917381,89834777,179669557,
      359339171,718678369,1437356741,

      //chunk #9
      43,89,179,359,719,1439,2879,5779,11579,23159,46327,92657,185323,370661,741337,
      1482707,2965421,5930887,11861791,23723597,47447201,94894427,189788857,379577741,
      759155483,1518310967,

      //chunk #10
      379,761,1523,3049,6101,12203,24407,48817,97649,195311,390647,781301,1562611,
      3125257,6250537,12501169,25002389,50004791,100009607,200019221,400038451,800076929,
      1600153859
  };

  static { //initializer
      // The above prime numbers are formatted for human readability.
      // To find numbers fast, we sort them once and for all.

      Arrays.sort(primeCapacities);
  }

  /**
   * Returns a prime number which is <code>&gt;= desiredCapacity</code>
   * and very close to <code>desiredCapacity</code> (within 11% if
   * <code>desiredCapacity &gt;= 1000</code>).
   *
   * @param desiredCapacity the capacity desired by the user.
   * @return the capacity which should be used for a hashtable.
   */
  public static final int nextPrime(int desiredCapacity) {
      int i = Arrays.binarySearch(primeCapacities, desiredCapacity);
      if (i<0) {
          // desired capacity not found, choose next prime greater
          // than desired capacity
          i = -i -1; // remember the semantics of binarySearch...
      }
      return primeCapacities[i];
  }
}
final class HashFunctions {
  /**
   * Returns a hashcode for the specified value.
   *
   * @return  a hash code value for the specified value.
   */
  public static int hash(double value) {
      assert !Double.isNaN(value) : "Values of NaN are not supported.";

      long bits = Double.doubleToLongBits(value);
      return (int)(bits ^ (bits >>> 32));
      //return (int) Double.doubleToLongBits(value*663608941.737);
      //this avoids excessive hashCollisions in the case values are
      //of the form (1.0, 2.0, 3.0, ...)
  }

  /**
   * Returns a hashcode for the specified value.
   *
   * @return  a hash code value for the specified value.
   */
  public static int hash(float value) {
      return Float.floatToIntBits(value*663608941.737f);
  }

  /**
   * Returns a hashcode for the specified value.
   *
   * @return  a hash code value for the specified value.
   */
  public static int hash(int value) {
      return value;
  }

  /**
   * Returns a hashcode for the specified value.
   *
   * @return  a hash code value for the specified value.
   */
  public static int hash(long value) {
      return ((int)(value ^ (value >>> 32)));
  }


  /**
   * In profiling, it has been found to be faster to have our own local implementation
   * of "ceil" rather than to call to {@link Math#ceil(double)}.
   */
  public static int fastCeil( float v ) {
      int possible_result = ( int ) v;
      if ( v - possible_result > 0 ) possible_result++;
      return possible_result;
  }
}

class Constants {

  /** the default capacity for new collections */
  public static final int DEFAULT_CAPACITY = 10;

  /** the load above which rehashing occurs. */
  public static final float DEFAULT_LOAD_FACTOR = 0.5f;


}

