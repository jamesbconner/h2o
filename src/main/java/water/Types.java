package water;

import water.api.Constants;
import water.util.Utils;

public abstract class Types {
  // @formatter:off
  // Cap at 2 bytes for shorter UDP packets & Timeline recording
  static final int MAX_ID = 0xffff;
  static final int NULL = MAX_ID;

  private static final Freezable[] BOOT = new Freezable[] {
    new Paxos.State(),
    new H2ONode(),
    new HeartBeat(),
    new TaskGetKey(),
    new Key(),
    new TaskPutKey(),
    new Value(),
    new Types.State(),
    new Types.AddType()
  };
  private static volatile Freezable[] _exemplars = BOOT;

  static final Key KEY = Key.make(Constants.BUILT_IN_KEY_TYPES, (byte) 0, Key.BUILT_IN_KEY);
  private static class State extends Iced {
    private String[] _names;
  }
  // @formatter:on

  private Types() {
  }

  public static Freezable newInstance(int id) {
    Freezable[] e = _exemplars;
    Freezable f = id < e.length ? e[id] : null;
    if( f == null ) {
      State state = UKV.get(KEY, new State());
      try {
        f = put(id, Class.forName(state._names[id]));
      } catch( ClassNotFoundException ex ) {
        throw new RuntimeException(ex);
      }
    }
    return f.newInstance();
  }

  // Called once by each Weaver's implementation of water.Freezable.type()
  public static int id(Class c) {
    // Special case for boot classes
    for( int i = 0; i < BOOT.length; i++ )
      if( BOOT[i].getClass() == c )
        return i;

    int id = getOrAdd(c.getName());
    put(id, c);
    return id;
  }

  // No barrier, but OK to write multiple times to same id
  private static Freezable put(int id, Class c) {
    Freezable[] e = _exemplars;
    if( id >= e.length ) {
      Freezable[] t = new Freezable[Utils.nextPowerOf2(id + 1)];
      System.arraycopy(e, 0, t, 0, e.length);
      _exemplars = e = t;
    }
    Freezable f;
    try {
      f = (Freezable) c.newInstance();
      e[id] = f;
    } catch( Exception ex ) {
      throw new RuntimeException(ex);
    }
    return f;
  }

  private static int getOrAdd(String type) {
    State state = UKV.get(KEY, new State());
    if( state != null )
      for( int i = 0; i < state._names.length; i++ )
        if( state._names[i].equals(type) )
          return i;

    AddType atomic = new AddType();
    atomic._type = type;
    atomic.invoke(KEY);
    return atomic._id;
  }

  private static class AddType extends TAtomic<State> {
    String _type;
    int    _id;

    @Override
    public State atomic(State old) {
      for( int i = 0; old != null && i < old._names.length; i++ ) {
        if( old._names[i].equals(_type) ) {
          _id = i;
          return null;
        }
      }

      State nnn = new State();
      if( old == null ) {
        Freezable[] e = _exemplars;
        nnn._names = new String[e.length + 1];
        for( int i = 0; i < e.length; i++ )
          nnn._names[i] = e[i].getClass().getName();
      } else {
        nnn._names = new String[old._names.length + 1];
        System.arraycopy(old._names, 0, nnn._names, 0, old._names.length);
      }
      _id = nnn._names.length - 1;
      assert _id <= MAX_ID;
      nnn._names[_id] = _type;
      return nnn;
    }
  }
}
