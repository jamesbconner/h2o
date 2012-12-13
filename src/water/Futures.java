package water;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;

// A collection of Futures.  We can add more, or block on the whole collection.
// Undefined if you try to add Futures while blocking.
//
// @author <a href="mailto:cliffc@0xdata.com"></a>
// @version 1.0
public class Futures extends RecursiveAction {
  // As a service to sub-tasks, collect pending-but-not-yet-done future tasks,
  // that need to complete prior to *this* task completing... or if the caller
  // of this task is knowledgable, pass these pending tasks along to him to
  // block on before he completes.

  // I am implementing this as an exposed array mostly because I need proper
  // synchronization and the ArrayList API doesn't offer the right level of
  // sync or constant-time removal.
  Future[] _pending;
  int _pending_cnt;

  // Some Future task which needs to complete before this task completes
  synchronized public Futures add( Future f ) {
    if( f == null ) return this;
    if( f.isDone() ) return this;
    if( _pending == null ) _pending = new Future[1];
    else if( _pending_cnt == _pending.length ) {
      clean_pending();
      if( _pending_cnt == _pending.length )
        _pending = Arrays.copyOf(_pending,_pending_cnt<<1);
    }
    _pending[_pending_cnt++] = f;
    return this;
  }

  // Merge pending-task lists as part of doing a 'reduce' step
  public void add( Futures fs ) {
    if( fs == null ) return;
    for( int i=0; i<fs._pending_cnt; i++ )
      add(fs._pending[i]);
  }

  // Clean out from the list any pending-tasks which are already done.  Note
  // that this drops the algorithm from O(n) to O(1) in practice, since mostly
  // things clean out as fast as new ones are added and the list never gets
  // very large.
  synchronized private void clean_pending() {
    for( int i=0; i<_pending_cnt; i++ )
      if( _pending[i].isDone() ) // Done?
        // Do cheap array compression to remove from list
        _pending[i--] = _pending[--_pending_cnt];
  }

  synchronized public final void block_pending() {
    try {
      while( _pending_cnt > 0 )
        // Block until the last Future finishes.  This will unlock/wait/relock
        // - so the _pending fields all change, and will be freshly reloaded
        // after returning from the blocking get() call.
        _pending[--_pending_cnt].get();
    } catch( InterruptedException ie ) {
      throw new RuntimeException(ie);
    } catch( ExecutionException ee ) {
      throw new RuntimeException(ee);
    }
  }

  public void compute() { block_pending(); }
}

