package hexlytics.rf;

import hexlytics.rf.Tree.INode;
import java.util.LinkedList;
import java.util.Queue;


class Job {
  final INode _node; final int _direction; final Data _data;
  final Statistic _stat; final Tree _tree;
  Job(Tree t, INode n, int i, Data d, Statistic s) {
    _tree=t; _node=n; _direction=i; _data=d; _stat=s;
  }
  void run() {
    Job[] jobs = new Job[2];
    INode newnode =  _tree.compute(_node == null ? 0 : _node.nodeDepth_+1,_data, _stat, jobs);
    if (_node==null)  _tree.tree_ = newnode;
    else _node.set(_direction,newnode);
    RFTask task = (RFTask) Thread.currentThread();
    if (jobs[0]==null) return;
    task.put(jobs[0]); task.put(jobs[1]);
  }
}

class GiniJob {
  final INode _node; final int _direction; final Data _data;
  final GiniStatistic.Split _split; final Tree _tree;
  GiniJob(Tree t, INode n, int i, Data d, GiniStatistic.Split s) {
    _tree=t; _node=n; _direction=i; _data=d; _split=s;
  }
  void run() {
    RFGiniTask task = (RFGiniTask) Thread.currentThread();
    GiniJob[] jobs = new GiniJob[2];
    INode newnode =  _tree.computeGini(_node == null ? 0 : _node.nodeDepth_+1,_data, _split, jobs,task.stats_);
    if (_node==null)  _tree.tree_ = newnode;
    else _node.set(_direction,newnode);
    if (jobs[0]!=null)
      task.put(jobs[0]);
    if (jobs[1]!=null)
      task.put(jobs[1]);
  }
}



 class RFGiniTask extends Thread {
   
  final GiniStatistic[] stats_;
   
  static RFGiniTask[] _;
  static Data _data;
  static Data data() { return _data; }
  
  RFGiniTask(Data d) {
    _data=d;
    stats_ = new GiniStatistic[2];
    stats_[0] = new GiniStatistic(d);
    stats_[1] = new GiniStatistic(d);
  }
  
  Queue<GiniJob> _q = new LinkedList();
  boolean idle = false;
  @Override public void run() {
    while (true) {
      GiniJob j = take();
      if (j==null) for (RFGiniTask r : _) if ( (j = r.take()) != null) break;
      if (j!=null) j.run();
      else if(idle()) return;
    }
  }
  boolean idle() {
    idle = true;
    boolean done = true;
    for (RFGiniTask r : _) done &= r.idle;
    if (done) return true;
    try {
      sleep(100);
      } catch (Exception _){ }
    idle = false;
    return false;
  }
  synchronized void put(GiniJob j){ if (j!=null) _q.add(j); }
  synchronized GiniJob take() { return _q.isEmpty()? null : _q.remove(); }
}


class RFTask extends Thread {
  static RFTask[] _;
  static Data _data;
  static Data data() { return _data; }
  RFTask(Data d){ _data=d; }
  Queue<Job> _q = new LinkedList<Job>();
  boolean idle = false;
  public void run() {
    while (true) {
      Job j = take();
      if (j==null) for (RFTask r : _) if ( (j = r.take()) != null) break;
      if (j!=null) j.run();
      else if(idle()) return;
    }
  }
  boolean idle() {
    idle = true;
    boolean done = true;
    for (RFTask r : _) done &= r.idle;
    if (done) return true;
    try {
      sleep(100);
      } catch (Exception _){ }
    idle = false;
    return false;
  }
  synchronized void put(Job j){ if (j!=null) _q.add(j); }
  synchronized Job take() { return _q.isEmpty()? null : _q.remove(); }
}
