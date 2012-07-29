package test.analytics;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.DecisionTree;


public class DecisionTreeTest {
  
  public DecisionTreeTest() {
  }

  @BeforeClass public static void setUpClass() throws Exception { }

  @AfterClass public static void tearDownClass() throws Exception { }
  
  @Before public void setUp() { }
  
  @After public void tearDown() { }

  @Test public void testLeafNode() {
    DecisionTree.LeafNode n = new DecisionTree.LeafNode(1);
    assertEquals(n.classify(null),1);
    assertEquals(n.numClasses(),1);
  }
  
  @Test public void testNode() {
  }
  
  @Test public void testClassify() {
  }
  
  @Test public void testNumClasses() {
  }
  
}
