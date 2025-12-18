package edu.umass.cs.causal.dag;

import edu.umass.cs.xdn.request.XdnHttpRequestTest;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DirectedAcyclicGraphTest {

  @Test
  public void DirectedAcyclicGraphTest_EmptyGraphNoCycle() {
    DirectedAcyclicGraph graph = new DirectedAcyclicGraph();
    assert !graph.isCycleExist();
  }

  @Test
  public void DirectedAcyclicGraphTest_SingleNodeGraphNoCycle() {
    GraphVertex n =
        new GraphVertex(
            new VectorTimestamp(List.of("ar1", "ar2", "ar3")),
            List.of(XdnHttpRequestTest.helpCreateDummyRequest()));
    DirectedAcyclicGraph graph = new DirectedAcyclicGraph(n);
    assert !graph.isCycleExist();
  }
}
