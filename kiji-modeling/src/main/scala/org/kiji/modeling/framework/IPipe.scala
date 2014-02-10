package org.kiji.modeling.framework

import com.twitter.scalding._
import cascading.pipe.Pipe
import cascading.flow.FlowDef
import java.util.{Map => JMap}
import com.twitter.scalding.Tsv

/**
 * Extends the functionality of a RichPipe or Pipe in a way that the
 * following methods trigger a run of the flow:
 * - collect
 * - write
 * These are the only two available ways to "finish" your flow.
 * By triggering the run of a flow, we can ensure that multiple flows
 * can be run within the same KijiModelingJob, thereby supporting
 * iteration.
 */
class IPipe(val pipe: Pipe) extends TupleConversions {
  implicit def p2rp(pipe : Pipe) = new RichPipe(pipe)

  /**
   * Triggers the run of this pipe (and any other heads that it needs).
   * Ensures that the result is read back into a local, client side
   * variable. Use this to write iterative jobs.
   *
   * @param kmc is the context for this Modeling Job.
   * @return a sequence containing the results. Currently uses TextLine
   *     to read the results back. Thus the results will be available as
   *     a tuple of (offset: Long, line: String).
   */
  def collect(kmc: KijiModelingContext): Seq[(Long, String)] = {
    println("Calling collect on pipe name " + pipe.getName)
    val mode: Mode = kmc.mode
    val config: Map[AnyRef, AnyRef] = kmc.makeConfig()
    // find the source for this pipe
    implicit val flowDef: FlowDef = new FlowDef
    val sources = flowDef.getSources.asInstanceOf[JMap[String,Any]]
    pipe.getHeads.foreach {
      p => if (!sources.containsKey(p.getName)) {
        sources.put(p.getName, kmc.flowDef.getSources.get(p.getName))
      }
    }
    pipe.write(TextLine("temp"))
    mode.newFlowConnector(config).connect(flowDef).complete
    val t = TextLine("temp").readAtSubmitter[(Long, String)]
    t
  }

    /**
     * Triggers the run of this pipe (and any other heads that it needs).
     */
  def write(kmc: KijiModelingContext, sink: Source) {
    println("Calling write on pipe named " + pipe.getName)
    val mode: Mode = kmc.mode
    val config: Map[AnyRef, AnyRef] = kmc.makeConfig()
    implicit val flowDef: FlowDef = new FlowDef
    val sources = flowDef.getSources.asInstanceOf[JMap[String,Any]]
    pipe.getHeads.foreach {
      p => if (!sources.containsKey(p.getName)) {
        sources.put(p.getName, kmc.flowDef.getSources.get(p.getName))
      }
    }
    sink.writeFrom(pipe)(flowDef, mode)
    mode.newFlowConnector(config).connect(flowDef).complete
  }
}
