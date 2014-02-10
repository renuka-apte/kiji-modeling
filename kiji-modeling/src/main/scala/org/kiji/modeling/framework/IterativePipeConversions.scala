package org.kiji.modeling.framework

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

/**
 * Implicit conversions required to be able to use the IPipe API.
 */
trait IterativePipeConversions {
  /**
   * Converts a cascading pipe to a RichPipe.
   *
   * @param pipe to convert to a RichPipe.
   * @return a RichPipe created from the given cascading Pipe.
   */
  implicit def pipe2RichPipe(pipe : Pipe) = new RichPipe(pipe)

  /**
   * Converts a cascading pipe into a Pipe capable of supporting Iteration.
   *
   * @param pipe to convert to IPipe.
   * @return IPipe created from the given cascading Pipe.
   */
  implicit def pipe2IPipe(pipe: Pipe) = new IPipe(pipe)
}
