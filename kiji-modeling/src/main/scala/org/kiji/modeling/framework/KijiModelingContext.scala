package org.kiji.modeling.framework

import com.twitter.scalding.{Read, Source, Args, Mode}
import cascading.flow.FlowDef
import cascading.pipe.Pipe
import java.util.{Calendar, Map => JMap}

/**
 * This holds the context for a given Modeling Job. Makes it
 * so that multiple flows can be run and supports iteration.
 *
 * Basically a container for the configuration and flowdef.
 * We need a flowDef around because of the way sources are
 * created. This will not be the flowdef that is actually
 * used to run the job. By keeping this here, we have a way
 * of attaching our sources and keeping them around at the
 * beginning. When the time comes to actually run a flowdef,
 * these sources will be copied over to a new flowdef to
 * run them.
 */
class KijiModelingContext (val mode: Mode, val args: Args) {
  val flowDef: FlowDef = new FlowDef

  /**
   * Get a new pipe corresponding to this source.
   *
   * @param source A Scalding Source such as TextLine, TSV, KijiSource etc.
   * @return A Pipe connected to the flowdef.
   */
  def getPipeFor(source: Source): Pipe = {
    val srcName = source.toString

    val sources = flowDef.getSources.asInstanceOf[JMap[String,Any]]
    if (!sources.containsKey(srcName)) {
      sources.put(srcName, source.createTap(Read)(mode))
    }
    mode.getReadPipe(source, new Pipe(srcName))
  }

  /**
   * Copied straight from Scalding. Creates a config from a mode that is
   * associated with this Kiji Modeling Context.
   *
   * @return a map containing the config.
   */
  def makeConfig() : Map[AnyRef,AnyRef] = {
    /*val ioserVals = (ioSerializations ++
      List("com.twitter.scalding.serialization.KryoHadoop")).mkString(",")*/

    mode.config ++
      /*Map("io.serializations" -> ioserVals) ++
      (defaultComparator match {
        case Some(defcomp) => Map(FlowProps.DEFAULT_ELEMENT_COMPARATOR -> defcomp)
        case None => Map[String,String]()
      }) ++*/
      Map("cascading.spill.threshold" -> "100000", //Tune these for better performance
        "cascading.spillmap.threshold" -> "100000") ++
      Map("scalding.version" -> "0.9.0-SNAPSHOT",
        "scalding.flow.class.name" -> getClass.getName,
        "scalding.flow.submitted.timestamp" ->
          Calendar.getInstance().getTimeInMillis().toString
      )
  }
}

object KijiModelingContext {
  def apply(mode: Mode, args: Args): KijiModelingContext = {
    new KijiModelingContext(mode, args)
  }
}