package org.kiji.modeling.framework

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop
import org.slf4j.{LoggerFactory, Logger}
import com.twitter.scalding.{Args, Mode}

/**
 * This is the runner class passed to the express jar command.
 * The first argument must be the name of the NewModelingJob to run.
 * The remaining arguments are those passed to the job that will be run.
 *
 * Example:
 * express jar /path/to/jar package.name.JobRunner package.name.ExampleJob --local
 */
class JobRunner extends hadoop.conf.Configured with hadoop.util.Tool {
  private val logger: Logger = LoggerFactory.getLogger(classOf[JobRunner])

  override def run(args: Array[String]): Int = {
    require(args.size > 0, "Please supply the name of the job to run.")
    val jobName = args(0)
    // Remove the job name from the positional arguments:
    val nonJobNameArgs = args.tail
    logger.debug(jobName)
    nonJobNameArgs.map( logger.debug )

    val (mode, parsedArgs) = parseModeArgs(nonJobNameArgs)
    // Uses reflection to create a job by name
    Class.forName(jobName)
      .getConstructor(classOf[Mode], classOf[Args])
      .newInstance(mode, parsedArgs)

    return 0
  }

  protected def nonHadoopArgsFrom(args : Array[String]) : Array[String] = {
    new hadoop.util.GenericOptionsParser(getConf, args).getRemainingArgs
  }

  def parseModeArgs(args : Array[String]) : (Mode, Args) = {
    val a = Args(nonHadoopArgsFrom(args))
    (Mode(a, getConf), a)
  }
}

object JobRunner {
  def main(args: Array[String]) {
    ToolRunner.run(HBaseConfiguration.create(),  new JobRunner, args)
  }
}