package org.kiji.modeling.lib

import org.kiji.modeling.framework.{KijiModelingContext, NewModelingJob}
import com.twitter.scalding.{Args, Mode, TextLine}
import cascading.pipe.Pipe
import scala.io.{Source => scalaSource }
import java.io.{PrintWriter, File}

/**
 * An example job that uses the NewModelingJob API to implement an
 * iterative job; in this case KMeans.
 * 
 * This is the standard Lloyd's Algorithm.
 *
 * Run this as:
 * express jar $MODELINGJAR org.kiji.modeling.framework.JobRunner \
 * org.kiji.modeling.lib.KMeansExample --input /Users/renuka/random/t1 \
 * --centroids /Users/renuka/random/centroids --output /Users/renuka/random/output \
 * --newcentroids /Users/renuka/random/newcentroids
 *
 * Example
 *
 * Input Data:
 * 1.0 1.0
 * 1.5 2.0
 * 3.0 4.0
 * 5.0 7.0
 * 3.5 5.0
 * 4.5 5.0
 * 3.5 4.5
 *
 * Initial Centroids
 * 1.0 1.0
 * 5.0 7.0
 */
class KMeansExample(mode: Mode, args: Args) extends NewModelingJob(mode, args) {
  val mc = KijiModelingContext(mode, args)

    /**
     * Convert the input from lines in a text file to doubles
     * Every line in the input text file represents a single data point
     * The pipe now contains a single field called 'fieldVector, whose type
     * is an IndexedSeq of Doubles.
     */
  var inputData: Pipe = mc.getPipeFor(TextLine(args("input")))
      .mapTo('line -> 'fieldVector) {
        line: String => Vector(line.split("\\s+").map { _.toDouble }: _*)
      }

  /**
   * Read initial centroids from a local file.
   * Done this intentionally to demonstrate client side operations.
   */
  var centroids: IndexedSeq[Vector[Double]] = scalaSource
      .fromFile(args("centroids")).getLines().map {
        line: String => Vector(line.split("\\s+").map { _.toDouble }: _*)
      }
      .toIndexedSeq

  println("Initial centroids:\n" + centroids.toString)

  /**
   * Find L2 norm between vectors
   * @param v1 First vector
   * @param v2 Second vector
   * @return the L2 norm
   */
  def euclideanDistance(v1: Vector[Double], v2: Vector[Double]): Double = {
    math.sqrt(
      v1.zip(v2)
        .map { element => math.pow(element._1 - element._2, 2) }
        .reduce { _ + _}
    )
  }

  /**
   * Find the closest centroid to this datapoint.
   *
   * @param datapoint The input datapoint as a vector of Doubles.
   * @param centroids The existing centroids.
   * @return The index of the closest datapoint.
   */
  def findClosestCentroid(datapoint: Vector[Double], centroids: IndexedSeq[Vector[Double]]): Int = {
    val distances: IndexedSeq[Double] = centroids.map {
      centroid: Vector[Double] => euclideanDistance(centroid, datapoint)
    }
    distances.indexOf(distances.min)
  }

  /**
   * Calculate the average of the datapoints to find the new centroid.
   *
   * @param pointList List of data points mapped to this centroid.
   * @return the new centroid corresponding to the average of the new points.
   */
  def getNewCentroid(pointList: Seq[Vector[Double]]): Vector[Double] = {
    val numVectors = pointList.size
    val total: Vector[Double] = pointList.reduce (
        (a, b) => a.zip(b).map { case (a,b) => a + b }
    )
    total.map { element: Double => element/numVectors }
  }

  var convergence = Double.PositiveInfinity
  var dataMappings: Pipe = null

  do {
    dataMappings = inputData
        // Add a field called 'closestCentroid which contains the index of the closest centroid
        // for each data point
        .map('fieldVector -> 'closestCentroid) {
          datapoint: Vector[Double] => findClosestCentroid(datapoint, centroids)
        }
        .debug

    var newCentroidsRaw = dataMappings
        .groupBy('closestCentroid) { _.toList[Vector[Double]]('fieldVector -> 'fieldVectors) }
        .mapTo(('closestCentroid, 'fieldVectors) -> ('centroidIndex, 'newCentroid)) {
          tuple: (Int, List[Vector[Double]]) => {
            val (centroidNumber: Int, pointList: List[Vector[Double]]) = tuple
            (centroidNumber, getNewCentroid(pointList))
          }
        }
        .map('newCentroid -> 'newCentroid) { v: Vector[Double] => v.mkString("\t") }
        .debug
        .collect(mc)

    /**
     * The new centroids are available as Seq(line, offset). Convert this to a sequence of
     * Vector[Double]
     */
    var newCentroids: IndexedSeq[Vector[Double]] = newCentroidsRaw
        .map {
          case (offset: Long, line: String) => {
            val v: Vector[Double] = Vector(line.split("\\s+").map { _.toDouble }: _*)
            (v.head, v.tail)
          }
        }
        .sortBy ( _._1 )
        .map ( _._2 )
        .toIndexedSeq

    println("new centroids\n" + newCentroids.toString())

    /**
     * Check whether centroids have moved
     */
    convergence = newCentroids.zip(centroids).map {
      case (v1: Vector[Double], v2: Vector[Double]) => euclideanDistance(v1, v2)
    }.sum

    println("Convergence: " + convergence)
    /**
     * Write out the new centroids
     */
    val writer = new PrintWriter(new File(args("newcentroids")))
    writer.write(newCentroids.toString)
    writer.close

    centroids = newCentroids
  } while (convergence > 0.001)

  dataMappings.write(mc, TextLine(args("output")))
}
