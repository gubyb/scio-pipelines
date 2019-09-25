package join

import com.spotify.scio._

object JoinFiles {
  def main(cmdlineArgs: Array[String]): Unit = {

    val streamsHeader = Array("id", "user_id", "track_id")
    val recsHeader = Array("id", "user_id", "track_id")

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val streams = args("streams")
    val recs = args("recs")
    val output = args("output")

    // Read and parsecsv
    val streamsSc = sc.textFile(streams)
      .map(_.trim)
      .map(_.split(",").filter(_.nonEmpty))
      .filter(l => !streamsHeader.sameElements(l))
      .map(l => streamsHeader.zip(l).toMap)
      .map(row => ((row("user_id"), row("track_id")), row))

    val recsSc = sc.textFile(recs)
      .map(_.trim)
      .map(_.split(",").filter(_.nonEmpty))
      .filter(l => !recsHeader.sameElements(l))
      .map(l => recsHeader.zip(l).toMap)
      .map(row => ((row("user_id"), row("track_id")), row))

    // Join and format result object
    val resultSC = streamsSc
      .leftOuterJoin(recsSc)
      .map{ row =>
        val (_, (stream, rec)) = row
        stream + ("is_rec" -> (if (rec.isEmpty) false else true))
      }

    //Print result
    resultSC
      .map(_.values.mkString(","))
      .saveAsTextFile("output/streams", numShards = 0, suffix = ".csv")

    val result = sc.close().waitUntilFinish()
  }
}
