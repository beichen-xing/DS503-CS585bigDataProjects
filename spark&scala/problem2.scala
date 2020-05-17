package org.apache.spark.cs508

import org.apache.spark.SparkContext

object project3 {
  def problem2(): Unit = {
    val sc = new SparkContext()
    val Path = "hdfs://localhost:9000/user/bxing/Points.csv"
    val points = sc.textFile(Path)

    def getCellID(x: Int, y: Int): Int = {
      val cell_x = ((x - 1) / 20) + 1
      val cell_y = ((y - 1) / 20) + 1
      return cell_x + (500 - cell_y) * 500
    }

    val getNeighborID: Int => Array[Int] = id => {
      val upLeft = id - 501
      val up = id - 500
      val upRight = id - 499
      val right = id + 1
      val lowRight = id + 501
      val low = id + 500
      val lowLeft = id + 499
      val left = id - 1

      // boundary condition
      if (id == 0) {
        Array(right, lowRight, low)
      }
      if (id == 500) {
        Array(low, lowLeft, left)
      }
      if (id == 249501) {
        Array(up, upRight, right)
      }
      if (id == 250000) {
        Array(upLeft, up, left)
      }
      else if (id % 500 == 1) {
        Array(up, upRight, right, lowRight, low)
      }
      else if (id % 500 == 0) {
        Array(upLeft, up, low, lowLeft, left)
      }
      else {
        Array(upLeft, up, upRight, right, lowRight, low, lowLeft, left)
      }
    }

    val point = points.map(x => {
      val indexes = x.split(",")
      (indexes(0).toInt, indexes(1).toInt)
    })


    // count points in each cell
    val pointsInCell = point.map(x => {
      (getCellID(x._1, x._2), 1)
    })
      .reduceByKey((x, y) => x + y)


    // count how many neighbours belong to each cell
    val neighborsCount = pointsInCell.map(x => {
      val cellID = x._1
      val neigCount = getNeighborID(cellID).length
      (cellID, neigCount)
    })


    // count points in each neighbour
    val cellNeigCount = pointsInCell.flatMap(x => {
      val cellID = x._1
      val cellNeigID = getNeighborID(cellID)
      for (eachNeigID <- cellNeigID)
        yield (eachNeigID, cellID)
    }).leftOuterJoin(pointsInCell)


    // count points of neighbours in total
    val NeigTotalCount = cellNeigCount.map(x => {
      val cellID = x._2._1
      val cellNeigPointCount = if (x._2._2 == None) 0 else x._2._2.get
      (cellID, cellNeigPointCount)
    }).reduceByKey((x, y) => (x + y))


    //taskb
    val NeigAvgCount = NeigTotalCount.join(neighborsCount).map(x => (x._1, (x._2._1.toDouble / x._2._2.toDouble)))
    //NeigAvgCount.take(5)

    val cellDensity = pointsInCell.join(NeigAvgCount).map(x => (x._1, (x._2._1.toDouble / x._2._2.toDouble))).sortBy(-_._2)
    // cellDensity.take(10)

    val top16 = sc.parallelize(cellDensity.take(16))
    top16.foreach(println)

    // taskc
    val res = top16.flatMap(x => {
      val cellNeig = getNeighborID(x._1)
      for (oneNeig <- cellNeig)
        yield (oneNeig, (x._1, x._2, cellNeig.length))
    }).leftOuterJoin(cellDensity).map {
      case (neigID, ((cellID, cellDensity, neigCount), neigDensity)) => {
        (cellID, cellDensity, neigCount, neigID, neigDensity.get)
      }
    }.sortBy(_._1)

    res.collect().foreach(println)

    // taskd
    var popCount = pointsInCell.map(x => {
      (x._2, (1, Array(x._1)))
    }).reduceByKey((a, b) => (a._1 + b._1, (a._2 ++ b._2))).sortBy(_._2._1)

    val popNei = popCount.map(x => {
      val cID = x._1
      val sameIDs = x._2._2
      val len = sameIDs.length
      var A = Array(1)
      for (i <- 1 to len) {
        var neis = getNeighborID(sameIDs(i))
        for (j <- i + 1 to len) {
          if (neis.contains(sameIDs(j))) {
            A = A ++ Array(sameIDs(i), sameIDs(j))
          }
        }
      }
      if (A.length > 1)
        (cID, A.length - 1, A.drop(1))
      else
        (cID, 0, A.drop(1))
    })
    val result = popNei.collect()
  }
}
