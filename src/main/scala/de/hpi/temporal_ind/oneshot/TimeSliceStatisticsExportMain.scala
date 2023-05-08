package de.hpi.temporal_ind.oneshot

object TimeSliceStatisticsExportMain extends App {
//  println(s"Called with ${args.toIndexedSeq}")
//  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
//  println(GLOBAL_CONFIG.totalTimeInDays)
//  val version = "0.7_explore_time_slice" //TODO: update this if discovery algorithm changes!
//  //TODO: continue adapting here!
//  val sourceDirs = args(0).split(",")
//    .map(new File(_))
//    .toIndexedSeq
//  val targetDir = new File(args(1) + s"/${version}/")
//  targetDir.mkdir()
//  val targetFileBinary = args(2)
//  val epsilon = args(3).toDouble
//  val deltaInDays = args(4).toLong
//  val subsetValidation = true
//  val sampleSize = 10000
//  val bloomfilterSize = 4096
//  val interactiveIndexBuilding = false
//  val dataLoader = new InputDataManager(targetFileBinary)
//  val expectedQueryParams = TINDParameters(epsilon,TimeUtil.nanosPerDay * deltaInDays,new ConstantWeightFunction())
//  val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
//    new TimeSliceImpactResultSerializer(targetDir),
//    expectedQueryParams,
//    version,
//    subsetValidation,
//    bloomfilterSize,
//    TimeSliceChoiceMethod.RANDOM,
//    13)
//
//  val resultPR = new PrintWriter(targetDir.getAbsolutePath + "/timeSliceStats.csv")
//  val (data, _) = relaxedShiftedTemporalINDDiscovery.loadData()
//  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos * epsilon).toLong
//  val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(expectedQueryParams)
//  val timeSLiceStatGatherer = new TimeSliceStatisticsExtractor(data.histories.map(_.och), allSlices, resultPR)
//  timeSLiceStatGatherer.extractForAll()

}
