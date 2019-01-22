//package spark_mllib.spamfiltering.dfds
//
//import	spark.implicits._
//
///**
//  * Created by hussain on 1/6/18.
//  */
//object Test1 {
//
//
//  sealed	trait	Category
//  case	object	Scientific	extends	Category
//  case	object	NonScientific	extends	Category
//  //	FIXME:	Define	schema	for	Category
//  case	class	LabeledText(id:	Long,	category:	Category,	text:	String)
//  val	data	=	Seq(LabeledText(0,	Scientific,	"hello	world"),LabeledText(1,	NonScientific,	"witaj	swiecie")).toDF
//  	data.show
//
//  case	class	Article(id:	Long,	topic:	String,	text:	String)
//  val	articles	=	Seq(
//    Article(0,	"sci.math",	"Hello,	Math!"),
//    Article(1,	"alt.religion",	"Hello,	Religion!"),
//    Article(2,	"sci.physics",	"Hello,	Physics!"),
//    Article(3,	"sci.math",	"Hello,	Math	Revised!"),
//    Article(4,	"sci.math",	"Better	Math"),
//    Article(5,	"alt.religion",	"TGIF")).toDS
//
//  articles.show
//
//  val	topic2Label:	Boolean	=>	Double	=	isSci	=>	if	(isSci)	1	else	0
//  val	toLabel	=	udf(topic2Label)
//  val	labelled	=	articles.withColumn("label",	toLabel($"topic".like("sci%"))).cache
//  val	Array(trainDF,	testDF)	=	labelled.randomSplit(Array(0.75,	0.25))
//
//  trainDF.show
//
//  testDF.show
//
//
//  import	org.apache.spark.ml.feature.RegexTokenizer
//  val	tokenizer	=	new	RegexTokenizer()
//    .setInputCol("text")
//    .setOutputCol("words")
//  import	org.apache.spark.ml.feature.HashingTF
//  val	hashingTF	=	new	HashingTF()
//    .setInputCol(tokenizer.getOutputCol)		//	it	does	not	wire	transformers	--	it's	just a	column	name
//    .setOutputCol("features")
//    .setNumFeatures(5000)
//  import	org.apache.spark.ml.classification.LogisticRegression
//  val	lr	=	new	LogisticRegression().setMaxIter(20).setRegParam(0.01)
//  import	org.apache.spark.ml.Pipeline
//  val	pipeline	=	new	Pipeline().setStages(Array(tokenizer,	hashingTF,	lr))
//
//
//  val	model	=	pipeline.fit(trainDF)
//  val	trainPredictions	=	model.transform(trainDF)
//  val	testPredictions	=	model.transform(testDF)
//
//  	trainPredictions.select('id,	'topic,	'text,	'label,	'prediction).show
//
//
//  trainPredictions.printSchema
//
//  import	org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
//  val	evaluator	=	new	BinaryClassificationEvaluator().setMetricName("areaUnderROC")
//  import	org.apache.spark.ml.param.ParamMap
//  val	evaluatorParams	=	ParamMap(evaluator.metricName	->	"areaUnderROC")
//  scala>	val	areaTrain	=	evaluator.evaluate(trainPredictions,	evaluatorParams)
//  areaTrain:	Double	=	1.0
//  scala>	val	areaTest	=	evaluator.evaluate(testPredictions,	evaluatorParams)
//  areaTest:	Double	=	0.6666666666666666
//
//
//
//  import	org.apache.spark.ml.tuning.ParamGridBuilder
//  val	paramGrid	=	new	ParamGridBuilder()
//    .addGrid(hashingTF.numFeatures,	Array(100,	1000))
//    .addGrid(lr.regParam,	Array(0.05,	0.2))
//    .addGrid(lr.maxIter,	Array(5,	10,	15))
//    .build
//  //	That	gives	all	the	combinations	of	the	parameters
//  paramGrid:	Array[org.apache.spark.ml.param.ParamMap]	=
//  Array({
//    logreg_cdb8970c1f11-maxIter:	5,
//    hashingTF_8d7033d05904-numFeatures:	100,
//    logreg_cdb8970c1f11-regParam:	0.05
//  },	{
//    logreg_cdb8970c1f11-maxIter:	5,
//    hashingTF_8d7033d05904-numFeatures:	1000,
//    logreg_cdb8970c1f11-regParam:	0.05
//  },	{
//    logreg_cdb8970c1f11-maxIter:	10,
//    hashingTF_8d7033d05904-numFeatures:	100,
//    logreg_cdb8970c1f11-regParam:	0.05
//  },	{
//    logreg_cdb8970c1f11-maxIter:	10,
//    hashingTF_8d7033d05904-numFeatures:	1000,
//    logreg_cdb8970c1f11-regParam:	0.05
//  },	{
//    logreg_cdb8970c1f11-maxIter:	15,
//    hashingTF_8d7033d05904-numFeatures:	100,
//    logreg_cdb8970c1f11-regParam:	0.05
//  },	{
//    logreg_cdb8970c1f11-maxIter:	15,
//    hashingTF_8d7033d05904-numFeatures:	1000,
//    logreg_cdb8970c1f11-...
//    import	org.apache.spark.ml.tuning.CrossValidator
//    import	org.apache.spark.ml.param._
//    val	cv	=	new	CrossValidator()
//      .setEstimator(pipeline)
//      .setEstimatorParamMaps(paramGrid)
//      .setEvaluator(evaluator)
//      .setNumFolds(10)
//    val	cvModel	=	cv.fit(trainDF)
//
//    val	cvPredictions	=	cvModel.transform(testDF)
//    	cvPredictions.select('topic,	'text,	'prediction).show
//
//    evaluator.evaluate(cvPredictions,	evaluatorParams)
//
//    val	bestModel	=	cvModel.bestModel
//
//    cvModel.write.overwrite.save("model")
//}
