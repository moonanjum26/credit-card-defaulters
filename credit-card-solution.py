import os
import sys
if sys.platform.startswith('win'):
    os.chdir("D:\SparkPythonDoBigDataAnalytics-Resources")
    os.environ['SPARK_HOME'] = 'C:\spark-2.0.0-bin-hadoop2.7\spark-2.0.0-bin-hadoop2.7'

os.curdir
SPARK_HOME = os.environ['SPARK_HOME']
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.1-src.zip"))
from pyspark.sql import SparkSession
from pyspark import SparkContext
SpSession = SparkSession \
    .builder \
    .master("local") \
    .appName("mahwish") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir", "file:/c:/temp/spark-warehouse")\
    .getOrCreate()
SpContext = SpSession.sparkContext

ccRaw = SpContext.textFile("credit-card-default-1000.csv")
ccRaw.take(5)

#Remove header row
dataLines = ccRaw.filter(lambda x: "EDUCATION" not in x)
dataLines.count()
dataLines.take(1000)
filteredLines = dataLines.filter(lambda x : x.find("aaaaaa") < 0 )
filteredLines.count()

cleanedLines = filteredLines.map(lambda x: x.replace("\"", ""))
cleanedLines.count()
cleanedLines.cache()

from pyspark.sql import Row

def convertToRow(instr) :
    attList = instr.split(",")
 
    
    ageRound = round(float(attList[5]) / 10.0) * 10
    
    
    sex = attList[2]
    if sex =="M":
        sex=1
    elif sex == "F":
        sex=2
    
    
    avgBillAmt = (float(attList[12]) +  \
                    float(attList[13]) + \
                    float(attList[14]) + \
                    float(attList[15]) + \
                    float(attList[16]) + \
                    float(attList[17]) ) / 6.0
                    
    
    avgPayAmt = (float(attList[18]) +  \
                    float(attList[19]) + \
                    float(attList[20]) + \
                    float(attList[21]) + \
                    float(attList[22]) + \
                    float(attList[23]) ) / 6.0
                    
    
    avgPayDuration = round((abs(float(attList[6])) + \
                        abs(float(attList[7])) + \
                        abs(float(attList[8])) +\
                        abs(float(attList[9])) +\
                        abs(float(attList[10])) +\
                        abs(float(attList[11]))) / 6)
    
                    
    perPay = round((avgPayAmt/(avgBillAmt+1) * 100) / 25) * 25
                    
    values = Row (  CUSTID = attList[0], \
                    LIMIT_BAL = float(attList[1]), \
                    SEX = float(sex),\
                    EDUCATION = float(attList[3]),\
                    MARRIAGE = float(attList[4]),\
                    AGE = float(ageRound), \
                    AVG_PAY_DUR = float(avgPayDuration),\
                    AVG_BILL_AMT = abs(float(avgBillAmt)), \
                    AVG_PAY_AMT = float(avgPayAmt), \
                    PER_PAID= abs(float(perPay)), \
                    DEFAULTED = float(attList[24]) 
                    )

    return values

  
ccRows = cleanedLines.map(convertToRow)
ccRows.take(60)

ccDf = SpSession.createDataFrame(ccRows)
ccDf.cache()
ccDf.show(10)


import pandas as pd


genderDict = [{"SEX" : 1.0, "SEX_NAME" : "Male"}, \
                {"SEX" : 2.0, "SEX_NAME" : "Female"}]                
genderDf = SpSession.createDataFrame(pd.DataFrame(genderDict, \
            columns=['SEX', 'SEX_NAME']))
genderDf.collect()
ccDf1 = ccDf.join( genderDf, ccDf.SEX== genderDf.SEX ).drop(genderDf.SEX)
ccDf1.take(5)


eduDict = [{"EDUCATION" : 1.0, "ED_STR" : "Graduate"}, \
                {"EDUCATION" : 2.0, "ED_STR" : "University"}, \
                {"EDUCATION" : 3.0, "ED_STR" : "High School" }, \
                {"EDUCATION" : 4.0, "ED_STR" : "Others"}]                
eduDf = SpSession.createDataFrame(pd.DataFrame(eduDict, \
            columns=['EDUCATION', 'ED_STR']))
eduDf.collect()
ccDf2 = ccDf1.join( eduDf, ccDf1.EDUCATION== eduDf.EDUCATION ).drop(eduDf.EDUCATION)
ccDf2.take(5)


marrDict = [{"MARRIAGE" : 1.0, "MARR_DESC" : "Single"}, \
                {"MARRIAGE" : 2.0, "MARR_DESC" : "Married"}, \
                {"MARRIAGE" : 3.0, "MARR_DESC" : "Others"}]                
marrDf = SpSession.createDataFrame(pd.DataFrame(marrDict, \
            columns=['MARRIAGE', 'MARR_DESC']))
marrDf.collect()
ccFinalDf = ccDf2.join( marrDf, ccDf2.MARRIAGE== marrDf.MARRIAGE ).drop(marrDf.MARRIAGE)
ccFinalDf.cache()
ccFinalDf.take(5)
ccFinalDf.createOrReplaceTempView("CCDATA")


SpSession.sql("SELECT SEX_NAME, count(*) as Total, " + \
                " SUM(DEFAULTED) as Defaults, " + \
                " ROUND(SUM(DEFAULTED) * 100 / count(*)) as PER_DEFAULT " + \
                "FROM CCDATA GROUP BY SEX_NAME"  ).show()

              
SpSession.sql("SELECT MARR_DESC, ED_STR, count(*) as Total," + \
                " SUM(DEFAULTED) as Defaults, " + \
                " ROUND(SUM(DEFAULTED) * 100 / count(*)) as PER_DEFAULT " + \
                "FROM CCDATA GROUP BY MARR_DESC,ED_STR " + \
                "ORDER BY 1,2").show()
               
SpSession.sql("SELECT AVG_PAY_DUR, count(*) as Total, " + \
                " SUM(DEFAULTED) as Defaults, " + \
                " ROUND(SUM(DEFAULTED) * 100 / count(*)) as PER_DEFAULT " + \
                "FROM CCDATA GROUP BY AVG_PAY_DUR ORDER BY 1"  ).show()


for i in ccDf.columns:
    if not( isinstance(ccDf.select(i).take(1)[0][0], str)) :
        print( "Correlation to DEFAULTED for ", i,\
            ccDf.stat.corr('DEFAULTED',i))

import math
from pyspark.ml.linalg import Vectors

def transformToLabeledPoint(row) :
    lp = ( row["DEFAULTED"], \
            Vectors.dense([
                row["AGE"], \
                row["AVG_BILL_AMT"], \
                row["AVG_PAY_AMT"], \
                row["AVG_PAY_DUR"], \
                row["EDUCATION"], \
                row["LIMIT_BAL"], \
                row["MARRIAGE"], \
                row["PER_PAID"], \
                row["SEX"]
        ]))
    return lp
    
ccLp = ccFinalDf.rdd.repartition(2).map(transformToLabeledPoint)
ccLp.collect()
ccNormDf = SpSession.createDataFrame(ccLp,["label", "features"])
ccNormDf.select("label","features").show(10)
ccNormDf.cache()

from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(ccNormDf)
td = si_model.transform(ccNormDf)
td.collect()
(trainingData, testData) = td.randomSplit([0.7, 0.3])
trainingData.count()
testData.count()

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="indexed",metricName="accuracy")


dtClassifer = DecisionTreeClassifier(labelCol="indexed", \
                featuresCol="features")
dtModel = dtClassifer.fit(trainingData)

predictions = dtModel.transform(testData)
predictions.select("prediction","indexed","label","features").collect()
print("Results of Decision Trees : ",evaluator.evaluate(predictions))      


rmClassifer = RandomForestClassifier(labelCol="indexed", \
                featuresCol="features")
rmModel = rmClassifer.fit(trainingData)

predictions = rmModel.transform(testData)
predictions.select("prediction","indexed","label","features").collect()
print("Results of Random Forest : ",evaluator.evaluate(predictions)  )


nbClassifer = NaiveBayes(labelCol="indexed", \
                featuresCol="features")
nbModel = nbClassifer.fit(trainingData)
#Predict on the test data
predictions = nbModel.transform(testData)
predictions.select("prediction","indexed","label","features").collect()
print("Results of Naive Bayes : ",evaluator.evaluate(predictions)  )

ccClustDf = ccFinalDf.select("SEX","EDUCATION","MARRIAGE","AGE","CUSTID")


summStats=ccClustDf.describe().toPandas()
meanValues=summStats.iloc[1,1:5].values.tolist()
stdValues=summStats.iloc[2,1:5].values.tolist()
bcMeans=SpContext.broadcast(meanValues)
bcStdDev=SpContext.broadcast(stdValues)

def centerAndScale(inRow) :
    global bcMeans
    global bcStdDev
    
    meanArray=bcMeans.value
    stdArray=bcStdDev.value

    retArray=[]
    for i in range(len(meanArray)):
        retArray.append( (float(inRow[i]) - float(meanArray[i])) /\
            float(stdArray[i]) )
    return Row(CUSTID=inRow[4], features=Vectors.dense(retArray))
    
ccMap = ccClustDf.rdd.repartition(2).map(centerAndScale)
ccMap.collect()
ccFClustDf = SpSession.createDataFrame(ccMap)
ccFClustDf.cache()

ccFClustDf.select("features").show(10)

from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=4, seed=1)
model = kmeans.fit(ccFClustDf)
predictions = model.transform(ccFClustDf)
predictions.select("*").show()