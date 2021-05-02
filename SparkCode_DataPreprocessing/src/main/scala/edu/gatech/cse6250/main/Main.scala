package edu.gatech.cse6250.main

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import edu.gatech.cse6250.helper.SparkHelper
import edu.gatech.cse6250.helper.SparkHelper.spark.implicits._
import edu.gatech.cse6250.helper.CSVHelper
import java.text.SimpleDateFormat

object Main {
  def main(args: Array[String]): Unit = {

    def sqlDateParser(input: String, pattern: String = "yyyy-MM-dd HH:mm:ss"): java.sql.Date = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      new java.sql.Date(dateFormat.parse(input).getTime)
    }

    val myDateParser = udf(sqlDateParser _)

    val spark = SparkHelper.spark
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    val patientsTable = CSVHelper.loadCSVAsTable(spark, "data/PATIENTS.csv")
    val admissionsTable = CSVHelper.loadCSVAsTable(spark, "data/ADMISSIONS.csv")
    val chartTable = CSVHelper.loadCSVAsTable(spark, "data/CHARTEVENTS.csv")
    val labTable = CSVHelper.loadCSVAsTable(spark, "data/LABEVENTS.csv")
    val icuTable = CSVHelper.loadCSVAsTable(spark, "data/ICUSTAYS.csv")

    val patients = sqlContext.sql("select SUBJECT_ID, GENDER, DOB from PATIENTS".stripMargin) // total 46520 patients
    val admissions = sqlContext.sql("select SUBJECT_ID, HADM_ID, ADMITTIME, HOSPITAL_EXPIRE_FLAG from ADMISSIONS".stripMargin)
    val chart = sqlContext.sql("select SUBJECT_ID, HADM_ID, ITEMID, CHARTTIME, VALUE, VALUEUOM from CHARTEVENTS".stripMargin)
    val lab = sqlContext.sql("select SUBJECT_ID, HADM_ID, ITEMID, CHARTTIME, VALUE, VALUEUOM from LABEVENTS".stripMargin)
    val icu = sqlContext.sql("select SUBJECT_ID, HADM_ID, ICUSTAY_ID from ICUSTAYS".stripMargin)


    /**
     * filter out patients with age < 18 when admission*/
    val patientsOver18 = patients.select("SUBJECT_ID", "DOB", "GENDER").
      join(admissions, patients("SUBJECT_ID") === admissions("SUBJECT_ID"), "inner").
      withColumn("DOB_DATE", to_timestamp($"DOB")).withColumn("ADMITTIME_DATE", to_timestamp($"ADMITTIME")).
      withColumn("Age", round(($"ADMITTIME_DATE".cast(LongType) - $"DOB_DATE".cast(LongType)) / (60 * 60 * 24 * 365), 2)).
      filter($"Age" >= 18).select("patients.SUBJECT_ID", "HADM_ID", "ADMITTIME_DATE", "Age", "GENDER", "HOSPITAL_EXPIRE_FLAG").cache()
    val patientsList = patientsOver18.select("SUBJECT_ID").rdd.map(rec => rec(0)).distinct().collect().toList // left 38552 patients
    // now 50767 admissions for 38552 patients

    /** filter out patients/admissions with more than two icustays per admission*/
    val icuOneStay = icu.select("HADM_ID", "ICUSTAY_ID").groupBy("HADM_ID").count().filter($"count" === 1)
    val patientsFiltered = patientsOver18.select("SUBJECT_ID", "HADM_ID", "Age", "GENDER", "HOSPITAL_EXPIRE_FLAG").
      join(icuOneStay, patientsOver18("HADM_ID") === icuOneStay("HADM_ID"), "inner").
      select("SUBJECT_ID", "icustays.HADM_ID", "Age", "GENDER", "HOSPITAL_EXPIRE_FLAG").cache()
    val patientsFilteredList = patientsFiltered.select("SUBJECT_ID").rdd.map(rec => rec(0)).distinct().collect().toList
    val hadmList = patientsFiltered.select("HADM_ID").rdd.map(rec => rec(0)).collect().toList
    // now 46545 admissions (5086 in-hospital expire) for 36616 patients

    /** icu INTIME */
    val icuTime = icuTable.select("HADM_ID", "INTIME").filter($"HADM_ID".isin(hadmList: _*)).cache()

    /** icu INTIME join chartevents */
    val chartWithTime = chart.join(icuTime, chart("HADM_ID") === icuTime("HADM_ID")).cache() // 236,474,022 events

    /** selected features: ITEMID */
    val items = List("8377", "115", "3348", "8368", "220051", "225310", "8555", "8441", "220180", "8502", "8440",
      "8503", "8504", "8507", "8506", "224643", "3420", "223835", "3422", "189", "727", "184", "220739", "454", "223901",
      "198", "723", "223900", "807", "811", "1529", "3745", "225664", "220621", "226537", "211", "220045", "226707",
      "226730", "1394", "52", "220052", "225312", "224", "6702", "224322", "456", "220181", "3312", "3314", "3316",
      "3320", "3322", "834", "8498", "220227", "646", "220277", "618", "220210", "3603", "224689", "614", "651", "224422",
      "615", "224690", "51", "220050", "225309", "6701", "455", "220179", "3313", "3315", "442", "3317", "3321", "3323",
      "224167", "227243", "3655", "677", "676", "223762", "3654", "678", "223761", "679", "763", "224639", "226512",
      "3580", "3693", "3581", "226531", "3582", "3839", "1673", "780", "1126", "223830", "4753", "4202", "860", "220274")
    val labitems = List("50931", "50809", "51478", "50817", "50820", "51491", "50831", "51094", "51300", "51301", "50971", "50822", "51006")

    /** chartevents with selected features for selected patients or admissions */
    val chartWithTimeSelected = chartWithTime.filter($"ITEMID".isin(items: _*)).cache() // 42,363,306 events

    /** save the chartWithTimeSelected to a textFile */
    chartWithTimeSelected.drop("icustays.HADM_ID").coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/chartWithTimeSelected.csv")

    /** filter 24hrs or 48Hrs chartevents */
    val chartHours = chartWithTimeSelected.withColumn("Hours", (to_timestamp($"CHARTTIME").cast(LongType) - to_timestamp($"INTIME").cast(LongType)) / (60 * 60)).cache()
    val chart24Hr = chartHours.filter($"Hours" < 24).select("SUBJECT_ID", "chartevents.HADM_ID", "ITEMID", "CHARTTIME", "VALUE", "VALUEUOM").cache() // 11,427,063 events
    val chart48Hr = chartHours.filter($"Hours" < 48).select("SUBJECT_ID", "chartevents.HADM_ID", "ITEMID", "CHARTTIME", "VALUE", "VALUEUOM").cache() // 18,026,265 events

    chart24Hr.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/chart24Hr.csv")

    chart48Hr.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/chart48Hr.csv")

    /** read chart24Hr.csv and chart48Hr.csv */
    val chart24HrTable = CSVHelper.loadCSVAsTable(spark, "output/chart24Hr.csv") // 36456 patients, 11,427,063 events
    val chart48HrTable = CSVHelper.loadCSVAsTable(spark, "output/chart48Hr.csv") // 36471 patients, 18,026,265 events

    /** labevents with selected features for selected patients or admissions */

    val icuTimeWithID = patientsFiltered.select("SUBJECT_ID", "HADM_ID").
      join(icuTime, patientsFiltered("HADM_ID") === icuTime("HADM_ID"), "inner").
      select("SUBJECT_ID", "icustays.HADM_ID", "INTIME").cache()

    val labWithTime = lab.join(icuTimeWithID, lab("SUBJECT_ID") === icuTimeWithID("SUBJECT_ID"), "inner").
      filter($"ITEMID".isin(labitems: _*)).cache() // 36562 patients

    val labHours = labWithTime.withColumn("Hours", (to_timestamp($"CHARTTIME").cast(LongType) - to_timestamp($"INTIME").cast(LongType)) / (60 * 60)).cache()
    val lab24Hr = labHours.filter($"Hours" < 24 && $"Hours" > 0).select("labevents.SUBJECT_ID", "icustays.HADM_ID", "ITEMID", "CHARTTIME", "VALUE", "VALUEUOM").cache() // 640,669 labevents
    val lab48Hr = labHours.filter($"Hours" < 48 && $"Hours" > 0).select("labevents.SUBJECT_ID", "icustays.HADM_ID", "ITEMID", "CHARTTIME", "VALUE", "VALUEUOM").cache() // 970,944 labevents

    lab24Hr.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/lab24Hr.csv")

    lab48Hr.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/lab48Hr.csv")

    /** read lab24Hr.csv and lab48Hr.csv */
    val lab24HrTable = CSVHelper.loadCSVAsTable(spark, "output/lab24Hr.csv") // 35994 patients, 640,669 events
    val lab48HrTable = CSVHelper.loadCSVAsTable(spark, "output/lab48Hr.csv") // 36147 patients, 970,944 events


    /** Convert categorical columns to numbers */
    def catConverter(input: String) = {
      if (input == "Normal <3 secs") {"0"}
      else if (input == "Abnormal >3 secs") {"1"}
      else if (input == "Other/Remarks") {"2"}
      else if (input == "Abnormal Flexion") {"3"}
      else if (input == "Inappropriate Words") {"3"}
      else if (input == "Confused") {"4"}
      else if (input == "To Pain") {"2"}
      else if (input == "Spontaneously") {"4"}
      else if (input == "Oriented") {"5"}
      else if (input == "Incomprehensible ...") {"2"}
      else if (input == "Flex-withdraws") {"4"}
      else if (input == "Abnormal extension") {"2"}
      else if (input == "Localizes Pain") {"5"}
      else if (input == "Obeys Commands") {"6"}
      else if (input == "To Speech") {"3"}
      else if (input == "No response") {"1"}
      else if (input == "No Response") {"1"}
      else if (input == "No Response-ETT") {"1"}
      else { input.split(" ")(0)}
    }

    val catConverterUdf = udf(catConverter _)

    /** convert units */
    def unitConverter(itemId:String, v:Double): Double = {
      if (itemId == "226707" || itemId == "1394") {v*2.54} // height inch to cm
      else if (List("3654", "678", "223761", "679").contains(itemId)) {(v-32)*5/9} // temp F to C
      else if (List("3581", "226531").contains(itemId)) {v*0.45} // weight lb to kg
      else if (itemId == "3582") {v*0.02835} // weight oz to kg
      else {v}
    }
    val unitConverterUdf = udf(unitConverter _)

    /** change column types: charttime to timestamp, categorical features to number, value to Double */
    val chart24 = chart24HrTable.select("SUBJECT_ID", "HADM_ID", "ITEMID","CHARTTIME","VALUE").na.drop().
      withColumn("VALUE", catConverterUdf($"VALUE")).
      withColumn("CHARTTIME", to_timestamp($"CHARTTIME")).withColumn("VALUE", $"VALUE".cast(DoubleType)).
      withColumn("VALUE", unitConverterUdf($"ITEMID", $"VALUE"))  // 11,377,878 events, 45824 admission, 36453 patients

    val chart48 = chart48HrTable.select("SUBJECT_ID", "HADM_ID", "ITEMID","CHARTTIME","VALUE").na.drop().
      withColumn("VALUE", catConverterUdf($"VALUE")).
      withColumn("CHARTTIME", to_timestamp($"CHARTTIME")).withColumn("VALUE", $"VALUE".cast(DoubleType)).
      withColumn("VALUE", unitConverterUdf($"ITEMID", $"VALUE"))  // 17,949,246 events, 45851 admission, 36467 patients

    val lab24 = lab24HrTable.select("SUBJECT_ID", "HADM_ID", "ITEMID","CHARTTIME","VALUE").na.drop().
      withColumn("CHARTTIME", to_timestamp($"CHARTTIME")).
      withColumn("VALUE", $"VALUE".cast(DoubleType))  //640,661 events, 45503 admission, 35994 patients

    val lab48 = lab48HrTable.select("SUBJECT_ID", "HADM_ID", "ITEMID","CHARTTIME","VALUE").na.drop().
      withColumn("CHARTTIME", to_timestamp($"CHARTTIME")).
      withColumn("VALUE", $"VALUE".cast(DoubleType)) // 970,932 events, 45723 admission, 36147 patients

    val chartLab24 = chart24.union(lab24) // 12,018,539 events, 46228 admission, 36561 patients
    val chartLab48 = chart48.union(lab48) // 18,920,178 events, 46237 admission, 36565 patients

    chart24.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/chart24.csv")

    chart48.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/chart48.csv")

    chartLab24.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/chartLab24.csv")

    chartLab48.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/chartLab48.csv")

    patientsFiltered.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/patientsInfo.csv")

    /** Now the data has been cleaned */

    /** NEXT, build data for logistic model -- combine all the features belonging to the same group (total 17 groups), and calculate the mean value */
    def itemConverter(itemId:String): String = {
      if (List("8377", "115", "3348").contains(itemId)) {"CRR"}
      else if (List("8368", "220051", "225310", "8555", "8441", "220180", "8502", "8440",
        "8503", "8504", "8507", "8506", "224643").contains(itemId)) {"DBP"}
      else if (List("3420", "223835", "3422", "189", "727").contains(itemId)) {"FIO"}
      else if (List("184", "220739").contains(itemId)) {"GO"}
      else if (List("454", "223901").contains(itemId)) {"GMR"}
      else if (List("198").contains(itemId)) {"GT"}
      else if (List("723", "223900").contains(itemId)) {"GVR"}
      else if (List("50931","807", "811", "1529","50809", "51478","3745", "225664", "220621", "226537").contains(itemId)) {"GLU"}
      else if (List("211", "220045").contains(itemId)) {"HR"}
      else if (List("226707", "226730", "1394").contains(itemId)) {"HEIGHT"}
      else if (List("52", "220052", "225312", "224", "6702", "224322","456", "220181", "3312", "3314", "3316", "3320","3322").contains(itemId)) {"MBP"}
      else if (List("834","50817","8498", "220227", "646", "220277").contains(itemId)) {"OS"}
      else if (List("618", "220210", "3603", "224689", "614", "651", "224422", "615", "224690").contains(itemId)) {"RESPR"}
      else if (List("51", "220050", "225309", "6701", "455", "220179", "3313", "3315", "442", "3317", "3321", "3323",
        "224167", "227243").contains(itemId)) {"SBP"}
      else if (List("3655", "677", "676", "223762", "3654", "678", "223761", "679").contains(itemId)) {"TEMP"}
      else if (List("763", "224639", "226512","3580", "3693", "3581", "226531", "3582").contains(itemId)) {"WEIGHT"}
      else if (List("50820", "51491","3839", "1673","50831", "51094","780", "1126", "223830", "4753", "4202", "860", "220274").contains(itemId)) {"PH"}
      else if (List("51300", "51301").contains(itemId)) {"WBC"}
      else if (List("50971", "50822").contains(itemId)) {"POTA"}
      else if (List("51006").contains(itemId)) {"UREA"}
      else {itemId}
    }
    val itemConverterUdf = udf(itemConverter _)

    val chart24Item = chart24.withColumn("ITEMID", itemConverterUdf($"ITEMID"))
    val chart48Item = chart48.withColumn("ITEMID", itemConverterUdf($"ITEMID"))
    val chartLab24Item = chartLab24.withColumn("ITEMID", itemConverterUdf($"ITEMID"))
    val chartLab48Item = chartLab48.withColumn("ITEMID", itemConverterUdf($"ITEMID"))

    chart24Item.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv("output/chart24Item17.csv")
    chart48Item.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv("output/chart48Item17.csv")
    chartLab24Item.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv("output/chartLab24Item20.csv")
    chartLab48Item.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv("output/chartLab48Item20.csv")

  }
}
