import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders


// Define your case classes
case class MyUserProfileMessage(UserId: Int, Email: String, FirstName: String, LastName: String, LanguageId: Option[Int])
case class MyLanguageMessage(LanguageId: Int, LanguageLocaleId: String)
case class MyDeviceMessage(DeviceId1: String, Created: Option[Timestamp], UpdatedDate: Timestamp, DeviceId2: String, DeviceName: String, LocationId: Option[Int], DeviceTypeId: Option[Int], DeviceClassId: Int, UserId1: Option[Int])
case class MyDeviceClassMessage(DeviceClassId: Int, DeviceClassName: String)
case class MyDeviceTypeMessage(DeviceTypeId: Int, DeviceTypeName: String)
case class MyLocation1(LocationId1: Int, LocationId: Int, Latitude: Option[Double], Longitude: Option[Double], Radius: Option[Double], CreatedDate: Timestamp)
case class MyTimeZoneLookupMessage(TimeZoneId: Int, ZoneName: String)
case class MyUserLocationMessage(UserId: Int, LocationId: Int, LocationName: String, Status: Int, CreatedDate: Timestamp)
case class MyUserMessage(UserId: Int, Created: Option[Timestamp], Deleted: Option[Timestamp], Active: Option[Boolean], ActivatedDate: Option[Timestamp])
case class MyLocationMessage(LocationId: Int, IsDeleted: Option[Boolean], Address1: String, Address2: String, City: String, State: String, Country: String, ZipCode: String, Feature2Enabled: Option[Boolean], LocationStatus: Option[Int], Location1Enabled: Option[Boolean], LocationKey: String, UpdatedDateTime: Timestamp, CreatedDate: Timestamp, Feature1Enabled: Option[Boolean], Level: Option[Int], TimeZone: Option[Int])

// Spark session setup
object Main extends App {

  val spark = SparkSession.builder()
    .appName("CSV Join Example")
    .master("local[*]")
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:src/main/resources/log4j2.properties")
    .getOrCreate()

  println("Spark Version "+spark.version)

  import spark.implicits._

  // Load each dataset
  val userProfile = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyUserProfileMessage].schema).csv("src/main/resources/userProfile.csv").as[MyUserProfileMessage]
  val language = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyLanguageMessage].schema).csv("src/main/resources/language.csv").as[MyLanguageMessage]
  val device = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyDeviceMessage].schema).csv("src/main/resources/device.csv").as[MyDeviceMessage]
  val deviceClass = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyDeviceClassMessage].schema).csv("src/main/resources/deviceClass.csv").as[MyDeviceClassMessage]
  val deviceType = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyDeviceTypeMessage].schema).csv("src/main/resources/deviceType.csv").as[MyDeviceTypeMessage]
  val location1 = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyLocation1].schema).csv("src/main/resources/location1.csv").as[MyLocation1]
  val timeZoneLookup = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyTimeZoneLookupMessage].schema).csv("src/main/resources/timeZoneLookup.csv").as[MyTimeZoneLookupMessage]
  val userLocation = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyUserLocationMessage].schema).csv("src/main/resources/userLocation.csv").as[MyUserLocationMessage]
  val user = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyUserMessage].schema).csv("src/main/resources/user.csv").as[MyUserMessage]
  val location = spark.read.option("header", "true").option("comment", "#").option("nullValue", "null").schema(Encoders.product[MyLocationMessage].schema).csv("src/main/resources/location.csv").as[MyLocationMessage]

  // Perform the joins and filters
  val result = user
    .join(userProfile, user("UserId") === userProfile("UserId"), "inner")
    .join(language, userProfile("LanguageId") === language("LanguageId"), "left")
    .join(userLocation, user("UserId") === userLocation("UserId"), "inner")
    .join(location, userLocation("LocationId") === location("LocationId"), "inner")
    .join(device, location("LocationId") === device("LocationId"), "inner")
    .join(deviceType, device("DeviceTypeId") === deviceType("DeviceTypeId"), "inner")
    .join(deviceClass, device("DeviceClassId") === deviceClass("DeviceClassId"), "inner")
    .join(timeZoneLookup, timeZoneLookup("TimeZoneId") === location("TimeZone"), "left")
    .join(location1, location("LocationId") === location1("LocationId"), "left")
    .where(device("UserId1").isNull && (user("Active") === lit(true) || user("ActivatedDate").isNotNull))
    .dropDuplicates()

  // Display the results
  println("df count = " + result.count())
  println("rdd count = "+ result.rdd.count())
  result.show(false)
  println("------")
  result.rdd.foreach(println)

  spark.stop()
}
