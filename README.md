# Spark in Scala using SBT (Scala Build Tool)
## A Beginner's Guide to Spark in Scala

Spark on Scala with Examples
- RDDs
- Dataframe
- Dataframe with UDFs
- Dataframe with UDAFs
- Datasets
- Read from Different file formats
- Write Datafram to multiple formats


## Scala Marshall/Unmarshall JSON - with Spray JSON

```scala
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.6"
```

```scala
package com.test.serialization

import models.{Address, Patient}
import spray.json._

object Hospital {

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val address = jsonFormat(Address, "country", "state", "zip")
    implicit val patient = jsonFormat(Patient, "name", "regNumber", "address")
  }

  import MyJsonProtocol._

  def main(args: Array[String]): Unit = {
    val p1 = Patient(name = "Amar", regNumber = 234, address = Address("IN", "KA", 49))
    println(p1.toJson.sortedPrint)
  }
}
```

### Case Class
Patient

```scala
package com.test.serialization.models

case class Patient(name: String = "XXXXX", regNumber: Int, address:Address) {

  def lengthOfName= {
      name.length
  }

  def getFullName = {
    "Mr./Mrs. "+this.name
  }

}
```
Address

```scala
package com.test.serialization.models

case class Address(country:String,state:String,zip:Int){
}
```

Output
```json
{
  "address": {
    "country": "IN",
    "state": "KA",
    "zip": 49
  },
  "name": "Amar",
  "regNumber": 234
}
```
