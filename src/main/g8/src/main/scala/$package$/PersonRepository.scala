package $package$

import java.util.UUID

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest, ReturnValue}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.lambda.runtime.Context
import com.github.dnvriend.lambda._
import com.github.dnvriend.lambda.annotation.{HttpHandler, ScheduleConf}
import play.api.libs.json.{Json, _}

import scala.collection.JavaConverters._

object Person {
  implicit val format: Format[Person] = Json.format[Person]
}
final case class Person(name: String, id: Option[String] = None)

object PersonRepository {
  final val TableName = {
    val projectName = sys.env("PROJECT_NAME")
    val stage = sys.env("STAGE")
  }
  val db: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient()

  def id: String = UUID.randomUUID.toString

  def put(id: String, person: Person): Unit = {
    db.putItem(
      new PutItemRequest()
        .withTableName(TableName)
        .withReturnValues(ReturnValue.NONE)
        .withItem(
          Map(
            "id" -> new AttributeValue(id),
            "json" -> new AttributeValue(Json.toJson(person.copy(id = Option(id))).toString)
          ).asJava
        )
    )
  }

  def get(id: String): Person = {
    val json = db.getItem(TableName, Map("id" -> new AttributeValue(id)).asJava)
      .getItem.get("json").getS
    Json.parse(json).as[Person]
  }

  def list: List[Person] = {
    db.scan(TableName, List("json").asJava)
      .getItems.asScala.flatMap(_.values().asScala).map(_.getS).toList
      .map(Json.parse)
      .map(_.as[Person])
  }
}

@ScheduleConf(schedule = "rate(1 minute)")
class PutPersonScheduled extends ScheduledEventHandler {
  override def handle(event: ScheduledEvent, ctx: Context): Unit = {
    val id: String = PersonRepository.id
    PersonRepository.put(id, Person("schedule", Option(id)))
  }
}

@HttpHandler(path = "/person", method = "post")
class PostPerson extends ApiGatewayHandler {
  override def handle(request: HttpRequest, ctx: Context): HttpResponse = {
    val id: String = PersonRepository.id
    val person = request.bodyOpt[Person].get
    PersonRepository.put(id, person)
    HttpResponse.ok.withBody(Json.toJson(person.copy(id = Option(id))))
  }
}

@HttpHandler(path = "/person", method = "get")
class GetListOfPerson extends ApiGatewayHandler {
  override def handle(request: HttpRequest, ctx: Context): HttpResponse = {
    HttpResponse.ok.withBody(Json.toJson(PersonRepository.list))
  }
}

@HttpHandler(path = "/person/{id}", method = "get")
class GetPerson extends ApiGatewayHandler {
  override def handle(request: HttpRequest, ctx: Context): HttpResponse = {
    request.pathParamsOpt[Map[String, String]].getOrElse(Map.empty).get("id")
      .fold(HttpResponse.notFound.withBody(Json.toJson("Person not found")))(id => {
        HttpResponse.ok.withBody(Json.toJson(PersonRepository.get(id)))
      })
  }
}
