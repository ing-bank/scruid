package ing.wbaa.druid.auth.basic

import akka.http.scaladsl.model.headers.{ Authorization, BasicHttpCredentials }
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import ing.wbaa.druid.client.{
  NoopRequestFlowExtension,
  RequestFlowExtension,
  RequestFlowExtensionBuilder
}
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Adds a basic authentication header with static credentials to every outgoing request. Does not
  * modify the response.
  *
  * @param username the username
  * @param password the password
  */
class BasicAuthenticationExtension(username: String, password: String)
    extends NoopRequestFlowExtension {
  override def alterRequest(request: HttpRequest): HttpRequest =
    request.withHeaders(Authorization(BasicHttpCredentials(username, password)))

  override def exportConfig: Config =
    ConfigFactory
      .empty()
      .withValue("username", ConfigValueFactory.fromAnyRef(username))
      .withValue("password", ConfigValueFactory.fromAnyRef(password))
}

object BasicAuthenticationExtension extends RequestFlowExtensionBuilder {
  val logger = LoggerFactory.getLogger(classOf[BasicAuthenticationExtension])

  override def apply(config: Config): RequestFlowExtension = {

    val username =
      Option(config.getString("username")).getOrElse {
        throw new IllegalStateException(
          "BasicAuthenticationExtension requires 'username' configuration parameter to be specified"
        )
      }

    val password =
      Option(config.getString("password")).getOrElse {
        throw new IllegalStateException(
          "BasicAuthenticationExtension requires 'password' configuration parameter to be specified"
        )
      }

    logger.info(s"BasicAuthenticationExtension[username=$username] created")
    new BasicAuthenticationExtension(username, password)
  }
}
