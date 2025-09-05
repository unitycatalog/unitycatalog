package io.unitycatalog.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.unitycatalog.client.ApiClient

import java.net.URI
import java.net.http.HttpRequest
import org.apache.spark.internal.Logging

import java.net.URLEncoder.encode
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class OAuth2Exception(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}

class OAuth2CredentialExchangeProvider(
    oauth2ServerUri: String,
    credential: String,
    setToken: String => Unit)
    extends Logging {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val oauth2Url = new URI(oauth2ServerUri)
  private val oauth2Client = new ApiClient()
    .setHost(oauth2Url.getHost)
    .setPort(oauth2Url.getPort)
    .setScheme(oauth2Url.getScheme)
    .setBasePath(oauth2Url.getPath)

  private val (clientId, clientSecret) = getClientIdAndSecret(credential)

  {
    // Initial token fetch on start up. If it fails we will throw and not
    // attempt to schedule a refresh.
    val tokenExpiresAtMillis = exchangeCredentialsForAccessToken()
    scheduleRefresh(tokenExpiresAtMillis)
  }

  private def mapToUrlEncoded(params: Map[String, String]): String = {
    params
      .map {
        case (key, value) =>
          s"${encode(key, UTF_8.toString)}=${encode(value, UTF_8.toString)}"
      }
      .mkString("&")
  }

  private def getClientIdAndSecret(credential: String): (String, String) = {
    val parts = credential.split(":")
    if (parts.length != 2 || parts(0).isEmpty || parts(1).isEmpty) {
      throw new IllegalArgumentException(
        "Credential must be in the format of <client_id>:<client_secret>."
      )
    }
    (parts(0), parts(1))
  }

  private def exchangeCredentialsForAccessToken(): Long = {
    val requestParameters =
      Map(
        "grant_type" -> "client_credentials",
        "scope" -> "all-apis",
        "client_id" -> clientId,
        "client_secret" -> clientSecret
      )

    val exchangeRequest = HttpRequest
      .newBuilder()
      .uri(URI.create(oauth2ServerUri))
      .header("Content-Type", "application/x-www-form-urlencoded")
      .POST(HttpRequest.BodyPublishers.ofString(mapToUrlEncoded(requestParameters)))
      .build()

    oauth2Client.getHttpClient
      .send(exchangeRequest, java.net.http.HttpResponse.BodyHandlers.ofString()) match {
      case response if response.statusCode() == 200 =>
        val body = response.body()

        logDebug(s"OAuth2 response body: $body")
        val bodyJson: Map[String, Any] = mapper.readValue(body, classOf[Map[String, Any]])

        val token = bodyJson.get("access_token") match {
          case Some(t: String) => t
          case _ => throw new OAuth2Exception("Failed to parse access_token from OAuth2 response.")
        }

        val expiresInSeconds = bodyJson.get("expires_in") match {
          case Some(e: Int) => e
          case Some(e: String) => e.toInt
          case _ => throw new OAuth2Exception("Failed to parse expires_in from OAuth2 response.")
        }

        // We got a new token and expiration, update the caller.
        setToken(token)

        // Return when the token expires
        System.currentTimeMillis + expiresInSeconds * 1000L

      case response =>
        throw new OAuth2Exception(
          s"Failed to obtain access token, status code: ${response.statusCode()}, body: ${response.body()}"
        )
    }
  }

  private def MAX_REFRESH_WINDOW_MILLIS = 300000L // 5 minutes
  private def MIN_REFRESH_WAIT_MILLIS = 1000L // 1 second

  private def scheduleRefresh(expiresAtMillis: Long): Unit = {

    val expiresInMillis = expiresAtMillis - System.currentTimeMillis
    // how much ahead of time to start the request to allow it to complete
    val refreshWindowMillis = math.min(expiresInMillis / 10, MAX_REFRESH_WINDOW_MILLIS)
    // how much time to wait before expiration
    val waitIntervalMillis = expiresInMillis - refreshWindowMillis
    // how much time to actually wait
    val timeToWait = math.max(waitIntervalMillis, MIN_REFRESH_WAIT_MILLIS)

    logInfo(s"Scheduling OAuth2 token refresh in ${timeToWait} ms")
    Future {
      Thread.sleep(timeToWait)
      try {
        val tokenExpiresAtMillis = exchangeCredentialsForAccessToken()
        scheduleRefresh(tokenExpiresAtMillis)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to refresh OAuth2 token: ${e.getMessage}, will attempt retry.")
          scheduleRefresh(expiresAtMillis)
      }
    }
  }
}
