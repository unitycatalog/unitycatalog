package io.unitycatalog.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL transcoder. Also the one public listener a host such as embedded OpenSharing shares a port
 * with: a request whose path starts with one of {@code sidecarPathPrefixes} is forwarded to {@code
 * sidecarPort} instead of {@code servicePort}, so there is one address for a client to reach either
 * half of the process at, chosen by URL rather than by port.
 */
class URLTranscoderVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(URLTranscoderVerticle.class);

  private final int transcodePort;
  private final int servicePort;
  private final Integer sidecarPort;
  private final List<String> sidecarPathPrefixes;

  URLTranscoderVerticle(int transcodePort, int servicePort) {
    this(transcodePort, servicePort, null, List.of());
  }

  /**
   * @param sidecarPort where a request matching {@code sidecarPathPrefixes} is forwarded instead of
   *     {@code servicePort}; null routes every request to {@code servicePort}, as before.
   */
  URLTranscoderVerticle(
      int transcodePort, int servicePort, Integer sidecarPort, List<String> sidecarPathPrefixes) {
    this.transcodePort = transcodePort;
    this.servicePort = servicePort;
    this.sidecarPort = sidecarPort;
    this.sidecarPathPrefixes = sidecarPathPrefixes;
  }

  /** Which backend a request's path routes to: the sidecar if one is configured and matches. */
  int targetPort(String path) {
    if (sidecarPort != null) {
      for (String prefix : sidecarPathPrefixes) {
        if (!prefix.isBlank() && path.startsWith(prefix)) {
          return sidecarPort;
        }
      }
    }
    return servicePort;
  }

  @Override
  public void start() {
    HttpServer server = vertx.createHttpServer();
    WebClient client = WebClient.create(vertx);

    server.requestHandler(
        transcodeRequest -> {
          transcodeRequest
              .body()
              .compose(
                  buffer -> {
                    HttpMethod method = transcodeRequest.method();
                    String host = "127.0.0.1";
                    String path = transcodeRequest.path().replace("%1F", ".");
                    HttpRequest<Buffer> serviceRequest =
                        client.request(method, targetPort(path), host, path);
                    serviceRequest.putHeaders(transcodeRequest.headers());
                    for (Map.Entry<String, String> entry : transcodeRequest.params()) {
                      serviceRequest.addQueryParam(
                          entry.getKey(), entry.getValue().replace('\u001f', '.'));
                    }
                    return serviceRequest.sendBuffer(buffer);
                  })
              .compose(
                  resp -> {
                    HttpServerResponse transcodeResp = transcodeRequest.response();
                    transcodeResp.setStatusCode(resp.statusCode());
                    for (Map.Entry<String, String> entry : resp.headers()) {
                      transcodeResp.putHeader(entry.getKey(), entry.getValue());
                    }
                    // A response with no body gives a null buffer, and end(null) throws before
                    // anything is written, leaving the client waiting on a response it never gets.
                    Buffer body = resp.bodyAsBuffer();
                    return body == null ? transcodeResp.end() : transcodeResp.end(body);
                  });
        });

    server.listen(
        transcodePort,
        ar -> {
          if (ar.succeeded()) {
            LOGGER.info("URL transcoder started on port {}", transcodePort);
          } else {
            LOGGER.info("Failed to start URL transcoder: {}", String.valueOf(ar.cause()));
          }
        });
  }
}
