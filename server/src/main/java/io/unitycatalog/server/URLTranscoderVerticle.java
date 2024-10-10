package io.unitycatalog.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** URL transcoder. */
class URLTranscoderVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(URLTranscoderVerticle.class);

  private final int transcodePort;
  private final int servicePort;

  URLTranscoderVerticle(int transcodePort, int servicePort) {
    this.transcodePort = transcodePort;
    this.servicePort = servicePort;
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
                        client.request(method, servicePort, host, path);
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
                    return transcodeResp.end(resp.bodyAsBuffer());
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
