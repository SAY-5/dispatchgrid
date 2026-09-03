package io.dispatchgrid.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;

/** Thin JSON client on java.net.http with a virtual-thread executor. */
final class Http {
  static final ObjectMapper JSON = new ObjectMapper();
  private final HttpClient client;

  Http() {
    this.client =
        HttpClient.newBuilder()
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .connectTimeout(Duration.ofSeconds(5))
            .build();
  }

  /** Returns the status code; throws on transport failure. */
  int postJson(String url, Object body) throws IOException, InterruptedException {
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
            .build();
    return client.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
  }

  JsonNode postJsonForBody(String url, Object body) throws IOException, InterruptedException {
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
            .build();
    HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() / 100 != 2) {
      throw new IOException("HTTP " + res.statusCode() + " from " + url);
    }
    return JSON.readTree(res.body());
  }

  JsonNode getJson(String url) throws IOException, InterruptedException {
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
    HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() / 100 != 2) {
      throw new IOException("HTTP " + res.statusCode() + " from " + url);
    }
    return JSON.readTree(res.body());
  }
}
