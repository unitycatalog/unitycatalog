import sbt._
import sbt.Keys._
import java.nio.file.{Files, Path, Paths}
import scala.util.{Try, Success, Failure}

/**
 * Object containing build processing utilities for Java client generation.
 *
 * This object provides methods to process generated Java client files,
 * adding user-agent support to the ApiClient class.
 */
object JavaClientPostBuild {

  /**
   * Processes the generated Java client files after OpenAPI code generation.
   *
   * This method modifies the ApiClient.java file to add user-agent support:
   * - Adds a userAgent field
   * - Adds setUserAgent() and getUserAgent() methods
   * - Initializes the user-agent with a default value
   * - Ensures the User-Agent header is applied to all requests
   *
   * @param log              The logger to output informational messages.
   * @param openApiOutputDir The directory where the OpenAPI generator outputs the files.
   */
  def processGeneratedFiles(
      log: Logger,
      openApiOutputDir: String,
      version: String
  ): Unit = {
    val apiClientPath = Paths.get(openApiOutputDir, "src", "main", "java", "io", "unitycatalog", "client", "ApiClient.java")

    if (!Files.exists(apiClientPath)) {
      sys.error(s"ApiClient.java not found at ${apiClientPath.toAbsolutePath}")
    }

    log.info(s"Processing ApiClient.java to add user-agent support")

    val originalContent = new String(Files.readAllBytes(apiClientPath), "UTF-8")
    val modifiedContent = addUserAgentSupport(originalContent, version, log)

    Files.write(apiClientPath, modifiedContent.getBytes("UTF-8"))
    log.info("Successfully added user-agent support to ApiClient.java")
  }

  /**
   * Modifies the ApiClient.java content to add user-agent support.
   *
   * @param content The original content of ApiClient.java
   * @param version The version string to use in the default user-agent
   * @param log     The logger for informational messages
   * @return The modified content with user-agent support
   */
  private def addUserAgentSupport(content: String, version: String, log: Logger): String = {
    var modified = content

    // Step 1: Add userAgent field after other fields (after connectTimeout declaration)
    val fieldToFind = "  private Duration connectTimeout;"
    val fieldToAdd =
      """  private Duration connectTimeout;
        |  private String userAgent;""".stripMargin

    if (modified.contains(fieldToFind) && !modified.contains("private String userAgent;")) {
      modified = modified.replace(fieldToFind, fieldToAdd)
      log.info("Added userAgent field")
    } else {
      log.warn("Could not add userAgent field - either field anchor not found or field already exists")
    }

    // Step 2: Initialize userAgent in the default constructor
    val constructorInit1 = """    updateBaseUri(getDefaultBaseUri());
    interceptor = null;
    readTimeout = null;
    connectTimeout = null;
    responseInterceptor = null;
    asyncResponseInterceptor = null;
  }"""
    val constructorInit1WithUserAgent = """    updateBaseUri(getDefaultBaseUri());
    setRequestInterceptor(null);
    readTimeout = null;
    connectTimeout = null;
    responseInterceptor = null;
    asyncResponseInterceptor = null;
    this.userAgent = "UnityCatalog-Java-Client/" + io.unitycatalog.cli.utils.VersionUtils.VERSION;
  }"""

    if (modified.contains(constructorInit1) && !modified.contains("this.userAgent = \"UnityCatalog-Java-Client/\"")) {
      modified = modified.replace(constructorInit1, constructorInit1WithUserAgent)
      log.info("Added userAgent initialization in default constructor")
    } else {
      log.warn("Could not modify default constructor - either anchor not found or already modified")
    }

    // Step 3: Initialize userAgent in the parameterized constructor
    val constructorInit2 = """    updateBaseUri(baseUri != null ? baseUri : getDefaultBaseUri());
    interceptor = null;
    readTimeout = null;
    connectTimeout = null;
    responseInterceptor = null;
    asyncResponseInterceptor = null;
  }"""
    val constructorInit2WithUserAgent = """    updateBaseUri(baseUri != null ? baseUri : getDefaultBaseUri());
    setRequestInterceptor(null);
    readTimeout = null;
    connectTimeout = null;
    responseInterceptor = null;
    asyncResponseInterceptor = null;
    this.userAgent = "UnityCatalog-Java-Client/" + io.unitycatalog.cli.utils.VersionUtils.VERSION;
  }"""

    if (modified.contains(constructorInit2)) {
      modified = modified.replace(constructorInit2, constructorInit2WithUserAgent)
      log.info("Added userAgent initialization in parameterized constructor")
    } else {
      log.warn("Could not modify parameterized constructor - anchor not found")
    }

    // Step 4: Add getUserAgent() and setUserAgent() methods before the closing brace
    // Find the last method (getConnectTimeout) and add after it
    val methodAnchor = """  public Duration getConnectTimeout() {
    return connectTimeout;
  }
}"""

    val methodsToAdd = """  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  /**
   * Set a custom user-agent header for HTTP requests.
   *
   * <p>This user-agent will be included in all HTTP requests made by the API client.</p>
   *
   * @param userAgent The user-agent string to use. A value of null will reset to the default.
   * @return This object.
   */
  public ApiClient setUserAgent(String userAgent) {
    this.userAgent = userAgent != null ? userAgent : "UnityCatalog-Java-Client/" + io.unitycatalog.cli.utils.VersionUtils.VERSION;
    return this;
  }

  /**
   * Get the current user-agent header value.
   *
   * @return The user-agent string.
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Set client application information in the user-agent header.
   *
   * <p>This sets client application name and version pairs that are appended to the Unity Catalog client
   * user-agent, following the standard User-Agent format. This method accepts multiple name-version
   * pairs and replaces any previously set client information.</p>
   *
   * <p>Examples:</p>
   * <ul>
   *   <li>{@code setClientVersion("MyApp", "1.0.0")} → "UnityCatalog-Java-Client/0.2.0 MyApp/1.0.0"</li>
   *   <li>{@code setClientVersion("MyApp", "1.0.0", "MyWrapper", "2.5")} → "UnityCatalog-Java-Client/0.2.0 MyApp/1.0.0 MyWrapper/2.5"</li>
   *   <li>{@code setClientVersion("MyApp", null)} → "UnityCatalog-Java-Client/0.2.0 MyApp"</li>
   * </ul>
   *
   * @param nameVersionPairs Variable arguments of name-version pairs. Must contain an even number of arguments
   *                         where each odd argument is a client application name (String) and each even argument
   *                         is the corresponding version (String or null).
   * @return This object.
   * @throws IllegalArgumentException if the number of arguments is odd or if any client name is null/empty.
   */
  public ApiClient setClientVersion(String... nameVersionPairs) {
    if (nameVersionPairs.length % 2 != 0) {
      throw new IllegalArgumentException("Must provide an even number of arguments (name-version pairs)");
    }
    
    String baseUserAgent = "UnityCatalog-Java-Client/" + io.unitycatalog.cli.utils.VersionUtils.VERSION;
    
    // Build application info from all pairs
    StringBuilder appInfoBuilder = new StringBuilder();
    for (int i = 0; i < nameVersionPairs.length; i += 2) {
      String name = nameVersionPairs[i];
      String version = nameVersionPairs[i + 1];
      
      if (name == null || name.trim().isEmpty()) {
        throw new IllegalArgumentException("Client name at index " + i + " cannot be null or empty");
      }
      
      if (appInfoBuilder.length() > 0) {
        appInfoBuilder.append(" ");
      }
      
      appInfoBuilder.append(name.trim());
      if (version != null && !version.trim().isEmpty()) {
        appInfoBuilder.append("/").append(version.trim());
      }
    }
    
    // Set the user agent, replacing any previous client info
    if (appInfoBuilder.length() > 0) {
      this.userAgent = baseUserAgent + " " + appInfoBuilder.toString();
    } else {
      this.userAgent = baseUserAgent;
    }
    
    return this;
  }
}"""

    if (modified.contains(methodAnchor) && !modified.contains("public String getUserAgent()")) {
      modified = modified.replace(methodAnchor, methodsToAdd)
      log.info("Added getUserAgent(), setUserAgent(), and setClientVersion() methods")
    } else {
      log.warn("Could not add user-agent methods - either anchor not found or methods already exist")
    }

    // Step 5: Modify setRequestInterceptor to chain the user-agent header
    val setRequestInterceptorMethod = """  public ApiClient setRequestInterceptor(Consumer<HttpRequest.Builder> interceptor) {
    this.interceptor = interceptor;
    return this;
  }"""

    val setRequestInterceptorMethodWithUserAgent = """  public ApiClient setRequestInterceptor(Consumer<HttpRequest.Builder> interceptor) {
    if (interceptor == null) {
      this.interceptor = builder -> builder.header("User-Agent", this.userAgent);
    } else {
      this.interceptor = builder -> {
        builder.header("User-Agent", this.userAgent);
        interceptor.accept(builder);
      };
    }
    return this;
  }"""

    if (modified.contains(setRequestInterceptorMethod) && !modified.contains("builder.header(\"User-Agent\", this.userAgent)")) {
      modified = modified.replace(setRequestInterceptorMethod, setRequestInterceptorMethodWithUserAgent)
      log.info("Modified setRequestInterceptor to apply user-agent header")
    } else {
      log.warn("Could not modify setRequestInterceptor - either method not found or already modified")
    }

    modified
  }
}

