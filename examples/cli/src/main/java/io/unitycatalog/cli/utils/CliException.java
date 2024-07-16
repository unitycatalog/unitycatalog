package io.unitycatalog.cli.utils;

public class CliException extends RuntimeException {
  public CliException(String message) {
    super(message);
  }

  public CliException(String message, Throwable cause) {
    super(message, cause);
  }
}
