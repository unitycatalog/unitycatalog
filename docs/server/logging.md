# Logging

Unity Catalog uses [SLF4J](https://www.slf4j.org/) as its logging facade with
[Apache Log4j2](https://logging.apache.org/log4j/2.x/) as the logging implementation.

## Default Behavior

Out of the box, the server logs to both the **console** (stdout) and a **rolling file** at
`logs/server.log`. The default log level is `info`.

No additional configuration is needed to get started.

## Changing the Log Level

Set the `uc.log.level` system property to change the log level at startup:

```sh
bin/start-uc-server -Duc.log.level=debug
```

Or, when running the server directly with Java:

```sh
java -Duc.log.level=debug -jar server.jar
```

Supported levels (from most to least verbose): `trace`, `debug`, `info`, `warn`, `error`.

## Changing the Log Directory

By default, log files are written to `logs/` relative to the working directory. Override
this with the `uc.log.dir` system property:

```sh
bin/start-uc-server -Duc.log.dir=/var/log/unitycatalog
```

## Using a Custom Configuration File

For full control over logging (appenders, filters, per-package levels), provide your own
Log4j2 configuration file using the standard `log4j.configurationFile` system property:

```sh
bin/start-uc-server -Dlog4j.configurationFile=/path/to/my-log4j2.properties
```

When a custom configuration file is provided, it takes full precedence over the built-in
defaults. The `uc.log.level` and `uc.log.dir` system properties only work if the custom
configuration uses them (as the built-in config does).

## Default Per-Package Log Levels

The built-in configuration quiets noisy third-party libraries while keeping Unity Catalog
output at the configured level:

| Package | Default Level |
|---------|---------------|
| `io.unitycatalog` | `info` (follows `uc.log.level`) |
| `org.hibernate` | `warn` |
| `org.h2` | `warn` |
| `com.linecorp.armeria` | `warn` |
| Everything else (root) | `info` (follows `uc.log.level`) |

These can be overridden by providing a custom configuration file as described above.

## Log Rotation

The built-in rolling file appender rotates log files when they reach **10 MB** and keeps up
to **5** archived files. Archived files are compressed as `.gz`.

## Logging in Tests

Tests use a separate configuration (`log4j2-test.properties`) that logs only to the console
at `warn` level, with Unity Catalog code at `info`. This keeps test output clean while still
surfacing important messages.

To increase test log verbosity, pass the system property when running tests:

```sh
sbt -Duc.log.level=debug "server/test"
```

## CLI Logging

The Unity Catalog CLI uses the same Log4j2 auto-discovery mechanism. It logs to the console
only (no file output) at `info` level by default. The same `uc.log.level` property works:

```sh
bin/uc -Duc.log.level=debug <command>
```
