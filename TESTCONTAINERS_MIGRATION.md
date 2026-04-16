# Migration to Testcontainers

## Overview

The integration tests have been migrated from the custom `MysqlOnetimeServer` implementation to use [Testcontainers](https://www.testcontainers.org/), a Java library that provides lightweight, throwaway instances of databases running in Docker containers.

## Benefits

- **Better Portability**: Tests run consistently across different environments without requiring custom server setup scripts
- **Multi-Database Support**: Easy support for both MySQL and MariaDB with the same infrastructure
- **Industry Standard**: Testcontainers is widely adopted and well-maintained
- **Simplified Setup**: No need for custom onetimeserver scripts or binaries
- **Docker Integration**: Leverages Docker for container management

## Changes

### New Classes

1. **`TestDatabaseContainer`**: Main container management class that replaces `MysqlOnetimeServer`
   - Supports both MySQL and MariaDB via the `MYSQL_VERSION` environment variable
   - Provides the same API as the old implementation for minimal migration impact
   - Automatically handles container lifecycle (start/stop)

2. **`TestDatabaseContainerOptions`**: Configuration options class that replaces `MysqlOnetimeServerOptions`
   - Same configuration options as before (GTID, server ID, extra parameters, etc.)
   - Added support for specifying database version directly

### Updated Classes

- **`AbstractIntegrationTest`**: Updated to use `TestDatabaseContainer` instead of `MysqlOnetimeServer`
- All integration test classes that override `getOptions()` now return `TestDatabaseContainerOptions`

### Deprecated Classes

- **`MysqlOnetimeServer`**: Marked as `@Deprecated`, will be removed in a future release
- **`MysqlOnetimeServerOptions`**: Marked as `@Deprecated`, will be removed in a future release

## Usage

### Environment Variables

The `MYSQL_VERSION` environment variable controls which database version to use:

- `5.7` - MySQL 5.7 (default if not specified)
- `8.0` - MySQL 8.0
- `mariadb` - MariaDB 10.6 (default MariaDB version)
- `mariadb-10.6` - Specific MariaDB version

### Running Tests

```bash
# Run with default MySQL 5.7
mvn clean verify

# Run with MySQL 8.0
MYSQL_VERSION=8.0 mvn clean verify

# Run with MariaDB
MYSQL_VERSION=mariadb mvn clean verify

# Run with specific MariaDB version
MYSQL_VERSION=mariadb-10.11 mvn clean verify
```

### Prerequisites

- Docker must be installed and running
- Docker daemon must be accessible to the user running the tests
- Sufficient disk space for pulling MySQL/MariaDB Docker images

### Example Test Configuration

```java
@Override
protected TestDatabaseContainerOptions getOptions() {
    TestDatabaseContainerOptions options = new TestDatabaseContainerOptions();
    options.gtid = true;
    options.fullRowMetaData = true;
    options.extraParams = "--binlog-transaction-compression=ON";
    return options;
}
```

## Migration Guide for Custom Tests

If you have custom tests using `MysqlOnetimeServer`, follow these steps:

1. Replace `MysqlOnetimeServer` with `TestDatabaseContainer`
2. Replace `MysqlOnetimeServerOptions` with `TestDatabaseContainerOptions`
3. Update method calls:
   - `server.boot()` → `server.start()`
   - `server.shutDown()` → `server.stop()`
   - `server.getPort()` remains the same
   - Add `server.getHost()` instead of hardcoded "127.0.0.1"

### Before

```java
MysqlOnetimeServer server = new MysqlOnetimeServer();
server.boot();
MySQLConnection conn = new MySQLConnection("127.0.0.1", server.getPort(), "root", "");
// ... test code ...
server.shutDown();
```

### After

```java
TestDatabaseContainer server = new TestDatabaseContainer();
server.start();
MySQLConnection conn = new MySQLConnection(server.getHost(), server.getPort(), "root", "");
// ... test code ...
server.stop();
```

## Troubleshooting

### Docker Not Available

If Docker is not available, tests will fail with a connection error. Ensure Docker is:
- Installed
- Running
- Accessible to the current user

### Container Startup Timeout

If containers take too long to start, you may need to:
- Increase available memory for Docker
- Pull the required images beforehand: `docker pull mysql:8.0` or `docker pull mariadb:10.6`
- Check Docker logs for issues

### Port Conflicts

Testcontainers automatically assigns random available ports, so port conflicts should not occur. The actual port can be retrieved via `container.getPort()`.

## Dependencies

The following dependencies were added to support Testcontainers:

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>${version.testcontainers}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers-mysql</artifactId>
    <version>${version.testcontainers}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers-mariadb</artifactId>
    <version>${version.testcontainers}</version>
    <scope>test</scope>
</dependency>
```

**Note:** Testcontainers 2.0+ uses new module names (`testcontainers-mysql` and `testcontainers-mariadb` instead of `mysql` and `mariadb`). The version is managed via the `version.testcontainers` property in `pom.xml` (currently 2.0.4).

### Additional Dependencies

Jackson libraries were upgraded to support Testcontainers 2.0.4:

```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-core</artifactId>
    <version>${version.jackson}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>${version.jackson}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-annotations</artifactId>
    <version>${version.jackson}</version>
    <scope>test</scope>
</dependency>
```

## Future Plans

- Remove deprecated `MysqlOnetimeServer` and `MysqlOnetimeServerOptions` classes
- Remove the `src/test/onetimeserver` script
- Add support for additional database versions as needed
