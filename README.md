# Hadoop OSS Connector

Hadoop OSS Connector V2 is a Hadoop-compatible file system implementation that enables Hadoop applications to read and write data to and from Alibaba Cloud Object Storage Service (OSS). This connector provides seamless integration between Hadoop ecosystem and OSS with optimized performance.

## Features

- Full Hadoop FileSystem interface implementation for OSS
- High-performance data access with optimized I/O operations
- Support for multipart upload and download
- Native integration with Alibaba Cloud OSS service
- Compatible with standard Hadoop operations (list, create, read, write, delete, etc.)
- Enhanced performance optimizations for big data workloads
- Dual-domain support (OSS + Accelerator)

## Architecture

The connector consists of several key components:

1. **AliyunOSSPerformanceFileSystem**: Main file system implementation that extends Hadoop's FileSystem class
2. **AliyunOSSFileSystemStore**: Core storage layer that interacts with OSS APIs
3. **OssManager**: OSS client management and operation tracking
4. **Constants**: Configuration constants for OSS filesystem

## Prerequisites

- Java 8 or higher
- Maven 3.x
- Access to Alibaba Cloud OSS service (AccessKey ID and AccessKey Secret required)

## Installation

### From Source

```bash
git clone <repository-url>
cd hadoop-oss-connector
mvn clean install
```

### Maven Dependency

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aliyun</artifactId>
    <version>${version}</version>
</dependency>
```

## Configuration

To use the OSS connector, you need to configure the following properties in your Hadoop configuration:

### Core Properties

| Property | Description | Default Value |
|---------|-------------|---------------|
| `fs.oss.accessKeyId` | Your Alibaba Cloud AccessKey ID | None |
| `fs.oss.accessKeySecret` | Your Alibaba Cloud AccessKey Secret | None |
| `fs.oss.endpoint` | OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com) | None |
| `fs.oss.acc.endpoint` | OSS Accelerator endpoint (internal network only) | None |
| `fs.oss.connection.maximum` | Number of simultaneous connections to OSS | 32 |
| `fs.oss.connection.secure.enabled` | Connect to OSS over SSL | true |
| `fs.oss.attempts.maximum` | Number of times to retry errors | 10 |
| `fs.oss.connection.timeout` | Connection timeout in milliseconds | 200000 |
| `fs.oss.paging.maximum` | Number of records to get while paging through a directory listing | 1000 |

### Performance Tuning Properties

| Property | Description | Default Value |
|---------|-------------|---------------|
| `fs.oss.multipart.upload.size` | Size of each multipart upload piece | 104857600 (100 MB) |
| `fs.oss.multipart.upload.threshold` | Minimum size before starting a multipart upload | 20971520 (20 MB) |
| `fs.oss.multipart.download.size` | Size of each multipart download piece | 524288 (512 KB) |
| `fs.oss.multipart.download.threads` | Number of threads for multipart download | 10 |
| `fs.oss.max.total.tasks` | Maximum total tasks | 128 |
| `fs.oss.fast.upload.buffer` | Buffer type for fast upload (disk, array, bytebuffer) | disk |

### Prefetch Properties

The connector supports advanced prefetching capabilities with the following configuration options:

| Property | Description | Default Value |
|---------|-------------|---------------|
| `fs.oss.prefetch.version` | Prefetch version (v1 or v2) | v2 |
| `fs.oss.prefetch.block.size` | Size of each prefetch block | 8388608 (8 MB) |
| `fs.oss.prefetch.block.count` | Number of blocks to prefetch per stream | 8 |
| `fs.oss.prefetch.max.blocks.count` | Maximum blocks to cache per stream | 16 |
| `fs.oss.input.async.drain.threshold` | Async drain threshold | 16000 |
| `fs.oss.threads.max` | Maximum threads for download and prefetch | 16 |

### Accelerator Domain Properties

The connector supports dual-domain configuration with OSS and Accelerator endpoints:

| Property | Description | Default Value |
|---------|-------------|---------------|
| `fs.oss.acc.endpoint` | Accelerator endpoint URL | None |
| `fs.oss.acc.rules` | Acceleration rules configuration | None |

### Configuration Examples

#### In configuration XML:

```xml
<configuration>
    <property>
        <name>fs.oss.accessKeyId</name>
        <value>YOUR_ACCESS_KEY_ID</value>
    </property>
    <property>
        <name>fs.oss.accessKeySecret</name>
        <value>YOUR_ACCESS_KEY_SECRET</value>
    </property>
    <property>
        <name>fs.oss.endpoint</name>
        <value>oss-cn-hangzhou.aliyuncs.com</value>
    </property>
</configuration>
```

#### Programmatically:

```java
Configuration conf = new Configuration();
conf.set("fs.oss.accessKeyId", "YOUR_ACCESS_KEY_ID");
conf.set("fs.oss.accessKeySecret", "YOUR_ACCESS_KEY_SECRET");
conf.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
```

## Usage

### URI Format

The URI format for OSS is: `oss://bucket/path/to/file`

Example:
```java
FileSystem fs = FileSystem.get(URI.create("oss://my-bucket/"), conf);
Path path = new Path("oss://my-bucket/my-file.txt");
FSDataInputStream in = fs.open(path);
```

### Basic Operations

```java
// Initialize file system
Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(URI.create("oss://my-bucket/"), conf);

// Create a file
FSDataOutputStream out = fs.create(new Path("oss://my-bucket/test.txt"));
out.write("Hello OSS!".getBytes());
out.close();

// Read a file
FSDataInputStream in = fs.open(new Path("oss://my-bucket/test.txt"));
byte[] buffer = new byte[1024];
int bytesRead = in.read(buffer);
in.close();

// List files
FileStatus[] files = fs.listStatus(new Path("oss://my-bucket/"));
for (FileStatus file : files) {
    System.out.println(file.getPath());
}

// Delete a file
fs.delete(new Path("oss://my-bucket/test.txt"), false);

// Check if file exists
boolean exists = fs.exists(new Path("oss://my-bucket/some-file.txt"));
```

### Working with Directories

```java
// Create directory
fs.mkdirs(new Path("oss://my-bucket/my-directory/"));

// List directory contents
FileStatus[] files = fs.listStatus(new Path("oss://my-bucket/my-directory/"));

// Delete directory recursively
fs.delete(new Path("oss://my-bucket/my-directory/"), true);
```

## Advanced Features

### Multipart Upload

The connector automatically uses multipart upload for large files based on the configured threshold. You can tune this behavior with:

```properties
fs.oss.multipart.upload.threshold=104857600  # 100MB
fs.oss.multipart.upload.part.size=16777216   # 16MB
```

### Retry Mechanism

Failed operations are automatically retried based on configuration:

```properties
fs.oss.attempts.maximum=5
fs.oss.connection.timeout=60000  # 60 seconds
```

### Buffer Management

For multipart operations, local buffer directories are used:

```properties
fs.oss.buffer.dir=/tmp/hadoop-oss,/data/tmp
```

### Prefetch

The connector supports advanced prefetching capabilities  with the following features:

1. For large files, use local disk cache
2. For small files, use memory cache (not yet implemented)
3. Split files into blocks and download them concurrently, with configurable maximum concurrency and block size
4. Optimized for random I/O patterns 
5. Immediately prefetch one block after seek
6. When an HTTP stream is about to complete, the data is read completely instead of aborting to reuse the HTTP connection
7. Single stream prefetches up to 8 blocks, with a global FS default of 96 blocks
8. If reading a cached block takes more than 5s (non-configurable), prefetching is stopped to avoid performance degradation

To enable Prefetch:

```xml
<property>
    <name>fs.oss.prefetch.version</name>
    <value>v2</value>
</property>
```

### Dual-Domain Support (OSS + Accelerator)

The connector supports dual-domain configuration with OSS and Accelerator endpoints for optimized performance:

1. Accelerator domain configuration (internal network only):
```xml
<property>
    <name>fs.oss.acc.endpoint</name>
    <value>https://oss-cn-hangzhou-acc.aliyuncs.com</value>
</property>
```

2. Acceleration rules configuration:
```xml
<property>
    <name>fs.oss.acc.rules</name>
    <value>
        &lt;rules&gt;
            &lt;rule&gt;
                &lt;keyPrefixes&gt;
                    &lt;keyPrefix&gt;root-path/test&lt;/keyPrefix&gt;
                    &lt;keyPrefix&gt;b/&lt;/keyPrefix&gt;
                &lt;/keyPrefixes&gt;
                &lt;keySuffixes&gt;
                    &lt;keySuffix&gt;.txt&lt;/keySuffix&gt;
                    &lt;keySuffix&gt;.png&lt;/keySuffix&gt;
                &lt;/keySuffixes&gt;
                &lt;sizeRanges&gt;
                    &lt;range&gt;
                        &lt;minSize&gt;0&lt;/minSize&gt;
                        &lt;maxSize&gt;1048576&lt;/maxSize&gt;
                    &lt;/range&gt;
                &lt;/sizeRanges&gt;
                &lt;operations&gt;
                    &lt;operation&gt;getObject&lt;/operation&gt;
                &lt;/operations&gt;
            &lt;/rule&gt;
        &lt;/rules&gt;
    </value>
</property>
```

Rules explanation:
1. Prefix matching (keyPrefixes):
    - Multiple prefixes allowed
    - OR relationship between multiple prefixes
2. Suffix matching (keySuffixes):
    - Multiple suffixes allowed
    - OR relationship between multiple suffixes
3. File size matching (sizeRanges):
    - Multiple ranges can be set, each representing a file size range
4. IO size matching:
    - Head access: requests at position [start, start+x]
    - Tail access: requests at position [end-y, end]
5. Operation matching:
    - Currently only supports getObject

## Development

### Project Structure

```
hadoop-oss/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── org/apache/hadoop/fs/aliyun/oss/v2/
│   │   │       ├── AliyunOSSFileSystemStore.java
│   │   │       ├── AliyunOSSPerformanceFileSystem.java
│   │   │       ├── Constants.java
│   │   │       ├── OssManager.java
│   │   │       └── model/
│   │   └── resources/
│   └── test/
│       ├── java/
│       └── resources/
├── pom.xml
└── README.md
```

### Building

To build the project:

```bash
mvn clean package
```

### Testing

To run tests:

```bash
mvn test
```

Note: Tests require valid OSS credentials configured in `src/test/resources/auth-keys.xml`.

Example auth-keys.xml:
```xml
<configuration>
  <property>
    <name>fs.oss.accessKeyId</name>
    <value>YOUR_ACCESS_KEY_ID</value>
  </property>
  <property>
    <name>fs.oss.accessKeySecret</name>
    <value>YOUR_ACCESS_KEY_SECRET</value>
  </property>
  <property>
    <name>test.fs.oss.name</name>
    <value>oss://your-test-bucket/</value>
  </property>
</configuration>
```

## Performance Considerations

1. **Block Size**: Adjust `fs.oss.block.size` based on your workload. Larger blocks are better for sequential access.

2. **Connection Management**: Tune `fs.oss.connection.maximum` based on your cluster size and concurrency requirements.

3. **Multipart Upload**: For large files, ensure multipart upload settings are optimized for your network conditions.

4. **Buffer Directories**: Use fast local storage for `fs.oss.buffer.dir` to improve multipart operation performance.

5. **Prefetch**: Enable Prefetch for improved read performance on large files with sequential access patterns.

6. **Accelerator Domain**: Configure accelerator domain and rules for improved performance on specific file types or access patterns.

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify your AccessKey ID and Secret are correct and have appropriate permissions.

2. **Connection Timeouts**: Increase `fs.oss.connection.timeout` if you're experiencing network issues.

3. **Performance Issues**: Check your block size and connection settings.

### Logging

Enable debug logging for troubleshooting:

```properties
log4j.logger.org.apache.hadoop.fs.aliyun.oss=DEBUG
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

Please ensure your code follows the existing style and includes appropriate tests.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please create an issue on the GitHub repository or contact the maintainers.