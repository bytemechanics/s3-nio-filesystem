# S3 NIO Filesystem
[![Latest version](https://maven-badges.herokuapp.com/maven-central/org.bytemechanics.filesystem/s3-nio-filesystem/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.bytemechanics.filesystem/s3-nio-filesystem/badge.svg)
[![Quality Gate](https://sonarcloud.io/api/badges/gate?key=org.bytemechanics.filesystem%3As3-nio-filesystem)](https://sonarcloud.io/dashboard/index/org.bytemechanics.filesystem%3As3-nio-filesystem)
[![Coverage](https://sonarcloud.io/api/badges/measure?key=org.bytemechanics.filesystem%3As3-nio-filesystem&metric=coverage)](https://sonarcloud.io/dashboard/index/org.bytemechanics.filesystem%3As3-nio-filesystem)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

S3 Filesystem is a library to convert S3 protocol to a NIO2 filesystem
Warning: This library does not accomplish the zero-dependencies objective

## Motivation
Utility to test the concept of create a virtual filesystem from s3 service

## Quick start
1. First of all include the Jar file in your compile and execution classpath.
### Maven
```Maven
	<dependency>
		<groupId>org.bytemechanics.filesystem</groupId>
		<artifactId>s3-nio-filesystem</artifactId>
		<version>X.X.X</version>
	</dependency>
```
### Graddle
```Gradle
dependencies {
    compile 'org.bytemechanics.filesystem:s3-nio-filesystem:X.X.X'
}
```
1. Create filesystem
```Java
package mypackage;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
public class S3FileSystemTest {
	
	private static final String S3URI="s3://{0}:{1}@192.168.56.1:9000";
	private static final String S3USER="9OIA67H2VBDP5T62ZCHK";
	private static final String S3PASSWORD="7a0iE4IHHeE5Curn8SJJG7Xe5a3plQ/YZ5sgedEM";
	private static final String S3BUCKET="es-spl";
	private static final long PROPERTY_MULTIPART_UPLOAD_MINSIZE=Long.MAX_VALUE;
	private static final long PROPERTY_CONNECTION_TIMEOUT=100l;
	private FileSystem fileSystem;
	
	public void S3FileSystemTest() throws IOException{
		Map<String,String> environment= new HashMap<>();
		environment.put(S3FileSystemEnvironment.PROPERTY_MULTIPART_UPLOAD_MINSIZE.name(),String.valueOf(PROPERTY_MULTIPART_UPLOAD_MINSIZE));
		environment.put(S3FileSystemEnvironment.PROPERTY_CONNECTION_TIMEOUT.name(),String.valueOf(PROPERTY_CONNECTION_TIMEOUT));
		String encodedUser=URLEncoder.encode(S3USER,"UTF-8");
		String encodedPassword=URLEncoder.encode(S3PASSWORD,"UTF-8");
		URI uri=URI.create(MessageFormat.format("s3://{0}:{1}@192.168.56.1:9000/{2}",encodedUser,encodedPassword,S3BUCKET));
		fileSystem=FileSystems.newFileSystem(uri,environment);
	}
}
```
1. Get path from filesystem
```Java
	Path path=this.fileSystem.getPath("test.pdf");
```