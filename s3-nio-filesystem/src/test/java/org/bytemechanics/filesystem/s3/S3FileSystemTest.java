/*
 * Copyright 2017 Byte Mechanics.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bytemechanics.filesystem.s3;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author afarre
 * @since 0.1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class S3FileSystemTest {
	
	private static final String S3URI="s3://{0}:{1}@192.168.56.1:9000";
	private static final String S3USER="9OIA67H2VBDP5T62ZCHK";
	private static final String S3PASSWORD="7a0iE4IHHeE5Curn8SJJG7Xe5a3plQ/YZ5sgedEM";
	private static final String S3BUCKET="es-spl";
	private static final Path LOCAL_FILE=Paths.get("src/test/resources/test.pdf");
	private static final long PROPERTY_MULTIPART_UPLOAD_MINSIZE=Long.MAX_VALUE;
	private static final long PROPERTY_CONNECTION_TIMEOUT=100l;
	private static FileSystem fileSystem;
	
	@BeforeClass
	public static void before() throws IOException{
		Map<String,String> environment= new HashMap<>();
		environment.put(S3FileSystemEnvironment.PROPERTY_MULTIPART_UPLOAD_MINSIZE.name(),String.valueOf(PROPERTY_MULTIPART_UPLOAD_MINSIZE));
		environment.put(S3FileSystemEnvironment.PROPERTY_CONNECTION_TIMEOUT.name(),String.valueOf(PROPERTY_CONNECTION_TIMEOUT));
		String encodedUser=URLEncoder.encode(S3USER,"UTF-8");
		String encodedPassword=URLEncoder.encode(S3PASSWORD,"UTF-8");
		URI uri=URI.create(MessageFormat.format("s3://{0}:{1}@192.168.56.1:9000/{2}",encodedUser,encodedPassword,S3BUCKET));
		fileSystem=FileSystems.newFileSystem(uri,environment);
	}
	
	@Test
	public void t01PutBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t01PutBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test.pdf");
		try(InputStream stream=Files.newInputStream(LOCAL_FILE,StandardOpenOption.READ)){
			Files.copy(stream, path,StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue(Files.exists(path));
			Assert.assertTrue(Files.isRegularFile(path));
			Assert.assertFalse(Files.isDirectory(path));
		}
	}
	@Test
	public void t02GetBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t02GetBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test.pdf");
		byte[] actualBuffer;
		try(InputStream actual=new BufferedInputStream(Files.newInputStream(path,StandardOpenOption.READ));
				ByteArrayOutputStream out=new ByteArrayOutputStream(2048)){
			byte[] buffer=new byte[1024];
			int read=actual.read(buffer);
			while(read>0){
				out.write(buffer, 0, read);
				read=actual.read(buffer);
			}
			actualBuffer=out.toByteArray();
		}
		Assert.assertArrayEquals(Files.readAllBytes(LOCAL_FILE), actualBuffer);
	}
	@Test
	public void t03DeleteBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t03DeleteBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test.pdf");
		Files.delete(path);
		Assert.assertFalse(Files.exists(path));
	}
	@Test(expected=IOException.class)
	public void t04GetDeletedBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t04GetDeletedBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test.pdf");
		try(InputStream actual=new BufferedInputStream(Files.newInputStream(path,StandardOpenOption.READ))){
		}
	}
	
	@Test
	public void t05CreateFolderTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t05CreateFolderTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test");
		Files.createDirectory(path);
		Assert.assertTrue(Files.exists(path));
		Assert.assertTrue(Files.isDirectory(path));
	}
	@Test
	public void t06FolderPutBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t06FolderPutBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test")
									.resolve("test.pdf");
		try(InputStream stream=Files.newInputStream(LOCAL_FILE,StandardOpenOption.READ)){
			Files.copy(stream, path,StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue(Files.exists(path));
			Assert.assertTrue(Files.isRegularFile(path));
			Assert.assertFalse(Files.isDirectory(path));
		}
	}
	@Test
	public void t07FolderGetBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t07FolderGetBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test")
									.resolve("test.pdf");
		byte[] actualBuffer;
		try(InputStream actual=new BufferedInputStream(Files.newInputStream(path,StandardOpenOption.READ));
				ByteArrayOutputStream out=new ByteArrayOutputStream(2048)){
			byte[] buffer=new byte[1024];
			int read=actual.read(buffer);
			while(read>0){
				out.write(buffer, 0, read);
				read=actual.read(buffer);
			}
			actualBuffer=out.toByteArray();
		}
		Assert.assertArrayEquals(Files.readAllBytes(LOCAL_FILE), actualBuffer);
	}
	@Test
	public void t08FolderDeleteBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t08FolderDeleteBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test")
									.resolve("test.pdf");
		Files.delete(path);
		Assert.assertFalse(Files.exists(path));
	}
	@Test(expected = IOException.class)
	public void t09FolderGetDeletedBlobTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t09FolderGetDeletedBlobTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test")
									.resolve("test.pdf");
		try(InputStream actual=new BufferedInputStream(Files.newInputStream(path,StandardOpenOption.READ))){
		}
	}
	@Test
	public void t10DeleteFolderTest() throws IOException{
		System.out.println("S3FileSystemTest >>> t10DeleteFolderTest");
		Path path=S3FileSystemTest.fileSystem.getPath("test");
		Files.delete(path);
		Assert.assertFalse(Files.exists(path));
	}
}
