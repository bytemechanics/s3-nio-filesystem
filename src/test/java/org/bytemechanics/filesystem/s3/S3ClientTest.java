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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import org.bytemechanics.filesystem.s3.internal.S3Client;
import org.bytemechanics.filesystem.s3.path.S3AbsolutePath;
import org.jclouds.io.Payload;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author afarre
 * @since 0.1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class S3ClientTest {
	
	private static final URI S3URI=URI.create("http://192.168.56.1:9000/es-spl");
	private static final String S3USER="9OIA67H2VBDP5T62ZCHK";
	private static final String S3PASSWORD="7a0iE4IHHeE5Curn8SJJG7Xe5a3plQ/YZ5sgedEM";
	private static final String S3BUCKET="es-spl";
	private static final Path LOCAL_FILE=Paths.get("src/test/resources/test.pdf");
	private static final long PROPERTY_MULTIPART_UPLOAD_MINSIZE=Long.MAX_VALUE;
	private static final long PROPERTY_CONNECTION_TIMEOUT=100l;
	private S3Client client;
	
	@Before
	public void before(){
		Properties properties=new Properties();
		properties.setProperty(S3FileSystemEnvironment.PROPERTY_MULTIPART_UPLOAD_MINSIZE.getkey(),String.valueOf(PROPERTY_MULTIPART_UPLOAD_MINSIZE));
		properties.setProperty(S3FileSystemEnvironment.PROPERTY_CONNECTION_TIMEOUT.getkey(),String.valueOf(PROPERTY_CONNECTION_TIMEOUT));
		this.client=new S3Client(S3URI,S3USER,S3PASSWORD,properties);
	}
	
	@Test
	public void t01PutBlobTest() throws IOException{
		System.out.println("S3ClientTest >>> t01PutBlobTest");
		try(InputStream stream=Files.newInputStream(LOCAL_FILE,StandardOpenOption.READ)){
			S3AbsolutePath path=new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test.pdf");
			String reply=this.client.putBlob(path,Files.size(LOCAL_FILE), stream, "application/pdf", Collections.emptyMap());
			Assert.assertNotNull(reply);
			Assert.assertTrue(this.client.exist(path));
		}
	}
	@Test
	public void t02GetBlobTest() throws IOException{
		System.out.println("S3ClientTest >>> t02GetBlobTest");
		S3AbsolutePath path=new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test.pdf");
		Optional<Payload> payload=this.client.getBlob(path);
		Assert.assertTrue(payload.isPresent());
		Assert.assertEquals(Long.valueOf(Files.size(LOCAL_FILE)),payload.get().getContentMetadata().getContentLength());
		byte[] actualBuffer;
		try(InputStream actual=new BufferedInputStream(payload.get().openStream());
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
		System.out.println("S3ClientTest >>> t03DeleteBlobTest");
		S3AbsolutePath path=new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test.pdf");
		this.client.deleteBlob(path);
	}
	@Test
	public void t04GetDeletedBlobTest() throws IOException{
		System.out.println("S3ClientTest >>> t04GetDeletedBlobTest");
		S3AbsolutePath path=new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test.pdf");
		Optional<Payload> payload=this.client.getBlob(path);
		Assert.assertFalse(payload.isPresent());
	}
	
	@Test
	public void t05CreateFolderTest() throws IOException{
		System.out.println("S3ClientTest >>> t05CreateFolderTest");
		try(InputStream stream=Files.newInputStream(LOCAL_FILE,StandardOpenOption.READ)){
			S3AbsolutePath path=new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test");
			String reply=this.client.createFolder(path);
			Assert.assertNotNull(reply);
			Assert.assertTrue(this.client.exist(path));
		}
	}
	@Test
	public void t06FolderPutBlobTest() throws IOException{
		System.out.println("S3ClientTest >>> t06FolderPutBlobTest");
		try(InputStream stream=Files.newInputStream(LOCAL_FILE,StandardOpenOption.READ)){
			S3AbsolutePath path=(S3AbsolutePath)new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test")
													.resolve("test.pdf");
			String reply=this.client.putBlob(path,Files.size(LOCAL_FILE), stream, "application/pdf", Collections.emptyMap());
			Assert.assertNotNull(reply);
			Assert.assertTrue(this.client.exist(path));
		}
	}
	@Test
	public void t07FolderGetBlobTest() throws IOException{
		System.out.println("S3ClientTest >>> t07FolderGetBlobTest");
		S3AbsolutePath path=(S3AbsolutePath)new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test")
													.resolve("test.pdf");
		Optional<Payload> payload=this.client.getBlob(path);
		Assert.assertTrue(payload.isPresent());
		Assert.assertEquals(Long.valueOf(Files.size(LOCAL_FILE)),payload.get().getContentMetadata().getContentLength());
		byte[] actualBuffer;
		try(InputStream actual=new BufferedInputStream(payload.get().openStream());
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
		System.out.println("S3ClientTest >>> t08FolderDeleteBlobTest");
		S3AbsolutePath path=(S3AbsolutePath)new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test")
													.resolve("test.pdf");
		this.client.deleteBlob(path);
	}
	@Test
	public void t09FolderGetDeletedBlobTest() throws IOException{
		System.out.println("S3ClientTest >>> t09FolderGetDeletedBlobTest");
		S3AbsolutePath path=(S3AbsolutePath)new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test")
													.resolve("test.pdf");
		Optional<Payload> payload=this.client.getBlob(path);
		Assert.assertFalse(payload.isPresent());
	}
	@Test
	public void t10DeleteFolderTest() throws IOException{
		System.out.println("S3ClientTest >>> t10DeleteFolderTest");
		S3AbsolutePath path=(S3AbsolutePath)new S3AbsolutePath(S3BUCKET,new S3FileSystem(S3URI,null,this.client),"test");
		this.client.deleteBlob(path);
		Optional<Payload> payload=this.client.getBlob(path);
		Assert.assertFalse(payload.isPresent());
	}
}
