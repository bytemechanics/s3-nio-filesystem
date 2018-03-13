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

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bytemechanics.filesystem.s3.internal.S3Client;
import org.bytemechanics.filesystem.s3.internal.copy.commons.string.SimpleFormat;
import org.bytemechanics.filesystem.s3.path.S3AbsolutePath;
import org.jclouds.blobstore.domain.StorageType;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3FileSystem extends FileSystem{

	private final URI uri;
	private final S3FileSystemProvider provider;
	private final String bucket;
	private S3Client client;
	
	protected S3FileSystem(final URI _uri,final S3FileSystemProvider _provider) throws IOException{
		this(_uri,_provider,null);
	}
	protected S3FileSystem(final URI _uri,final S3FileSystemProvider _provider,final S3Client _client) throws IOException{
		this.uri=_uri;
		this.bucket=Optional.ofNullable(_uri.getPath())
								.map(path -> path.substring(1))
								.map(path -> path.replace('/','-'))
								.orElseThrow(() -> new IOException(SimpleFormat.format("URI {} must have bucket as first level path",_uri)));
		this.provider=_provider;
		this.client=_client;
	}
	
	
	public URI getKey(){
		return this.uri;
	}
	
	protected S3Client getClient(){
		return this.client;
	}
	protected S3FileSystem setClient(final S3Client _client){
		this.client=_client;
		return this;
	}
	
	@Override
	public FileSystemProvider provider() {
		return this.provider;
	}

	@Override
	public boolean isOpen() {
		return (this.client!=null);
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public String getSeparator() {
		return S3AbsolutePath.PATH_SEPARATOR;
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		return this.client.listStorage()
								.filter(storageMetadata -> storageMetadata.getType().equals(StorageType.CONTAINER))
								.filter(storageMetadata -> storageMetadata.getName().equals(this.bucket))
								.map(S3FileStore::new)
								.collect(Collectors.toList());
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		return Stream.of(new S3AbsolutePath(this.bucket,this,""))
						.collect(Collectors.toList());
	}


	@Override
	public Set<String> supportedFileAttributeViews() {
		return Stream.of("basic","posix")
						.collect(Collectors.toSet());
	}

	@Override
	public Path getPath(final String _first,final String... _more) {
		return new S3AbsolutePath(this.bucket,this,_first,_more);
	}

	@Override
	public PathMatcher getPathMatcher(String _syntaxAndPattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchService newWatchService() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	@SuppressWarnings("ConvertToTryWithResources")
	public void close() throws IOException {
		final S3Client currentClient=this.client;
		this.client=null;
		this.provider.disconnectFileSystem(this);
		currentClient.close();
	}
}
