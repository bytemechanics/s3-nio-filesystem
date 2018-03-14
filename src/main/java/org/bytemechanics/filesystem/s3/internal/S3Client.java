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
package org.bytemechanics.filesystem.s3.internal;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;
import com.google.inject.Module;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import org.bytemechanics.filesystem.s3.path.S3AbsolutePath;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;
import org.jclouds.io.PayloadEnclosing;
import static org.jclouds.io.Payloads.newByteArrayPayload;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3Client implements Closeable{

	private static final String FOLDER_SELF = ".self";
	private static final String MULTIPART_MINSIZE="s3.filesystem.upload.multipart.minsize";
	
	private final long multipartMinSize;
	private final BlobStore blobStore;
	
	public S3Client(final URI _endpoint,final String _user,final String _password){
		this(_endpoint, _user, _password,null);
	}
	public S3Client(final URI _endpoint,final String _user,final String _password,final Properties _environment){
		this.blobStore=ContextBuilder
							.newBuilder("s3")
								.endpoint(_endpoint.toString())
								.overrides(Optional.ofNullable(_environment).orElse(new Properties()))
								.modules(ImmutableSet.<Module> of(new SLF4JLoggingModule()))
								.credentials(_user,_password)
								.buildView(BlobStoreContext.class)
									.getBlobStore();
		this.multipartMinSize=Optional.ofNullable(_environment.getProperty(MULTIPART_MINSIZE))
										.map(Long::valueOf)
										.orElse(Long.MAX_VALUE);
	}
	
	public Stream<StorageMetadata> listStorage(){
		return this.blobStore.list()
								.stream()
									.map(storage -> (StorageMetadata)storage);
	}
	public String createFolder(final S3AbsolutePath _path){
		return Optional.ofNullable(_path)
					.map(path -> path.resolve(FOLDER_SELF))
					.map(path -> (S3AbsolutePath)path)
					.map(absolutePath -> Tuple.of(absolutePath,
													this.blobStore.blobBuilder(absolutePath.getBucketPath())
																		.type(StorageType.FOLDER)
																		.payload(newByteArrayPayload(new byte[] {}))
																		.contentType("application/directory")
																	.build()))
					.map(tuple -> this.blobStore.putBlob(tuple.left().getBucket(),tuple.right()))
					.orElse(null);
	}
	public void deleteBlob(final S3AbsolutePath _path){
		Optional.ofNullable(_path)
				.map(path -> 
						(!this.blobStore.blobExists(path.getBucket(), path.getBucketPath()))? 
								path.resolve(FOLDER_SELF) 
								: path)
				.map(path -> (S3AbsolutePath)path)
				.ifPresent(path -> this.blobStore.removeBlob(path.getBucket(), path.getBucketPath()));
	}
	public String copyBlob(final S3AbsolutePath _sourcePath,final S3AbsolutePath _targePath,final CopyOptions _copyOptions){
		return this.blobStore.copyBlob(_sourcePath.getBucket(), _sourcePath.getBucketPath(),_targePath.getBucket(), _targePath.getBucketPath(),_copyOptions);
	}
	public boolean exist(final S3AbsolutePath _path){
		return this.blobStore.blobExists(_path.getBucket(), _path.getBucketPath())
				||this.blobStore.blobExists(_path.getBucket(),((S3AbsolutePath)_path.resolve(FOLDER_SELF)).getBucketPath());
	}
	public Optional<BlobMetadata> getBlobMetadata(final S3AbsolutePath _path){
		return Optional.ofNullable(Optional.ofNullable(this.blobStore.blobMetadata(_path.getBucket(), _path.getBucketPath()))
								.orElseGet(() -> 
										this.blobStore.blobMetadata(_path.getBucket(), ((S3AbsolutePath)_path.resolve(FOLDER_SELF)).getBucketPath())));
	}
	public Optional<Payload> getBlob(final S3AbsolutePath _path){
		return Optional.ofNullable(this.blobStore.getBlob(_path.getBucket(), _path.getBucketPath()))
									.map(PayloadEnclosing::getPayload);
	}
	public String putBlob(final S3AbsolutePath _path,final long _length,final InputStream _stream,final String _mediaType,final Map<String,String> _userMetadata){
		final Blob blob=this.blobStore.blobBuilder(_path.getBucketPath())
											.payload(_stream)
											.contentType(Optional.ofNullable(_mediaType)
																	.orElseGet(MediaType.ANY_APPLICATION_TYPE::toString))
											.contentLength(_length)
											.userMetadata(Optional.ofNullable(_userMetadata)
																	.orElseGet(Collections::emptyMap))
										.build();
		return this.blobStore.putBlob(_path.getBucket(), blob, PutOptions.Builder.multipart((this.multipartMinSize<blob.getMetadata().getContentMetadata().getContentLength())));
	}

	@Override
	public void close() throws IOException {
		this.blobStore
				.getContext()
				.close();
	}
}