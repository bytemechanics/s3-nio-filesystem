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
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.EnumMap;
import java.util.Map;
import org.bytemechanics.filesystem.s3.attributes.S3FileStoreAttribute;
import org.bytemechanics.filesystem.s3.attributes.S3FileStoreAttributeView;
import org.jclouds.blobstore.domain.StorageMetadata;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3FileStore extends FileStore{

	private final String name;
	private final S3FileStoreAttributeView attributeView;
	
	
	protected S3FileStore(final StorageMetadata _metadata){
		this.name=_metadata.getName();
		final Map<S3FileStoreAttribute,Object> attributes=new EnumMap<>(S3FileStoreAttribute.class);
		attributes.put(S3FileStoreAttribute.ETAG,_metadata.getETag());
		attributes.put(S3FileStoreAttribute.ID,_metadata.getUserMetadata().get(S3FileStoreAttribute.ID.getKey()));
		attributes.put(S3FileStoreAttribute.LAST_MODIFIED,_metadata.getUserMetadata().get(S3FileStoreAttribute.LAST_MODIFIED.getKey()));
		attributes.put(S3FileStoreAttribute.METADATA,_metadata.getUserMetadata().get(S3FileStoreAttribute.METADATA.getKey()));
		attributes.put(S3FileStoreAttribute.MILLISECOND_PRECISION,_metadata.getUserMetadata().get(S3FileStoreAttribute.MILLISECOND_PRECISION.getKey()));
		attributes.put(S3FileStoreAttribute.PUBLIC,_metadata.getUserMetadata().get(S3FileStoreAttribute.PUBLIC.getKey()));
		attributes.put(S3FileStoreAttribute.RECURSIVE_DELETE,_metadata.getUserMetadata().get(S3FileStoreAttribute.RECURSIVE_DELETE.getKey()));
		attributes.put(S3FileStoreAttribute.ROOTCONTAINER,_metadata.getUserMetadata().get(S3FileStoreAttribute.ROOTCONTAINER.getKey()));
		attributes.put(S3FileStoreAttribute.SIZE,_metadata.getUserMetadata().get(S3FileStoreAttribute.SIZE.getKey()));
		attributes.put(S3FileStoreAttribute.SKIP_CREATE_CONTAINER,_metadata.getUserMetadata().get(S3FileStoreAttribute.SKIP_CREATE_CONTAINER.getKey()));
		this.attributeView=new S3FileStoreAttributeView(attributes);
	}
	
	
	@Override
	public String name() {
		return this.name;
	}

	@Override
	public String type() {
		return "s3-Container";
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public long getTotalSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public long getUsableSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public long getUnallocatedSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean supportsFileAttributeView(Class<? extends FileAttributeView> _type) {
		return false;
	}

	@Override
	public boolean supportsFileAttributeView(String _name) {
		return false;
	}

	@Override
	public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> _type) {
		if (_type != S3FileStoreAttributeView.class)
			throw new IllegalArgumentException(String.format("FileStoreAttributeView of type %1$s is not supported.",_type.getName()));
		return (V)this.attributeView;
	}

	@Override
	public Object getAttribute(String _attribute) throws IOException {
		return this.attributeView.getAttribute(_attribute);
	}
}
