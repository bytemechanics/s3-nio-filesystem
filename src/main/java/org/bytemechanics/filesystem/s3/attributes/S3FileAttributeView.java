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
package org.bytemechanics.filesystem.s3.attributes;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.time.Instant;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageType;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3FileAttributeView implements FileAttributeView,BasicFileAttributes,PosixFileAttributes{
	
	public static final String ATTRIBUTE_VIEW_NAME = "S3FileAttributeView";

	private final Map<S3FileAttribute,Object> attributes;
	


    public S3FileAttributeView(final Map<S3FileAttribute,Object> _attributes) {
		this.attributes=_attributes;
    }
    public S3FileAttributeView(final BlobMetadata _blobMetadata) {
		this.attributes=new EnumMap<>(S3FileAttribute.class);
		this.attributes.put(S3FileAttribute.ID, _blobMetadata.getProviderId());
		this.attributes.put(S3FileAttribute.NAME, _blobMetadata.getName());
		this.attributes.put(S3FileAttribute.SIZE, _blobMetadata.getSize());
		this.attributes.put(S3FileAttribute.CREATION_DATE, _blobMetadata.getCreationDate());
		this.attributes.put(S3FileAttribute.LAST_MODIFIED, _blobMetadata.getLastModified());
		this.attributes.put(S3FileAttribute.LOCATION, _blobMetadata.getLocation());
		this.attributes.put(S3FileAttribute.MD5, _blobMetadata.getContentMetadata().getContentMD5AsHashCode());
		this.attributes.put(S3FileAttribute.ETAG, _blobMetadata.getETag());
		this.attributes.put(S3FileAttribute.TYPE, _blobMetadata.getType().name());
		this.attributes.put(S3FileAttribute.METADATA, _blobMetadata.getUserMetadata());
		this.attributes.put(S3FileAttribute.ENCODING, _blobMetadata.getContentMetadata().getContentEncoding());
		this.attributes.put(S3FileAttribute.LANGUAGE, _blobMetadata.getContentMetadata().getContentLanguage());
		this.attributes.put(S3FileAttribute.CONTENT_TYPE, _blobMetadata.getContentMetadata().getContentType());
		this.attributes.put(S3FileAttribute.CONTENT_DISPOSITION, _blobMetadata.getContentMetadata().getContentDisposition());
    }

    @Override
    public String name() {
        return ATTRIBUTE_VIEW_NAME;
    }

    public Object getAttribute(final String _attribute) {
        return getAttribute(S3FileAttribute.valueOf(_attribute));
    }

    public Object getAttribute(final S3FileAttribute _attribute) {
        return this.attributes.get(_attribute);
    }	

	@Override
	public FileTime lastModifiedTime() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.LAST_MODIFIED))
					.map(lastModified -> (Date)lastModified)
					.map(lastModifiedDate -> FileTime.fromMillis(lastModifiedDate.getTime()))
					.orElseGet(() -> FileTime.from(Instant.EPOCH));
	}

	@Override
	public FileTime lastAccessTime() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.LAST_MODIFIED))
					.map(lastModified -> (Date)lastModified)
					.map(lastModifiedDate -> FileTime.fromMillis(lastModifiedDate.getTime()))
					.orElseGet(() -> FileTime.from(Instant.EPOCH));
	}

	@Override
	public FileTime creationTime() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.CREATION_DATE))
					.map(creation -> (Date)creation)
					.map(creationDate -> FileTime.fromMillis(creationDate.getTime()))
					.orElseGet(() -> FileTime.from(Instant.EPOCH));
	}

	@Override
	public boolean isRegularFile() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.CONTENT_TYPE))
					.map(type -> (String)type)
					.map(contentType -> !contentType.equals("application/directory"))
					.orElse(true);
	}

	@Override
	public boolean isDirectory() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.CONTENT_TYPE))
					.map(type -> (String)type)
					.map(contentType -> contentType.equals("application/directory"))
					.orElse(false);
	}

	@Override
	public boolean isSymbolicLink() {
		return false;
	}

	@Override
	public boolean isOther() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.TYPE))
					.map(type -> (String)type)
					.map(storageType -> storageType.equals(StorageType.RELATIVE_PATH.name()))
					.orElse(false);
	}

	@Override
	public long size() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.SIZE))
					.map(size -> (Long)size)
					.orElse(0l);
	}

	@Override
	public Object fileKey() {
		return Optional.ofNullable(this.attributes)
					.map(attr -> attr.get(S3FileAttribute.ID))
					.orElse(null);
	}

	@Override
	public UserPrincipal owner() {
		throw new UnsupportedOperationException("Unsupported posix:owner attribute");
	}

	@Override
	public GroupPrincipal group() {
		throw new UnsupportedOperationException("Unsupported posix:group attribute");
	}

	@Override
	public Set<PosixFilePermission> permissions() {
		throw new UnsupportedOperationException("Unsupported posix:permissions attribute");
	}
}
