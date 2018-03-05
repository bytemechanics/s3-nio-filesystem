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

import org.jclouds.blobstore.attr.BlobCapability;

/**
 * @author afarre
 * @since 0.1.0
 */
public enum S3FileAttribute {
	
	ID(BlobCapability.ID.name()),
	NAME("s3.file.name"),
	SIZE(BlobCapability.SIZE.name()),
	CREATION_DATE("s3.file.date.creation"),
	LAST_MODIFIED(BlobCapability.LAST_MODIFIED.name()),
	LOCATION("s3.file.location"),
	MD5(BlobCapability.MD5.name()),
	CONTENT_METADATA("s3.file.content-metadata"),
	ETAG(BlobCapability.ETAG.name()),
	TYPE("s3.file.type"),
	METADATA(BlobCapability.METADATA.name()),
	ENCODING("s3.file.encoding"),
	LANGUAGE("s3.file.language"),
	CONTENT_TYPE("s3.file.content-type"),
	CONTENT_DISPOSITION("s3.file.content-disposition"),
	;
	
	private final String key;
	
	S3FileAttribute(final String _key){
		this.key=_key;
	}
	
	public String getKey(){
		return this.key;
	}
}
