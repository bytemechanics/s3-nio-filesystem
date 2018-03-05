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

import org.jclouds.blobstore.attr.ContainerCapability;

/**
 * @author afarre
 * @since 0.1.0
 */
public enum S3FileStoreAttribute {
	
	ETAG(ContainerCapability.ETAG.name()),
	ID(ContainerCapability.ID.name()),
	LAST_MODIFIED(ContainerCapability.LAST_MODIFIED.name()),
	METADATA(ContainerCapability.METADATA.name()),
	MILLISECOND_PRECISION(ContainerCapability.MILLISECOND_PRECISION.name()),
	PUBLIC(ContainerCapability.PUBLIC.name()),
	RECURSIVE_DELETE(ContainerCapability.RECURSIVE_DELETE.name()),
	ROOTCONTAINER(ContainerCapability.ROOTCONTAINER.name()),
	SIZE(ContainerCapability.SIZE.name()),
	SKIP_CREATE_CONTAINER(ContainerCapability.SKIP_CREATE_CONTAINER.name()),
	;
	
	private final String key;
	
	S3FileStoreAttribute(final String _key){
		this.key=_key;
	}
	
	public String getKey(){
		return this.key;
	}
}
