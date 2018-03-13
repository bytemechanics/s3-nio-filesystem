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

import org.jclouds.Constants;

/**
 * @author afarre
 * @since 0.1.0
 */
public enum S3FileSystemEnvironment {
	
	PROPERTY_CONNECTION_READONLY("s3.filesystem.readonly"),
	PROPERTY_CONNECTION_USER("s3.filesystem.user"),
	PROPERTY_CONNECTION_PASSWORD("s3.filesystem.password"),
	PROPERTY_MULTIPART_UPLOAD_MINSIZE("s3.filesystem.upload.multipart.minsize"),
	PROPERTY_CONNECTION_TIMEOUT(Constants.PROPERTY_CONNECTION_TIMEOUT),
	PROPERTY_MAX_CONNECTIONS_PER_CONTEXT(Constants.PROPERTY_MAX_CONNECTIONS_PER_CONTEXT),
	PROPERTY_MAX_CONNECTIONS_PER_HOST(Constants.PROPERTY_MAX_CONNECTIONS_PER_HOST),
	PROPERTY_RELAX_HOSTNAME(Constants.PROPERTY_RELAX_HOSTNAME),
	PROPERTY_REQUEST_TIMEOUT(Constants.PROPERTY_REQUEST_TIMEOUT),
	PROPERTY_SO_TIMEOUT(Constants.PROPERTY_SO_TIMEOUT),
	PROPERTY_TRUST_ALL_CERTS(Constants.PROPERTY_TRUST_ALL_CERTS),
	PROPERTY_PROXY_ENABLED(Constants.PROPERTY_PROXY_ENABLE_JVM_PROXY),
	PROPERTY_PROXY_HOST(Constants.PROPERTY_PROXY_HOST),
	PROPERTY_PROXY_PORT(Constants.PROPERTY_PROXY_PORT),
	PROPERTY_PROXY_USER(Constants.PROPERTY_PROXY_USER),
	PROPERTY_PROXY_PASSWORD(Constants.PROPERTY_PROXY_PASSWORD),
	;
	
	private final String key;
	
	S3FileSystemEnvironment(final String _key){
		this.key=_key;
	} 

	public String getkey() {
		return key;
	}
}
