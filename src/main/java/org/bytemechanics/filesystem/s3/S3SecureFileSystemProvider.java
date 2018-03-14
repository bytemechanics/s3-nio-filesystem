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

import java.net.URI;
import org.bytemechanics.filesystem.s3.internal.URIBuilder;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3SecureFileSystemProvider extends S3FileSystemProvider{

	private static final String FILE_SYSTEM_SCHEME="ss3";
	
	@Override
	public String getScheme() {
		return FILE_SYSTEM_SCHEME;
	}
	
	@Override
	protected URI clientURI(final URI _uri){
		return URIBuilder.build(_uri, "https");
	}
}
