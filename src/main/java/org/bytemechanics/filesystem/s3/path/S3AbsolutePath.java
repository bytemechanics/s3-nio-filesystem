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
package org.bytemechanics.filesystem.s3.path;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bytemechanics.filesystem.s3.S3FileSystem;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3AbsolutePath extends S3Path {

    private final String bucket;
	
    public S3AbsolutePath(final String _bucket,final S3FileSystem _fileSystem, String _first, String... _more) {
		super(_fileSystem,_bucket,Stream.concat(Stream.of(_first),Stream.of(_more))
											.filter(element -> !element.isEmpty())
											.collect(Collectors.toList())
											.toArray(new String[0]));
		this.bucket=Optional.ofNullable(_bucket)
					.orElseThrow(() -> new NullPointerException("Mandatory parameter _bucket"));
    }

	@Override
	protected Path buildPath(S3FileSystem _fileSystem, String _first, String... _more) {
		final String efectiveFirst=(_first.startsWith(PATH_SEPARATOR+this.bucket))? _first.substring(this.bucket.length()+1) : _first;
		return new S3AbsolutePath(this.bucket,_fileSystem, efectiveFirst, _more);
	}

	public String getBucket() {
		return bucket;
	}
	public String getBucketPath() {
		return segmentsToString(getFolders()
									.stream()
										.skip(1))
				.substring(1);
	}

	@Override
	public boolean isAbsolute() {
		return true;
	}

	@Override
	public Path getRoot() {
		return buildPath(getFileSystem(),"");
	}

	@Override
	public Path toAbsolutePath() {
		return this;
	}

	@Override
	public int hashCode() {
		int hash = super.hashCode();
		hash = 23 * hash + Objects.hashCode(this.bucket);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final S3AbsolutePath other = (S3AbsolutePath) obj;
		return (super.equals(obj))&&(Objects.equals(this.bucket, other.bucket));
	}
	
}
