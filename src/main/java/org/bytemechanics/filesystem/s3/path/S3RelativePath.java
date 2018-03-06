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
import org.bytemechanics.filesystem.s3.S3FileSystem;
import org.bytemechanics.filesystem.s3.internal.copy.commons.string.SimpleFormat;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3RelativePath extends S3Path {

    public S3RelativePath(final S3FileSystem _fileSystem,final String _first,final String... _more) {
		super(_fileSystem,_first,_more);
    }
	
	@Override
	protected Path buildPath(final S3FileSystem _fileSystem,final String _first,final String... _more){
		return new S3RelativePath(_fileSystem,_first,_more);
	}
	
    @Override
    public boolean isAbsolute() {
        return false;
    }

    @Override
    public Path getRoot() {
        return null;
    }

    @Override
    public Path toAbsolutePath() {
        throw new IllegalStateException(SimpleFormat.format("Relative path cannot be made absolute: {}", this));
    }
}
