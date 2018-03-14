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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.bytemechanics.filesystem.s3.attributes.S3FileAttribute;
import org.bytemechanics.filesystem.s3.attributes.S3FileAttributeView;
import org.bytemechanics.filesystem.s3.internal.copy.commons.functional.LambdaUnchecker;
import org.bytemechanics.filesystem.s3.internal.copy.commons.string.SimpleFormat;
import org.bytemechanics.filesystem.s3.path.S3AbsolutePath;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3SeekableByteChannel implements SeekableByteChannel {

    private final S3AbsolutePath path;
    private final Set<StandardOpenOption> options;
    private final SeekableByteChannel seekable;
    private final Path tempFile;
	private final S3FileAttributeView attributes;
	private final S3Client client;

	
	
    /**
     * Open or creates a file, returning a seekable byte channel
     * @param _path    the path open or create
	 * @param _client s3 client
     * @param _options options specifying how the file is opened
     * @throws IOException if an I/O error occurs
     */
    public S3SeekableByteChannel(final S3AbsolutePath _path,final S3Client _client,final Set<StandardOpenOption> _options) throws IOException {
        this.path = _path;
		this.client=_client;
        this.options =_options;
        this.attributes = _client.getBlobMetadata(_path)
									.map(S3FileAttributeView::new)
									.orElse(null);
        if((this.options.contains(StandardOpenOption.CREATE_NEW))&&(this.attributes!=null))
            throw new FileAlreadyExistsException(SimpleFormat.format("Object {} already exists", _path));
        if((!this.options.contains(StandardOpenOption.CREATE)&&!this.options.contains(StandardOpenOption.CREATE_NEW))&&(this.attributes==null))
            throw new FileAlreadyExistsException(SimpleFormat.format("Object {} not exists", _path));
        this.tempFile = Files.createTempFile("temp-s3-", _path.toString().replaceAll("/", "_"));
        boolean removeTempFile = true;
        try {
            if (this.attributes!=null) {
				_client.getBlob(_path)
						.ifPresent(payload -> {
							try (InputStream inputStream=new BufferedInputStream(payload.openStream())){
								Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
							} catch (IOException e) {
					            LambdaUnchecker.uncheckedGet(
										() -> {throw new IOException(SimpleFormat.format("Can not recover object {}", _path));}
								);
							}
						});
            }
            Set<? extends OpenOption> seekOptions = new HashSet<>(this.options);
            seekOptions.remove(StandardOpenOption.CREATE_NEW);
            this.seekable = Files.newByteChannel(this.tempFile, _options.stream()
																	.filter(option -> !StandardOpenOption.CREATE_NEW.equals(option))
																	.collect(Collectors.toSet()));
            removeTempFile = false;
        } finally {
            if (removeTempFile) {
                Files.deleteIfExists(this.tempFile);
            }
        }
    }

    @Override
    public boolean isOpen() {
        return this.seekable.isOpen();
    }

    @Override
    public void close() throws IOException {
        try {
            if (!this.seekable.isOpen())
                return;
            this.seekable.close();
            if (this.options.contains(StandardOpenOption.DELETE_ON_CLOSE)) {
                this.path.getFileSystem().provider().delete(path);
                return;
            }
            if (this.options.contains(StandardOpenOption.READ) && this.options.size() == 1) {
                return;
            }
            reSynchronize();
        } finally {
            Files.deleteIfExists(this.tempFile);
        }
    }

    /**
     * try to reSynchronize the temp file with the remote s3 path.
	 * @return the s3 identifier
     * @throws IOException if the tempFile fails to open a newInputStream
     */
    protected String reSynchronize() throws IOException {
        try(InputStream stream = new BufferedInputStream(Files.newInputStream(this.tempFile))){
			return this.client.putBlob(this.path,
								Files.size(this.tempFile), 
								stream, 
								Optional.ofNullable(this.attributes)
										.map(attribs -> attribs.getAttribute(S3FileAttribute.CONTENT_TYPE))
										.map(contentType -> (String)contentType)
										.orElse("application/octet-stream"),
								Optional.ofNullable(this.attributes)
										.map(attribs -> attribs.getAttribute(S3FileAttribute.METADATA))
										.map(contentType -> (Map<String, String>)contentType)
										.orElse(Collections.emptyMap()));
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return this.seekable.write(src);
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return this.seekable.truncate(size);
    }

    @Override
    public long size() throws IOException {
        return this.seekable.size();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return this.seekable.read(dst);
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return this.seekable.position(newPosition);
    }

    @Override
    public long position() throws IOException {
        return this.seekable.position();
    }
}
