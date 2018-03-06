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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bytemechanics.filesystem.s3.S3FileSystem;

/**
 * @author afarre
 * @since 0.1.0
 */
public abstract class S3Path implements Path {

    public static final String PATH_SEPARATOR = "/";
    
    protected final S3FileSystem fileSystem;
	protected final List<String> folders;

    protected S3Path(final S3FileSystem _fileSystem,final String _first,final String... _more) {
		this.fileSystem=Optional.ofNullable(_fileSystem)
									.orElseThrow(() -> new NullPointerException("Mandatory parameter filesystem"));
		if(_first==null)
			throw new NullPointerException("Mandatory _first path");
		this.folders=Stream.concat(Stream.of(_first),Stream.of(_more))
								.flatMap(pathUnit -> Stream.of(pathUnit.split(PATH_SEPARATOR))
															.filter(pathElement -> !pathElement.trim().isEmpty()))
								.map(String::trim)
								.collect(Collectors.toList());
    }
	
	protected List<String> getFolders(){
		return this.folders.stream()
								.collect(Collectors.toList());
	}
	protected abstract Path buildPath(final S3FileSystem _fileSystem,final String _first,final String... _more);
	
    @Override
    public S3FileSystem getFileSystem() {
        return this.fileSystem;
    }

    @Override
    public Path getFileName() {
		return new S3RelativePath(this.fileSystem, this.folders.get(this.folders.size()-1));
    }

    @Override
    public Path getParent() {
		
		Path reply=null;
		
        if(!this.folders.isEmpty()) {
			reply=buildPath(this.fileSystem, this.folders.get(this.folders.size()-2));
		}
		
		return reply;
    }

    @Override
    public int getNameCount() {
        return this.folders.size();
    }

    @Override
    public Path getName(final int _index) {

		Path reply;
		
		if((_index<0)||(_index>this.folders.size()))
			 throw new IllegalArgumentException("index out of range");
		reply=new S3RelativePath(this.fileSystem, this.folders.get(_index));

		return reply;
    }

    @Override
    public Path subpath(final int _beginIndex,final int _endIndex) {

        Path reply;

		if((_beginIndex<0)||(_beginIndex>this.folders.size()))
			 throw new IllegalArgumentException("beginIndex out of range");
		if((_endIndex<0)||(_endIndex>this.folders.size()))
			 throw new IllegalArgumentException("endIndex out of range");
		if(_beginIndex>_endIndex)
			 throw new IllegalArgumentException("beginIndex must be below endIndex");

		reply=new S3RelativePath(fileSystem
								,this.folders.get(_beginIndex)
								, this.folders
										.subList(_beginIndex+1, _endIndex)
										.toArray(new String[0]));

        return reply;
    }

	@Override
    public boolean startsWith(final Path _other) {
		final String currentPath=pathToString(this);
		final String starterPath=pathToString(_other);
		return currentPath.startsWith(starterPath);
    }


    @Override
    public boolean startsWith(final String _path) {
        return this.startsWith(buildPath(this.fileSystem, _path));
    }

    @Override
    public boolean endsWith(final Path _other) {
		final String currentPath=pathToString(this);
		final String endsPath=pathToString(_other);
		return currentPath.endsWith(endsPath);
    }

    @Override
    public boolean endsWith(final String _other) {
        return this.endsWith(buildPath(this.fileSystem, _other));
    }

    @Override
    public Path normalize() {
        return this;
    }

    @Override
    public Path resolve(final Path _other) {
		return buildPath(this.fileSystem, pathToString(this),pathToString(_other));
    }

    @Override
    public Path resolve(final String _other) {
		return buildPath(this.fileSystem, pathToString(this),_other);
    }

    @Override
    public Path resolveSibling(final Path _other) {
		return buildPath(this.fileSystem, pathToString(this.getParent()),pathToString(_other));
    }

    @Override
    public Path resolveSibling(final String _other) {
		return buildPath(this.fileSystem, pathToString(this.getParent()),_other);
    }

    @Override
    public Path relativize(final Path _other) {
		return new S3RelativePath(this.fileSystem, pathToString(_other).substring(pathToString(this).length()));
    }

    @Override
    public URI toUri() {
		return URI.create(pathToString(this));
    }

    public URL toURL() {
		return null;
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        return toAbsolutePath();
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator() {
		return this.folders
					.stream()
						.map(element -> buildPath(this.fileSystem, element))
						.map(s3path -> (Path)s3path)
						.iterator();
    }

    @Override
    public int compareTo(Path other) {
        return toString().compareTo(other.toString());
    }

    @Override
    public String toString() {
        return pathToString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        return pathToString(this)
				.equals(pathToString((S3Path) o));
    }

    @Override
    public int hashCode() {
		return pathToString(this).hashCode();
    }


	protected String pathToString(final Path _path){
		return Optional.ofNullable(_path)
						.filter(path -> path instanceof S3Path)
						.map(path -> (S3Path)path)
						.map(S3Path::getFolders)
						.map(List::stream)
						.map(this::segmentsToString)
						.orElse(null);
	}
	protected String segmentsToString(final Stream<String> _segments){
		return _segments
					.collect(Collectors
								.joining(S3Path.PATH_SEPARATOR
										,S3Path.PATH_SEPARATOR
										, ""));
	}
}
