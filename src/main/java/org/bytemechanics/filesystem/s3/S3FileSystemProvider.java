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
import java.net.URI;
import java.net.URLDecoder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.bytemechanics.filesystem.s3.attributes.S3FileAttribute;
import org.bytemechanics.filesystem.s3.attributes.S3FileAttributeView;
import org.bytemechanics.filesystem.s3.attributes.S3FileAttributesExtractor;
import org.bytemechanics.filesystem.s3.internal.S3Client;
import org.bytemechanics.filesystem.s3.internal.S3SeekableByteChannel;
import org.bytemechanics.filesystem.s3.internal.Tuple;
import org.bytemechanics.filesystem.s3.internal.copy.commons.functional.LambdaUnchecker;
import org.bytemechanics.filesystem.s3.internal.copy.commons.string.SimpleFormat;
import org.bytemechanics.filesystem.s3.path.S3AbsolutePath;
import org.bytemechanics.filesystem.s3.path.S3Path;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.io.ContentMetadata;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3FileSystemProvider extends FileSystemProvider{

	private static final String FILE_SYSTEM_SCHEME="s3";
	
	private final Map<URI,S3FileSystem> fileSystems;
	
	
	public S3FileSystemProvider(){
		this.fileSystems=new ConcurrentHashMap<>();
	}
	
	
	@Override
	public String getScheme() {
		return FILE_SYSTEM_SCHEME;
	}

	private String getUser(final URI _uri,final Map<String, ?> _environment){
		return Optional.ofNullable(_uri)
					.map(URI::getUserInfo)
					.map(userInfo -> userInfo.split(":"))
					.filter(userInfoSplit -> userInfoSplit.length>0)
					.map(userInfoSplit -> userInfoSplit[0])
					.map(userEncoded -> LambdaUnchecker.uncheckedGet(() -> URLDecoder.decode(userEncoded, "UTF-8")))
					.orElseGet(() -> String.valueOf(_environment.get(S3FileSystemEnvironment.PROPERTY_CONNECTION_USER.getkey())));
		
	}
	private String getPassword(final URI _uri,final Map<String, ?> _environment){
		return Optional.ofNullable(_uri)
					.map(URI::getUserInfo)
					.map(passwordInfo -> passwordInfo.split(":"))
					.filter(passwordInfoSplit -> passwordInfoSplit.length>1)
					.map(passwordInfoSplit -> passwordInfoSplit[1])
					.map(passwordEncoded -> LambdaUnchecker.uncheckedGet(() -> URLDecoder.decode(passwordEncoded, "UTF-8")))
					.orElseGet(() -> String.valueOf(_environment.get(S3FileSystemEnvironment.PROPERTY_CONNECTION_PASSWORD.getkey())));
	}
	private Optional<URI> clean(final URI _uri){
		return  Optional.ofNullable(_uri)
							.map(uri -> LambdaUnchecker.uncheckedGet(() -> new URI(uri.getScheme(),null,uri.getHost(),uri.getPort(),uri.getPath(),null,null)));
	}
	protected URI clientURI(final URI _uri){
		return  Optional.ofNullable(_uri)
							.map(uri -> LambdaUnchecker.uncheckedGet(() -> new URI("http",null,uri.getHost(),uri.getPort(),uri.getPath(),null,null)))
							.orElse(null);
	}
	
	protected boolean existFileSystem(final URI _uri){
		return clean(_uri)
					.map(this.fileSystems::containsKey)
					.orElse(false);
	}

	protected FileSystem putAndGet(final URI _uri,final S3FileSystem _fileSystem){
		this.fileSystems.put(_uri,_fileSystem);
		return _fileSystem;
	}
	protected Optional<S3FileSystem> createFileSystem(final URI _uri){
		return clean(_uri)
				.map(uri -> LambdaUnchecker.uncheckedGet(() -> Tuple.of(uri,new S3FileSystem(uri,this))))
				.map(tuple -> (S3FileSystem)putAndGet(tuple.left(),tuple.right()));
	}
	
	@Override
	public FileSystem newFileSystem(final URI _uri,final Map<String, ?> _environment) throws IOException {
		
		return Optional.ofNullable(_uri)
					.filter(uri -> !existFileSystem(uri))
					.map(uri -> _environment.entrySet()
											.stream()
											.map(entry -> Tuple.of(entry.getKey(),entry.getValue()))
											.map(environmentConfigTuple -> environmentConfigTuple.replaceLeft(S3FileSystemEnvironment.valueOf(environmentConfigTuple.left())))
											.map(environmentConfigTuple -> environmentConfigTuple.replaceLeft(environmentConfigTuple.left().getkey()))
											.map(environmentConfigTuple -> environmentConfigTuple.replaceRight(String.valueOf(environmentConfigTuple.right())))
											.reduce(new Properties()
													,(properties,tuple) -> {properties.setProperty(tuple.left(),tuple.right()); return properties;}
													,(properties1,properties2) -> {properties1.putAll(properties2); return properties1;}))
					.map(config -> Tuple.of(config,createFileSystem(_uri)))
					.map(tuple -> tuple.replaceLeft(new S3Client(clientURI(_uri), getUser(_uri,_environment), getPassword(_uri,_environment), tuple.left())))
					.flatMap(tuple -> tuple.right().map(fileSystem -> fileSystem.setClient(tuple.left())))
					.orElseThrow(() -> new FileSystemAlreadyExistsException(SimpleFormat.format("FileSystem already exist for uri {}", _uri)));
	}

	@Override
	public FileSystem getFileSystem(final URI _uri) {
		return clean(_uri)
					.map(this.fileSystems::get)
					.orElseThrow(() -> new FileSystemNotFoundException(SimpleFormat.format("FileSystem not exist for uri {}", _uri)));
	}

	@Override
	public Path getPath(final URI _uri) {
		return getFileSystem(_uri)
					.getPath(_uri.getPath());
	}

	@Override
	public SeekableByteChannel newByteChannel(final Path _path,final Set<? extends OpenOption> _options,final FileAttribute<?>... _attrs) throws IOException {
		final Set<StandardOpenOption> options=_options.stream()
													.map(openOption -> (StandardOpenOption)openOption)
													.collect(Collectors.toSet());
		final S3AbsolutePath absolutePath=s3AbsolutePathVerified(_path)
											.orElseThrow(() -> new IOException(SimpleFormat.format("Path {} must be absolute",_path)));
		return new S3SeekableByteChannel(absolutePath,absolutePath.getFileSystem().getClient(), options);
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(final Path _dir,final DirectoryStream.Filter<? super Path> _filter) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void createDirectory(final Path _path,final FileAttribute<?>... _attrs) throws IOException {
		s3AbsolutePathVerified(_path)
				.map(s3AbsolutePath -> Tuple.of(s3AbsolutePath,s3AbsolutePath.getFileSystem()))
				.map(s3fileSystemTuple -> s3fileSystemTuple.replaceRight(s3fileSystemTuple.right().getClient()))
				.ifPresent(s3fileSystemTuple -> s3fileSystemTuple.right().createFolder(s3fileSystemTuple.left()));
	}

	@Override
	public void delete(final Path _path) throws IOException {
		existVerified(_path)
				.map(s3AbsolutePath -> Tuple.of(s3AbsolutePath,s3AbsolutePath.getFileSystem()))
				.map(s3fileSystemTuple -> s3fileSystemTuple.replaceRight(s3fileSystemTuple.right().getClient()))
				.ifPresent(s3fileSystemTuple -> s3fileSystemTuple.right().deleteBlob(s3fileSystemTuple.left()));
	}

	@Override
	public void copy(final Path _source,final Path _target,final CopyOption... _options) throws IOException {
		try{
			if(!isSameFile(_source,_target)){
				final Set<StandardCopyOption> options=Stream.of(_options)
															.map(copyOption -> (StandardCopyOption)copyOption)
															.collect(Collectors.toSet());
				final S3AbsolutePath sourcePath=existVerified(_source)
													.orElseThrow(() -> new IOException(SimpleFormat.format("Can not copy non existent source file {}",_source)));
				final S3FileAttributeView fileAttributes=getFileAttributes(sourcePath)
																	.filter(S3FileAttributeView::isRegularFile)
																	.orElseThrow(() -> new IOException(SimpleFormat.format("Source {} is folder: can not copy entire folders",_source)));
				final S3AbsolutePath targetPath=existVerified(_target)
													.orElseThrow(() -> new IOException(SimpleFormat.format("Can not copy non existent target file {}",_target)));

				CopyOptions.Builder copyOptionsBuilder=CopyOptions.builder();
				if(options.contains(StandardCopyOption.COPY_ATTRIBUTES)){
					copyOptionsBuilder.contentMetadata((ContentMetadata)fileAttributes.getAttribute(S3FileAttribute.CONTENT_METADATA));
					copyOptionsBuilder.userMetadata((Map<String,String>)fileAttributes.getAttribute(S3FileAttribute.METADATA));
				}
				sourcePath.getFileSystem()
							.getClient()
								.copyBlob(sourcePath,targetPath,copyOptionsBuilder.build());
			}
		}catch(ClassCastException e){
			throw new UnsupportedOperationException("Only StandardCopyOption supported.",e);
		}
	}

	@Override
	public void move(final Path _source,final Path _target,final CopyOption... _options) throws IOException {
		copy(_source, _target, _options);
		delete(_source);
	}

	@Override
	public boolean isSameFile(final Path _path,final Path _path2) throws IOException {
		return _path.toString().equals(_path2.toString());
	}

	@Override
	public boolean isHidden(final Path _path) throws IOException {
		return existVerified(_path)
					.map(S3Path::getFileName)
					.map(Path::toString)
					.map(filenameString -> filenameString.startsWith("."))
					.orElse(false);
	}

	@Override
	public FileStore getFileStore(final Path _path) throws IOException {
		return s3AbsolutePathVerified(_path)
						.flatMap(path -> StreamSupport.stream(path.getFileSystem().getFileStores().spliterator(), false)
													.filter(filestore -> filestore.name().equals(path.getBucket()))
													.findAny())
						.orElseThrow(() -> new IOException(SimpleFormat.format("No filestore matches with path {}",_path)));
	}

	@Override
	public void checkAccess(final Path _path,final AccessMode... _modes) throws IOException {
		if(!exist(_path)){
			throw new IOException(SimpleFormat.format("File {} not exist",_path));
		}
		if(Stream.of(_modes).anyMatch(AccessMode.EXECUTE::equals)){
			throw new IOException("Access mode EXECUTE not supported");
		}
	}

	protected Optional<S3AbsolutePath> s3AbsolutePathVerified(final Path _path){
		return Optional.ofNullable(_path)
						.filter(Path::isAbsolute)
						.filter(path -> path instanceof S3AbsolutePath)
						.map(path -> (S3AbsolutePath)path);
	}
	protected Optional<S3AbsolutePath> existVerified(final Path _path){
		return s3AbsolutePathVerified(_path)
						.map(s3path -> Tuple.of(s3path,s3path.getFileSystem()))
						.map(s3fileSystemTuple -> s3fileSystemTuple.replaceRight(s3fileSystemTuple.right().getClient()))
						.filter(blobStoreTuple -> blobStoreTuple.right().exist(blobStoreTuple.left()))
						.map(Tuple::left);
	}
	protected boolean exist(final Path _path){
		return existVerified(_path)
				.map(path -> true)
				.orElse(false);
					
	}
	protected Optional<S3FileAttributeView> getFileAttributes(final Path _path){
		return s3AbsolutePathVerified(_path)
					.map(s3path -> Tuple.of(s3path,s3path.getFileSystem()))
					.map(s3fileSystemTuple -> s3fileSystemTuple.replaceRight(s3fileSystemTuple.right().getClient()))
					.flatMap(blobStoreTuple -> blobStoreTuple.right().getBlobMetadata(blobStoreTuple.left()))
					.map(S3FileAttributeView::new);
	}
	
	@Override
	public <V extends FileAttributeView> V getFileAttributeView(final Path _path,final Class<V> _type,final LinkOption... _options) {
		return getFileAttributes(_path)
					.filter(path -> _type.isAssignableFrom(S3FileAttributeView.class))
					.map(attributesView -> (V)attributesView)
					.orElseThrow(() -> new NullPointerException("Null path, not absolute, not S3Path or file not exist"));
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(final Path _path,final Class<A> _type,final LinkOption... _options) throws IOException {
		return getFileAttributes(_path)
					.filter(path -> _type.isAssignableFrom(S3FileAttributeView.class))
					.map(attributesView -> (A)attributesView)
					.orElseThrow(() -> new IOException("Null path, not absolute, not S3Path or file not exist"));
	}

	@Override
	public Map<String, Object> readAttributes(final Path _path,final String _attributes,final LinkOption... _options) throws IOException {
		
		return Optional.ofNullable(_attributes)
							.map(attributes -> (attributes.indexOf(':')>-1)? 
													attributes.split(":") 
													: new String[]{"basic",attributes})
							.map(attributeArray -> Tuple.of(attributeArray[0],attributeArray[1]))
							.map(viewAttributes -> (viewAttributes.right().equals("*"))?
														S3FileAttributesExtractor
																.getAttributes(viewAttributes.left())																		
																.map(extractor -> Tuple.of(extractor.name(),extractor.getOptionalExtractor()))
														: Stream.of(viewAttributes.right().split(","))
																.map(attrib -> S3FileAttributesExtractor.valueOf(viewAttributes.left(),attrib))
																.map(extractor -> Tuple.of(extractor.name(),extractor.getOptionalExtractor())))
							.map(extractorStream -> Tuple.of(getFileAttributes(_path), extractorStream))
							.filter(extractorStreamTuple -> extractorStreamTuple.left().isPresent())
							.map(extractorStreamTuple -> extractorStreamTuple.replaceLeft(extractorStreamTuple.left().get()))
							.map(extractorStreamTuple -> extractorStreamTuple.right()
																				.map(extractorFunction -> extractorFunction.replaceRight(extractorFunction.right().apply(extractorStreamTuple.left())))
																				.map(extractedValue -> extractedValue.replaceRight(extractedValue.right().orElse(null)))
																				.collect(Collectors.toMap(Tuple::left, Tuple::right)))
							.orElseGet(Collections::emptyMap);
	}

	@Override
	public void setAttribute(final Path _path,final String _attribute,final Object _value,final LinkOption... _options) throws IOException {
		throw new UnsupportedOperationException("Not supported yet."); 
	}
	
	protected void disconnectFileSystem(final S3FileSystem _fileSystem){
		this.fileSystems.remove(_fileSystem.getKey());
	}
}
