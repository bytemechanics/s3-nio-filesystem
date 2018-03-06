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

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.bytemechanics.filesystem.s3.internal.copy.commons.string.SimpleFormat;

/**
 * @author afarre
 * @since 0.1.0
 */
public enum S3FileAttributesExtractor {

	LASTMODIFIEDTIME("basic","lastModifiedTime",attributeView -> attributeView.lastModifiedTime()),
	LASTACCESSTIME("basic","lastAccessTime",attributeView -> attributeView.lastAccessTime()),
	CREATIONTIME("basic","creationTime",attributeView -> attributeView.creationTime()),
	ISREGULARFILE("basic","isRegularFile",attributeView -> attributeView.isRegularFile()),
	ISDIRECTORY("basic","isDirectory",attributeView -> attributeView.isDirectory()),
	ISSYMBOLICLINK("basic","isSymbolicLink",attributeView -> attributeView.isSymbolicLink()),
	ISOTHER("basic","isOther",attributeView -> attributeView.isOther()),
	SIZE("basic","size",attributeView -> attributeView.size()),
	FILEKEY("basic","fileKey",attributeView -> attributeView.fileKey()),
	OWNER("posix","owner",attributeView -> attributeView.owner()),
	GROUP("posix","group",attributeView -> attributeView.group()),
	PERMISSIONS("posix","permissions",attributeView -> attributeView.permissions()),	
	;
		
	private final String view;
	private final String attribute;
	private final Function<S3FileAttributeView,Object> extractor;
	
	S3FileAttributesExtractor(final String _view,final String _key,final Function<S3FileAttributeView,Object> _extractor){
		this.view=_view;
		this.attribute=_key;
		this.extractor=_extractor;
	}
	
	public String getAttribute(){
		return this.attribute;
	}
	public String getView() {
		return view;
	}
	public Function<S3FileAttributeView, Object> getExtractor() {
		return extractor;
	}
	public Function<S3FileAttributeView, Optional<Object>> getOptionalExtractor() {
		return (S3FileAttributeView attributeView) -> 
					{
						Optional<Object> reply;

						try{
							reply=Optional.ofNullable(this.extractor.apply(attributeView));
						}catch(Exception e){
							reply=Optional.empty();
						}

						return reply;
					};
	}
	
	public static final Stream<S3FileAttributesExtractor> getAttributes(final String _viewFilter){
		return Stream.of(S3FileAttributesExtractor.values())
					.filter(attributeExtractor -> attributeExtractor.view.equals(_viewFilter));
	}
	public static final S3FileAttributesExtractor valueOf(final String _view,final String _attribute){
		return Stream.of(S3FileAttributesExtractor.values())
					.filter(attributeExtractor -> attributeExtractor.view.equals(_view))
					.filter(attributeExtractor -> attributeExtractor.attribute.equals(_attribute))
					.findAny()
						.orElseThrow(() -> new IllegalArgumentException(SimpleFormat.format("No attribute {} found at {} view",_attribute,_view)));
	}
}
