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
	
	LASTMODIFIEDTIME(S3FileAttributeType.BASIC,"lastModifiedTime",attributeView -> attributeView.lastModifiedTime()),
	LASTACCESSTIME(S3FileAttributeType.BASIC,"lastAccessTime",attributeView -> attributeView.lastAccessTime()),
	CREATIONTIME(S3FileAttributeType.BASIC,"creationTime",attributeView -> attributeView.creationTime()),
	ISREGULARFILE(S3FileAttributeType.BASIC,"isRegularFile",attributeView -> attributeView.isRegularFile()),
	ISDIRECTORY(S3FileAttributeType.BASIC,"isDirectory",attributeView -> attributeView.isDirectory()),
	ISSYMBOLICLINK(S3FileAttributeType.BASIC,"isSymbolicLink",attributeView -> attributeView.isSymbolicLink()),
	ISOTHER(S3FileAttributeType.BASIC,"isOther",attributeView -> attributeView.isOther()),
	SIZE(S3FileAttributeType.BASIC,"size",attributeView -> attributeView.size()),
	FILEKEY(S3FileAttributeType.BASIC,"fileKey",attributeView -> attributeView.fileKey()),
	OWNER(S3FileAttributeType.POSIX,"owner",attributeView -> attributeView.owner()),
	GROUP(S3FileAttributeType.POSIX,"group",attributeView -> attributeView.group()),
	PERMISSIONS(S3FileAttributeType.POSIX,"permissions",attributeView -> attributeView.permissions()),	
	;
		
	private final String view;
	private final String attribute;
	private final Function<S3FileAttributeView,Object> extractor;
	
	S3FileAttributesExtractor(final S3FileAttributeType _type,final String _key,final Function<S3FileAttributeView,Object> _extractor){
		this.view=_type.getView();
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
