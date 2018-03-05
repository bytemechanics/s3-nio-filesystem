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

import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Map;

/**
 * @author afarre
 * @since 0.1.0
 */
public class S3FileStoreAttributeView implements FileStoreAttributeView{

	public static final String ATTRIBUTE_VIEW_NAME = "S3FileStoreAttributeView";

	private final Map<S3FileStoreAttribute,Object> attributes;


    public S3FileStoreAttributeView(final Map<S3FileStoreAttribute,Object> _attributes) {
		this.attributes=_attributes;
    }

    @Override
    public String name() {
        return ATTRIBUTE_VIEW_NAME;
    }

    public Object getAttribute(final String _attribute) {
        return getAttribute(S3FileStoreAttribute.valueOf(_attribute));
    }

    private Object getAttribute(final S3FileStoreAttribute _attribute) {
        return this.attributes.get(_attribute);
    }
}
