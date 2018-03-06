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

import org.bytemechanics.filesystem.s3.internal.copy.commons.string.SimpleFormat;

/**
 * @author afarre
 * @since 0.1.0
 * @param <A>
 * @param <B>
 */
public class Tuple<A,B> {

	private final A firstValue;
	private final B secondValue;

	public Tuple(final A _first,final B _second) {
		this.firstValue = _first;
		this.secondValue = _second;
	}

	public A left() {
		return firstValue;
	}
	public B right() {
		return secondValue;
	}
	
	public <C> Tuple<C,B> replaceLeft(C _newValue){
		return replace(_newValue,secondValue);
	}
	public <C> Tuple<A,C> replaceRight(C _newValue){
		return replace(firstValue,_newValue);
	}
	public <C,D> Tuple<C,D> replace(C _left,D _right){
		return new Tuple<>(_left,_right);
	}
	
	public static final <LEFT,RIGHT> Tuple<LEFT,RIGHT> of(final LEFT _left,final RIGHT _right){
		return new Tuple<>(_left,_right);
	}

	@Override
	public String toString() {
		return SimpleFormat.format("Tuple[firstValue={}, secondValue={}]", firstValue, secondValue);
	}
}
