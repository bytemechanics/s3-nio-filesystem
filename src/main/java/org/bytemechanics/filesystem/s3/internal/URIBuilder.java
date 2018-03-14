/*
 * Copyright 2018 Byte Mechanics.
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import org.bytemechanics.filesystem.s3.internal.copy.commons.functional.LambdaUnchecker;

/**
 *
 * @author E103880
 */
public class URIBuilder {
	
	private String scheme;
	private String host;
	private int port;
	private String path;
	
	private URI generate() throws URISyntaxException{
		
		URI reply=null;
		
		if((this.host!=null)&&(!this.host.isEmpty())){
			if(this.port>0){
				reply=new URI(this.scheme,null,this.host,this.port,this.path,null,null);
			}else{
				reply=new URI(this.scheme,this.host,this.path,null);
			}
		}
		
		return reply;
	}

	public URIBuilder uri(URI _uri) {
		if(_uri!=null){
			this.scheme=_uri.getScheme();
			this.host=_uri.getHost();
			this.port=_uri.getPort();
			this.path=_uri.getPath();
		}
		return this;
	}
	public URIBuilder scheme(String _scheme) {
		this.scheme = _scheme;
		return this;
	}
	public URIBuilder host(String _host) {
		this.host = _host;
		return this;
	}
	public URIBuilder port(int _port) {
		this.port = _port;
		return this;
	}
	public URIBuilder path(String _path) {
		this.path = _path;
		return this;
	}
	
	public Optional<URI> tryBuild(){
		return Optional.ofNullable(LambdaUnchecker.uncheckedGet(this::generate));
	}
	public URI build(){
		return tryBuild().orElse(null);
	}
	
	public static final URIBuilder builder(){
		return new URIBuilder();
	}
	public static final URI build(final URI _uri,final String _scheme){
		return URIBuilder.builder()	
								.uri(_uri)
								.scheme(_scheme)
							.build();
	}
	public static final Optional<URI> tryBuild(final URI _uri){
		return URIBuilder.builder()	
								.uri(_uri)
							.tryBuild();
	}
	public static final URI build(final URI _uri){
		return URIBuilder.tryBuild(_uri)	
								.orElse(null);
	}
}
