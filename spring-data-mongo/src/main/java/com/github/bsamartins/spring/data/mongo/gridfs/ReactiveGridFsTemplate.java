/*
 * Copyright 2011-2017 the original author or authors.
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
package com.github.bsamartins.spring.data.mongo.gridfs;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.AntPathAdapter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.whereFilename;

/**
 * {@link ReactiveGridFsOperations} implementation to store content into MongoDB GridFS.
 *
 * @author Bernardo Martins
 */
public class ReactiveGridFsTemplate implements ReactiveGridFsOperations {

	private Logger LOGGER = LoggerFactory.getLogger(ReactiveGridFsTemplate.class);

	private final ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory;

	private final String bucket;
	private final MongoConverter converter;
	private final QueryMapper queryMapper;

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link ReactiveMongoDatabaseFactory} and {@link MongoConverter}.
	 * 
	 * @param reactiveMongoDatabaseFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public ReactiveGridFsTemplate(ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory, MongoConverter converter) {
		this(reactiveMongoDatabaseFactory, converter, null);
	}

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link ReactiveMongoDatabaseFactory} and {@link MongoConverter}.
	 * 
	 * @param reactiveMongoDatabaseFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param bucket
	 */
	public ReactiveGridFsTemplate(ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory, MongoConverter converter, String bucket) {

		Assert.notNull(reactiveMongoDatabaseFactory, "ReactiveMongoDatabaseFactory must not be null!");
		Assert.notNull(converter, "MongoConverter must not be null!");

		this.reactiveMongoDatabaseFactory = reactiveMongoDatabaseFactory;
		this.converter = converter;
		this.bucket = bucket;

		this.queryMapper = new QueryMapper(converter);
	}

	/*
	 * (non-Javadoc)
	 * @see com.github.bsamartins.spring.data.mongo.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, java.lang.String)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, String filename) {
		return store(content, filename, (Object) null);
	}

	/*
	 * (non-Javadoc)
	 * @see com.github.bsamartins.spring.data.mongo.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, java.lang.Object)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable Object metadata) {
		return store(content, null, metadata);
	}

	/*
	 * (non-Javadoc)
	 * @see com.github.bsamartins.spring.data.mongo.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, com.mongodb.Document)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable Document metadata) {
		return store(content, null, metadata);
	}

	/*
	 * (non-Javadoc)
	 * @see com.github.bsamartins.spring.data.mongo.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, java.lang.String, java.lang.String)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType) {
		return store(content, filename, contentType, (Object) null);
	}

	/*
	 * (non-Javadoc)
	 * @see com.github.bsamartins.spring.data.mongo.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, java.lang.String, java.lang.Object)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable Object metadata) {
		return store(content, filename, null, metadata);
	}

	/*
	 * (non-Javadoc)
	 * @see com.github.bsamartins.spring.data.mongo.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, java.lang.String, java.lang.String, java.lang.Object)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType, @Nullable Object metadata) {

		Document document = null;

		if (metadata != null) {
			document = new Document();
			converter.write(metadata, document);
		}

		return store(content, filename, contentType, document);
	}

	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable Document metadata) {
		return this.store(content, filename, null, metadata);
	}

	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType, @Nullable Document metadata) {

		Assert.notNull(content, "InputStream must not be null!");

		GridFSUploadOptions options = new GridFSUploadOptions();

		Document mData = new Document();

		if (StringUtils.hasText(contentType)) {
			mData.put(GridsFSHeaderConstants.CONTENT_TYPE_FIELD, contentType);
		}

		if (metadata != null) {
			mData.putAll(metadata);
		}

		options.metadata(mData);

		return Mono.from(getGridFs().uploadFromStream(filename, content, options))
				.doOnNext(id -> LOGGER.info("Saved file with id: {}", id));
	}

	@Override
	public Flux<GridFSFile> find(Query query) {

		Assert.notNull(query, "Query must not be null!");

		Document queryObject = getMappedQuery(query.getQueryObject());
		Document sortObject = getMappedQuery(query.getSortObject());

		return Flux.from(getGridFs().find(queryObject).sort(sortObject));
	}

	@Override
	public Mono<GridFSFile> findOne(Query query) {
		return find(query).take(1).singleOrEmpty();
	}

	@Override
	public Mono<Void> delete(Query query) {
		return find(query).flatMap(x -> getGridFs().delete(((BsonObjectId) x.getId()).getValue()))
			.then();
	}

	@Override
	public Mono<GridFSDownloadStream> getResource(String location) {
		return findOne(query(whereFilename().is(location)))
				.map(file -> getGridFs().openDownloadStream(location));
	}

	@Override
	public Flux<GridFSDownloadStream> getResources(String locationPattern) {

		if (!StringUtils.hasText(locationPattern)) {
			return Flux.empty();
		}

		AntPathAdapter path = new AntPathAdapter(locationPattern);

		if (path.isPattern()) {
			return find(query(whereFilename().regex(path.toRegex())))
					.map(file -> getGridFs().openDownloadStream(file.getFilename()));
		}

		return getResource(locationPattern).flux();
	}

	private Document getMappedQuery(Document query) {
		return queryMapper.getMappedObject(query, Optional.empty());
	}

	private GridFSBucket getGridFs() {
		MongoDatabase db = reactiveMongoDatabaseFactory.getMongoDatabase();
		return bucket == null ? GridFSBuckets.create(db) : GridFSBuckets.create(db, bucket);
	}
}
