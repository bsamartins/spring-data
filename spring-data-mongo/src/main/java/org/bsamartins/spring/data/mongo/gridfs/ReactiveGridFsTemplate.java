package org.bsamartins.spring.data.mongo.gridfs;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.AntPathExtension;
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
	 * @param bucket optional parameter
	 */
	public ReactiveGridFsTemplate(ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory, MongoConverter converter, String bucket) {

		Assert.notNull(reactiveMongoDatabaseFactory, "ReactiveMongoDatabaseFactory must not be null!");
		Assert.notNull(converter, "MongoConverter must not be null!");

		this.reactiveMongoDatabaseFactory = reactiveMongoDatabaseFactory;
		this.converter = converter;
		this.bucket = bucket;

		this.queryMapper = new QueryMapper(converter);
	}

	@Override
	public Mono<ObjectId> store(AsyncInputStream content, String filename) {
		return store(content, filename, (Object) null);
	}

	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable Object metadata) {
		return store(content, null, metadata);
	}

	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable Document metadata) {
		return store(content, null, metadata);
	}

	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType) {
		return store(content, filename, contentType, (Object) null);
	}

	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable Object metadata) {
		return store(content, filename, null, metadata);
	}

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
			mData.put(GridsFsHeaderConstants.CONTENT_TYPE_FIELD, contentType);
		}

		if (metadata != null) {
			mData.putAll(metadata);
		}

		options.metadata(mData);

		return Mono.from(getGridFs().uploadFromStream(filename, content, options))
				.doOnNext(id -> LOGGER.info("Saved file `{}` with id `{}`", filename, id));
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
	public Mono<ReactiveGridFsResource> getResource(String location) {
		return findOne(query(whereFilename().is(location)))
				.map(file -> new ReactiveGridFsResource(file, getGridFs().openDownloadStream(file.getFilename())));
	}

	@Override
	public Flux<ReactiveGridFsResource> getResources(String locationPattern) {

		if (!StringUtils.hasText(locationPattern)) {
			return Flux.empty();
		}

		AntPathExtension path = new AntPathExtension(locationPattern);

		if (path.isPattern()) {
			return find(query(whereFilename().regex(path.toRegex())))
					.map(file -> new ReactiveGridFsResource(file, getGridFs().openDownloadStream(file.getFilename())));
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
