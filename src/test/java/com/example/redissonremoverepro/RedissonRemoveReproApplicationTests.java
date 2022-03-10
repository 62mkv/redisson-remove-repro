package com.example.redissonremoverepro;

import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonLocalCachedMap;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;

@Testcontainers
class RedissonRemoveReproApplicationTests {

    @Container
    public GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);
    private RedissonLocalCachedMap<String, Value> cache;

    @BeforeEach
    void initializeCache() {

        if (cache == null) {
            String address = redis.getHost();
            Integer port = redis.getFirstMappedPort();
            Config config = new Config();
            config.useSingleServer().setAddress("redis://" + address + ":" + port);
            RedissonClient client = Redisson.create(config);

            cache = (RedissonLocalCachedMap<String, Value>) client.getLocalCachedMap("test-cache", LocalCachedMapOptions.<String, Value>defaults()
                    .storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE_REDIS)
                    .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.LOAD)
                    .syncStrategy(LocalCachedMapOptions.SyncStrategy.UPDATE));
            cache.preloadCache();
        }
    }

    @Test
    void testRemoveEachAsync() {

        var value1 = createValue("value1", 1);
        var value2 = createValue("value2", 2);
        var value3 = createValue("value3", 3);
        var value4 = createValue("value4", 4);

        var values = Arrays.array(value1, value2, value3, value4);

        Flux.fromArray(values)
                .flatMap(v -> set(v.getUuid().toString(), v))
                .blockLast();

        var flow = removeMatching(v -> v.getAttribute() % 2 == 0)
                .thenMany(Flux.fromArray(values))
                .flatMap(v -> get(v.getUuid().toString()));

        StepVerifier.create(flow)
                .expectNext(value1, value3)
                .verifyComplete();

    }

    private Value createValue(String name, int i) {
        return new Value(UUID.randomUUID(), name, i);
    }

    private Mono<Void> set(String key, Value value) {
        return Mono.fromFuture(() -> cache.putAsync(key, value).toCompletableFuture())
                .doOnNext(v -> System.out.println("value added with key " + key + " and value " + value))
                .then();
    }


    private Mono<Value> get(String key) {
        return Mono.fromFuture(() -> cache.getAsync(key).toCompletableFuture());
    }

    private Mono<Void> removeMatching(Predicate<Value> predicate) {
        return Mono.fromFuture(() -> cache.readAllEntrySetAsync().toCompletableFuture())
                .flatMapMany(Flux::fromIterable)
                .filter(Objects::nonNull)
                .doOnNext(v -> System.out.println("Testing value:" + v))
                .filter(entry -> predicate.test(entry.getValue()))
                .doOnNext(v -> System.out.println("Removing value:" + v))
                .flatMap(entry -> Mono.fromFuture(cache.removeAsync(entry.getKey(), entry.getValue()).toCompletableFuture()))
                .then();
    }


    static class Value implements Serializable {

        private UUID uuid;
        private String name;
        private Integer attribute;

        public Value(UUID uuid, String name, Integer attribute) {
            this.uuid = uuid;
            this.name = name;
            this.attribute = attribute;
        }

        public UUID getUuid() {
            return uuid;
        }

        public void setUuid(UUID uuid) {
            this.uuid = uuid;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAttribute() {
            return attribute;
        }

        public void setAttribute(Integer attribute) {
            this.attribute = attribute;
        }

        @Override
        public String toString() {
            return "Value{" +
                    "uuid=" + uuid +
                    ", name='" + name + '\'' +
                    ", attribute=" + attribute +
                    '}';
        }
    }
}
