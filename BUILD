# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "test_redispp",
    srcs = glob(["src/sw/redis++/*.cpp", "src/sw/redis++/*.h", "src/sw/redis++/*.hpp", "test/test_redis.cpp"]),
    copts = ["-Isrc"],
    deps = [
        "@cpp3rd_lib//hiredis:hiredis",
    ],
)