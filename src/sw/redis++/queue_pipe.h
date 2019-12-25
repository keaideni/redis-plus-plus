#ifndef QUEUE_PIPE_H
#define QUEUE_PIPE_H
#include <cassert>
#include <chrono>
#include <initializer_list>
#include <vector>
#include "connection.h"
#include "utils.h"
#include "reply.h"
#include "command.h"
#include "redis.h"
namespace sw {

namespace redis {

template <typename Impl>
class QueuedPipe {
public:
    QueuedPipe(QueuedPipe &&) = default;
    QueuedPipe& operator=(QueuedPipe &&) = default;

    // When it destructs, the underlying *Connection* will be closed,
    // and any command that has NOT been executed will be ignored.
    ~QueuedPipe() = default;

    Redis redis();

    template <typename Cmd, typename ...Args>
    auto command(Cmd cmd, Args &&...args)
        -> typename std::enable_if<!std::is_convertible<Cmd, StringView>::value,
                                    QueuedPipe&>::type;

    template <typename ...Args>
    QueuedPipe& command(const StringView &cmd_name, Args &&...args);

    template <typename Input>
    auto command(Input first, Input last)
        -> typename std::enable_if<IsIter<Input>::value, QueuedPipe&>::type;

    QueuedReplies exec();

    void discard();

    // CONNECTION commands.

    QueuedPipe& auth(const StringView &password) {
        return command(cmd::auth, password);
    }

    QueuedPipe& echo(const StringView &msg) {
        return command(cmd::echo, msg);
    }

    QueuedPipe& ping() {
        return command<void (*)(Connection &)>(cmd::ping);
    }

    QueuedPipe& ping(const StringView &msg) {
        return command<void (*)(Connection &, const StringView &)>(cmd::ping, msg);
    }

    // We DO NOT support the QUIT command. See *Redis::quit* doc for details.
    //
    // QueuedPipe& quit();

    QueuedPipe& select(long long idx) {
        return command(cmd::select, idx);
    }

    QueuedPipe& swapdb(long long idx1, long long idx2) {
        return command(cmd::swapdb, idx1, idx2);
    }

    // SERVER commands.

    QueuedPipe& bgrewriteaof() {
        return command(cmd::bgrewriteaof);
    }

    QueuedPipe& bgsave() {
        return command(cmd::bgsave);
    }

    QueuedPipe& dbsize() {
        return command(cmd::dbsize);
    }

    QueuedPipe& flushall(bool async = false) {
        return command(cmd::flushall, async);
    }

    QueuedPipe& flushdb(bool async = false) {
        return command(cmd::flushdb, async);
    }

    QueuedPipe& info() {
        return command<void (*)(Connection &)>(cmd::info);
    }

    QueuedPipe& info(const StringView &section) {
        return command<void (*)(Connection &, const StringView &)>(cmd::info, section);
    }

    QueuedPipe& lastsave() {
        return command(cmd::lastsave);
    }

    QueuedPipe& save() {
        return command(cmd::save);
    }

    // KEY commands.

    QueuedPipe& del(const StringView &key) {
        return command(cmd::del, key);
    }

    template <typename Input>
    QueuedPipe& del(Input first, Input last) {
        return command(cmd::del_range<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& del(std::initializer_list<T> il) {
        return del(il.begin(), il.end());
    }

    QueuedPipe& dump(const StringView &key) {
        return command(cmd::dump, key);
    }

    QueuedPipe& exists(const StringView &key) {
        return command(cmd::exists, key);
    }

    template <typename Input>
    QueuedPipe& exists(Input first, Input last) {
        return command(cmd::exists_range<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& exists(std::initializer_list<T> il) {
        return exists(il.begin(), il.end());
    }

    QueuedPipe& expire(const StringView &key, long long timeout) {
        return command(cmd::expire, key, timeout);
    }

    QueuedPipe& expire(const StringView &key,
                        const std::chrono::seconds &timeout) {
        return expire(key, timeout.count());
    }

    QueuedPipe& expireat(const StringView &key, long long timestamp) {
        return command(cmd::expireat, key, timestamp);
    }

    QueuedPipe& expireat(const StringView &key,
                            const std::chrono::time_point<std::chrono::system_clock,
                                                            std::chrono::seconds> &tp) {
        return expireat(key, tp.time_since_epoch().count());
    }

    QueuedPipe& keys(const StringView &pattern) {
        return command(cmd::keys, pattern);
    }

    QueuedPipe& move(const StringView &key, long long db) {
        return command(cmd::move, key, db);
    }

    QueuedPipe& persist(const StringView &key) {
        return command(cmd::persist, key);
    }

    QueuedPipe& pexpire(const StringView &key, long long timeout) {
        return command(cmd::pexpire, key, timeout);
    }

    QueuedPipe& pexpire(const StringView &key,
                            const std::chrono::milliseconds &timeout) {
        return pexpire(key, timeout.count());
    }

    QueuedPipe& pexpireat(const StringView &key, long long timestamp) {
        return command(cmd::pexpireat, key, timestamp);
    }

    QueuedPipe& pexpireat(const StringView &key,
                            const std::chrono::time_point<std::chrono::system_clock,
                                                            std::chrono::milliseconds> &tp) {
        return pexpireat(key, tp.time_since_epoch().count());
    }

    QueuedPipe& pttl(const StringView &key) {
        return command(cmd::pttl, key);
    }

    QueuedPipe& randomkey() {
        return command(cmd::randomkey);
    }

    QueuedPipe& rename(const StringView &key, const StringView &newkey) {
        return command(cmd::rename, key, newkey);
    }

    QueuedPipe& renamenx(const StringView &key, const StringView &newkey) {
        return command(cmd::renamenx, key, newkey);
    }

    QueuedPipe& restore(const StringView &key,
                                const StringView &val,
                                long long ttl,
                                bool replace = false) {
        return command(cmd::restore, key, val, ttl, replace);
    }

    QueuedPipe& restore(const StringView &key,
                            const StringView &val,
                            const std::chrono::milliseconds &ttl = std::chrono::milliseconds{0},
                            bool replace = false) {
        return restore(key, val, ttl.count(), replace);
    }

    // TODO: sort

    QueuedPipe& scan(long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::scan, cursor, pattern, count);
    }

    QueuedPipe& scan(long long cursor) {
        return scan(cursor, "*", 10);
    }

    QueuedPipe& scan(long long cursor,
                        const StringView &pattern) {
        return scan(cursor, pattern, 10);
    }

    QueuedPipe& scan(long long cursor,
                        long long count) {
        return scan(cursor, "*", count);
    }

    QueuedPipe& touch(const StringView &key) {
        return command(cmd::touch, key);
    }

    template <typename Input>
    QueuedPipe& touch(Input first, Input last) {
        return command(cmd::touch_range<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& touch(std::initializer_list<T> il) {
        return touch(il.begin(), il.end());
    }

    QueuedPipe& ttl(const StringView &key) {
        return command(cmd::ttl, key);
    }

    QueuedPipe& type(const StringView &key) {
        return command(cmd::type, key);
    }

    QueuedPipe& unlink(const StringView &key) {
        return command(cmd::unlink, key);
    }

    template <typename Input>
    QueuedPipe& unlink(Input first, Input last) {
        return command(cmd::unlink_range<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& unlink(std::initializer_list<T> il) {
        return unlink(il.begin(), il.end());
    }

    QueuedPipe& wait(long long numslaves, long long timeout) {
        return command(cmd::wait, numslaves, timeout);
    }

    QueuedPipe& wait(long long numslaves, const std::chrono::milliseconds &timeout) {
        return wait(numslaves, timeout.count());
    }

    // STRING commands.

    QueuedPipe& append(const StringView &key, const StringView &str) {
        return command(cmd::append, key, str);
    }

    QueuedPipe& bitcount(const StringView &key,
                            long long start = 0,
                            long long end = -1) {
        return command(cmd::bitcount, key, start, end);
    }

    QueuedPipe& bitop(BitOp op,
                        const StringView &destination,
                        const StringView &key) {
        return command(cmd::bitop, op, destination, key);
    }

    template <typename Input>
    QueuedPipe& bitop(BitOp op,
                        const StringView &destination,
                        Input first,
                        Input last) {
        return command(cmd::bitop_range<Input>, op, destination, first, last);
    }

    template <typename T>
    QueuedPipe& bitop(BitOp op,
                        const StringView &destination,
                        std::initializer_list<T> il) {
        return bitop(op, destination, il.begin(), il.end());
    }

    QueuedPipe& bitpos(const StringView &key,
                        long long bit,
                        long long start = 0,
                        long long end = -1) {
        return command(cmd::bitpos, key, bit, start, end);
    }

    QueuedPipe& decr(const StringView &key) {
        return command(cmd::decr, key);
    }

    QueuedPipe& decrby(const StringView &key, long long decrement) {
        return command(cmd::decrby, key, decrement);
    }

    QueuedPipe& get(const StringView &key) {
        return command(cmd::get, key);
    }

    QueuedPipe& getbit(const StringView &key, long long offset) {
        return command(cmd::getbit, key, offset);
    }

    QueuedPipe& getrange(const StringView &key, long long start, long long end) {
        return command(cmd::getrange, key, start, end);
    }

    QueuedPipe& getset(const StringView &key, const StringView &val) {
        return command(cmd::getset, key, val);
    }

    QueuedPipe& incr(const StringView &key) {
        return command(cmd::incr, key);
    }

    QueuedPipe& incrby(const StringView &key, long long increment) {
        return command(cmd::incrby, key, increment);
    }

    QueuedPipe& incrbyfloat(const StringView &key, double increment) {
        return command(cmd::incrbyfloat, key, increment);
    }

    template <typename Input>
    QueuedPipe& mget(Input first, Input last) {
        return command(cmd::mget<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& mget(std::initializer_list<T> il) {
        return mget(il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& mset(Input first, Input last) {
        return command(cmd::mset<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& mset(std::initializer_list<T> il) {
        return mset(il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& msetnx(Input first, Input last) {
        return command(cmd::msetnx<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& msetnx(std::initializer_list<T> il) {
        return msetnx(il.begin(), il.end());
    }

    QueuedPipe& psetex(const StringView &key,
                        long long ttl,
                        const StringView &val) {
        return command(cmd::psetex, key, ttl, val);
    }

    QueuedPipe& psetex(const StringView &key,
                        const std::chrono::milliseconds &ttl,
                        const StringView &val) {
        return psetex(key, ttl.count(), val);
    }

    QueuedPipe& set(const StringView &key,
                        const StringView &val,
                        const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
                        UpdateType type = UpdateType::ALWAYS) {
        _set_cmd_indexes.push_back(_cmd_num);

        return command(cmd::set, key, val, ttl.count(), type);
    }

    QueuedPipe& setex(const StringView &key,
                        long long ttl,
                        const StringView &val) {
        return command(cmd::setex, key, ttl, val);
    }

    QueuedPipe& setex(const StringView &key,
                        const std::chrono::seconds &ttl,
                        const StringView &val) {
        return setex(key, ttl.count(), val);
    }

    QueuedPipe& setnx(const StringView &key, const StringView &val) {
        return command(cmd::setnx, key, val);
    }

    QueuedPipe& setrange(const StringView &key,
                            long long offset,
                            const StringView &val) {
        return command(cmd::setrange, key, offset, val);
    }

    QueuedPipe& strlen(const StringView &key) {
        return command(cmd::strlen, key);
    }

    // LIST commands.

    QueuedPipe& blpop(const StringView &key, long long timeout) {
        return command(cmd::blpop, key, timeout);
    }

    QueuedPipe& blpop(const StringView &key,
                        const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return blpop(key, timeout.count());
    }

    template <typename Input>
    QueuedPipe& blpop(Input first, Input last, long long timeout) {
        return command(cmd::blpop_range<Input>, first, last, timeout);
    }

    template <typename T>
    QueuedPipe& blpop(std::initializer_list<T> il, long long timeout) {
        return blpop(il.begin(), il.end(), timeout);
    }

    template <typename Input>
    QueuedPipe& blpop(Input first,
                        Input last,
                        const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return blpop(first, last, timeout.count());
    }

    template <typename T>
    QueuedPipe& blpop(std::initializer_list<T> il,
                        const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return blpop(il.begin(), il.end(), timeout);
    }

    template <typename Input>
    QueuedPipe& brpop(Input first, Input last, long long timeout) {
        return command(cmd::brpop<Input>, first, last, timeout);
    }

    template <typename T>
    QueuedPipe& brpop(std::initializer_list<T> il, long long timeout) {
        return brpop(il.begin(), il.end(), timeout);
    }

    template <typename Input>
    QueuedPipe& brpop(Input first,
                        Input last,
                        const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return brpop(first, last, timeout.count());
    }

    template <typename T>
    QueuedPipe& brpop(std::initializer_list<T> il,
                        const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return brpop(il.begin(), il.end(), timeout);
    }

    QueuedPipe& brpoplpush(const StringView &source,
                            const StringView &destination,
                            long long timeout) {
        return command(cmd::brpoplpush, source, destination, timeout);
    }

    QueuedPipe& brpoplpush(const StringView &source,
                            const StringView &destination,
                            const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return brpoplpush(source, destination, timeout.count());
    }

    QueuedPipe& lindex(const StringView &key, long long index) {
        return command(cmd::lindex, key, index);
    }

    QueuedPipe& linsert(const StringView &key,
                            InsertPosition position,
                            const StringView &pivot,
                            const StringView &val) {
        return command(cmd::linsert, key, position, pivot, val);
    }

    QueuedPipe& llen(const StringView &key) {
        return command(cmd::llen, key);
    }

    QueuedPipe& lpop(const StringView &key) {
        return command(cmd::lpop, key);
    }

    QueuedPipe& lpush(const StringView &key, const StringView &val) {
        return command(cmd::lpush, key, val);
    }

    template <typename Input>
    QueuedPipe& lpush(const StringView &key, Input first, Input last) {
        return command(cmd::lpush_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& lpush(const StringView &key, std::initializer_list<T> il) {
        return lpush(key, il.begin(), il.end());
    }

    QueuedPipe& lpushx(const StringView &key, const StringView &val) {
        return command(cmd::lpushx, key, val);
    }

    QueuedPipe& lrange(const StringView &key,
                        long long start,
                        long long stop) {
        return command(cmd::lrange, key, start, stop);
    }

    QueuedPipe& lrem(const StringView &key, long long count, const StringView &val) {
        return command(cmd::lrem, key, count, val);
    }

    QueuedPipe& lset(const StringView &key, long long index, const StringView &val) {
        return command(cmd::lset, key, index, val);
    }

    QueuedPipe& ltrim(const StringView &key, long long start, long long stop) {
        return command(cmd::ltrim, key, start, stop);
    }

    QueuedPipe& rpop(const StringView &key) {
        return command(cmd::rpop, key);
    }

    QueuedPipe& rpoplpush(const StringView &source, const StringView &destination) {
        return command(cmd::rpoplpush, source, destination);
    }

    QueuedPipe& rpush(const StringView &key, const StringView &val) {
        return command(cmd::rpush, key, val);
    }

    template <typename Input>
    QueuedPipe& rpush(const StringView &key, Input first, Input last) {
        return command(cmd::rpush_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& rpush(const StringView &key, std::initializer_list<T> il) {
        return rpush(key, il.begin(), il.end());
    }

    QueuedPipe& rpushx(const StringView &key, const StringView &val) {
        return command(cmd::rpushx, key, val);
    }

    // HASH commands.

    QueuedPipe& hdel(const StringView &key, const StringView &field) {
        return command(cmd::hdel, key, field);
    }

    template <typename Input>
    QueuedPipe& hdel(const StringView &key, Input first, Input last) {
        return command(cmd::hdel_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& hdel(const StringView &key, std::initializer_list<T> il) {
        return hdel(key, il.begin(), il.end());
    }

    QueuedPipe& hexists(const StringView &key, const StringView &field) {
        return command(cmd::hexists, key, field);
    }

    QueuedPipe& hget(const StringView &key, const StringView &field) {
        return command(cmd::hget, key, field);
    }

    QueuedPipe& hgetall(const StringView &key) {
        return command(cmd::hgetall, key);
    }

    QueuedPipe& hincrby(const StringView &key,
                            const StringView &field,
                            long long increment) {
        return command(cmd::hincrby, key, field, increment);
    }

    QueuedPipe& hincrbyfloat(const StringView &key,
                                const StringView &field,
                                double increment) {
        return command(cmd::hincrbyfloat, key, field, increment);
    }

    QueuedPipe& hkeys(const StringView &key) {
        return command(cmd::hkeys, key);
    }

    QueuedPipe& hlen(const StringView &key) {
        return command(cmd::hlen, key);
    }

    template <typename Input>
    QueuedPipe& hmget(const StringView &key, Input first, Input last) {
        return command(cmd::hmget<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& hmget(const StringView &key, std::initializer_list<T> il) {
        return hmget(key, il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& hmset(const StringView &key, Input first, Input last) {
        return command(cmd::hmset<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& hmset(const StringView &key, std::initializer_list<T> il) {
        return hmset(key, il.begin(), il.end());
    }

    QueuedPipe& hscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::hscan, key, cursor, pattern, count);
    }

    QueuedPipe& hscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern) {
        return hscan(key, cursor, pattern, 10);
    }

    QueuedPipe& hscan(const StringView &key,
                        long long cursor,
                        long long count) {
        return hscan(key, cursor, "*", count);
    }

    QueuedPipe& hscan(const StringView &key,
                        long long cursor) {
        return hscan(key, cursor, "*", 10);
    }

    QueuedPipe& hset(const StringView &key, const StringView &field, const StringView &val) {
        return command(cmd::hset, key, field, val);
    }

    QueuedPipe& hset(const StringView &key, const std::pair<StringView, StringView> &item) {
        return hset(key, item.first, item.second);
    }

    QueuedPipe& hsetnx(const StringView &key, const StringView &field, const StringView &val) {
        return command(cmd::hsetnx, key, field, val);
    }

    QueuedPipe& hsetnx(const StringView &key, const std::pair<StringView, StringView> &item) {
        return hsetnx(key, item.first, item.second);
    }

    QueuedPipe& hstrlen(const StringView &key, const StringView &field) {
        return command(cmd::hstrlen, key, field);
    }

    QueuedPipe& hvals(const StringView &key) {
        return command(cmd::hvals, key);
    }

    // SET commands.

    QueuedPipe& sadd(const StringView &key, const StringView &member) {
        return command(cmd::sadd, key, member);
    }

    template <typename Input>
    QueuedPipe& sadd(const StringView &key, Input first, Input last) {
        return command(cmd::sadd_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& sadd(const StringView &key, std::initializer_list<T> il) {
        return sadd(key, il.begin(), il.end());
    }

    QueuedPipe& scard(const StringView &key) {
        return command(cmd::scard, key);
    }

    template <typename Input>
    QueuedPipe& sdiff(Input first, Input last) {
        return command(cmd::sdiff<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& sdiff(std::initializer_list<T> il) {
        return sdiff(il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& sdiffstore(const StringView &destination,
                            Input first,
                            Input last) {
        return command(cmd::sdiffstore<Input>, destination, first, last);
    }

    template <typename T>
    QueuedPipe& sdiffstore(const StringView &destination, std::initializer_list<T> il) {
        return sdiffstore(destination, il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& sinter(Input first, Input last) {
        return command(cmd::sinter<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& sinter(std::initializer_list<T> il) {
        return sinter(il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& sinterstore(const StringView &destination,
                                Input first,
                                Input last) {
        return command(cmd::sinterstore<Input>, destination, first, last);
    }

    template <typename T>
    QueuedPipe& sinterstore(const StringView &destination, std::initializer_list<T> il) {
        return sinterstore(destination, il.begin(), il.end());
    }

    QueuedPipe& sismember(const StringView &key, const StringView &member) {
        return command(cmd::sismember, key, member);
    }

    QueuedPipe& smembers(const StringView &key) {
        return command(cmd::smembers, key);
    }

    QueuedPipe& smove(const StringView &source,
                        const StringView &destination,
                        const StringView &member) {
        return command(cmd::smove, source, destination, member);
    }

    QueuedPipe& spop(const StringView &key) {
        return command(cmd::spop, key);
    }

    QueuedPipe& spop(const StringView &key, long long count) {
        return command(cmd::spop_range, key, count);
    }

    QueuedPipe& srandmember(const StringView &key) {
        return command(cmd::srandmember, key);
    }

    QueuedPipe& srandmember(const StringView &key, long long count) {
        return command(cmd::srandmember_range, key, count);
    }

    QueuedPipe& srem(const StringView &key, const StringView &member) {
        return command(cmd::srem, key, member);
    }

    template <typename Input>
    QueuedPipe& srem(const StringView &key, Input first, Input last) {
        return command(cmd::srem_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& srem(const StringView &key, std::initializer_list<T> il) {
        return srem(key, il.begin(), il.end());
    }

    QueuedPipe& sscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::sscan, key, cursor, pattern, count);
    }

    QueuedPipe& sscan(const StringView &key,
                    long long cursor,
                    const StringView &pattern) {
        return sscan(key, cursor, pattern, 10);
    }

    QueuedPipe& sscan(const StringView &key,
                        long long cursor,
                        long long count) {
        return sscan(key, cursor, "*", count);
    }

    QueuedPipe& sscan(const StringView &key,
                        long long cursor) {
        return sscan(key, cursor, "*", 10);
    }

    template <typename Input>
    QueuedPipe& sunion(Input first, Input last) {
        return command(cmd::sunion<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& sunion(std::initializer_list<T> il) {
        return sunion(il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& sunionstore(const StringView &destination, Input first, Input last) {
        return command(cmd::sunionstore<Input>, destination, first, last);
    }

    template <typename T>
    QueuedPipe& sunionstore(const StringView &destination, std::initializer_list<T> il) {
        return sunionstore(destination, il.begin(), il.end());
    }

    // SORTED SET commands.

    QueuedPipe& bzpopmax(const StringView &key, long long timeout) {
        return command(cmd::bzpopmax, key, timeout);
    }

    QueuedPipe& bzpopmax(const StringView &key,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return bzpopmax(key, timeout.count());
    }

    template <typename Input>
    QueuedPipe& bzpopmax(Input first, Input last, long long timeout) {
        return command(cmd::bzpopmax_range<Input>, first, last, timeout);
    }

    template <typename Input>
    QueuedPipe& bzpopmax(Input first,
                            Input last,
                            const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return bzpopmax(first, last, timeout.count());
    }

    template <typename T>
    QueuedPipe& bzpopmax(std::initializer_list<T> il, long long timeout) {
        return bzpopmax(il.begin(), il.end(), timeout);
    }

    template <typename T>
    QueuedPipe& bzpopmax(std::initializer_list<T> il,
                            const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return bzpopmax(il.begin(), il.end(), timeout);
    }

    QueuedPipe& bzpopmin(const StringView &key, long long timeout) {
        return command(cmd::bzpopmin, key, timeout);
    }

    QueuedPipe& bzpopmin(const StringView &key,
                            const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return bzpopmin(key, timeout.count());
    }

    template <typename Input>
    QueuedPipe& bzpopmin(Input first, Input last, long long timeout) {
        return command(cmd::bzpopmin_range<Input>, first, last, timeout);
    }

    template <typename Input>
    QueuedPipe& bzpopmin(Input first,
                            Input last,
                            const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return bzpopmin(first, last, timeout.count());
    }

    template <typename T>
    QueuedPipe& bzpopmin(std::initializer_list<T> il, long long timeout) {
        return bzpopmin(il.begin(), il.end(), timeout);
    }

    template <typename T>
    QueuedPipe& bzpopmin(std::initializer_list<T> il,
                            const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return bzpopmin(il.begin(), il.end(), timeout);
    }

    // We don't support the INCR option, since you can always use ZINCRBY instead.
    QueuedPipe& zadd(const StringView &key,
                        const StringView &member,
                        double score,
                        UpdateType type = UpdateType::ALWAYS,
                        bool changed = false) {
        return command(cmd::zadd, key, member, score, type, changed);
    }

    template <typename Input>
    QueuedPipe& zadd(const StringView &key,
                        Input first,
                        Input last,
                        UpdateType type = UpdateType::ALWAYS,
                        bool changed = false) {
        return command(cmd::zadd_range<Input>, key, first, last, type, changed);
    }

    QueuedPipe& zcard(const StringView &key) {
        return command(cmd::zcard, key);
    }

    template <typename Interval>
    QueuedPipe& zcount(const StringView &key, const Interval &interval) {
        return command(cmd::zcount<Interval>, key, interval);
    }

    QueuedPipe& zincrby(const StringView &key, double increment, const StringView &member) {
        return command(cmd::zincrby, key, increment, member);
    }

    template <typename Input>
    QueuedPipe& zinterstore(const StringView &destination,
                                Input first,
                                Input last,
                                Aggregation type = Aggregation::SUM) {
        return command(cmd::zinterstore<Input>, destination, first, last, type);
    }

    template <typename T>
    QueuedPipe& zinterstore(const StringView &destination,
                                std::initializer_list<T> il,
                                Aggregation type = Aggregation::SUM) {
        return zinterstore(destination, il.begin(), il.end(), type);
    }

    template <typename Interval>
    QueuedPipe& zlexcount(const StringView &key, const Interval &interval) {
        return command(cmd::zlexcount<Interval>, key, interval);
    }

    QueuedPipe& zpopmax(const StringView &key) {
        return command(cmd::zpopmax, key, 1);
    }

    QueuedPipe& zpopmax(const StringView &key, long long count) {
        return command(cmd::zpopmax, key, count);
    }

    QueuedPipe& zpopmin(const StringView &key) {
        return command(cmd::zpopmin, key, 1);
    }

    QueuedPipe& zpopmin(const StringView &key, long long count) {
        return command(cmd::zpopmin, key, count);
    }

    // NOTE: *QueuedPipe::zrange*'s parameters are different from *Redis::zrange*.
    // *Redis::zrange* is overloaded by the output iterator, however, there's no such
    // iterator in *QueuedPipe::zrange*. So we have to use an extra parameter: *with_scores*,
    // to decide whether we should send *WITHSCORES* option to Redis. This also applies to
    // other commands with the *WITHSCORES* option, e.g. *ZRANGEBYSCORE*, *ZREVRANGE*,
    // *ZREVRANGEBYSCORE*.
    QueuedPipe& zrange(const StringView &key,
                        long long start,
                        long long stop,
                        bool with_scores = false) {
        return command(cmd::zrange, key, start, stop, with_scores);
    }

    template <typename Interval>
    QueuedPipe& zrangebylex(const StringView &key,
                                const Interval &interval,
                                const LimitOptions &opts) {
        return command(cmd::zrangebylex<Interval>, key, interval, opts);
    }

    template <typename Interval>
    QueuedPipe& zrangebylex(const StringView &key, const Interval &interval) {
        return zrangebylex(key, interval, {});
    }

    // See comments on *ZRANGE*.
    template <typename Interval>
    QueuedPipe& zrangebyscore(const StringView &key,
                                const Interval &interval,
                                const LimitOptions &opts,
                                bool with_scores = false) {
        return command(cmd::zrangebyscore<Interval>, key, interval, opts, with_scores);
    }

    // See comments on *ZRANGE*.
    template <typename Interval>
    QueuedPipe& zrangebyscore(const StringView &key,
                                const Interval &interval,
                                bool with_scores = false) {
        return zrangebyscore(key, interval, {}, with_scores);
    }

    QueuedPipe& zrank(const StringView &key, const StringView &member) {
        return command(cmd::zrank, key, member);
    }

    QueuedPipe& zrem(const StringView &key, const StringView &member) {
        return command(cmd::zrem, key, member);
    }

    template <typename Input>
    QueuedPipe& zrem(const StringView &key, Input first, Input last) {
        return command(cmd::zrem_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& zrem(const StringView &key, std::initializer_list<T> il) {
        return zrem(key, il.begin(), il.end());
    }

    template <typename Interval>
    QueuedPipe& zremrangebylex(const StringView &key, const Interval &interval) {
        return command(cmd::zremrangebylex<Interval>, key, interval);
    }

    QueuedPipe& zremrangebyrank(const StringView &key, long long start, long long stop) {
        return command(cmd::zremrangebyrank, key, start, stop);
    }

    template <typename Interval>
    QueuedPipe& zremrangebyscore(const StringView &key, const Interval &interval) {
        return command(cmd::zremrangebyscore<Interval>, key, interval);
    }

    // See comments on *ZRANGE*.
    QueuedPipe& zrevrange(const StringView &key,
                            long long start,
                            long long stop,
                            bool with_scores = false) {
        return command(cmd::zrevrange, key, start, stop, with_scores);
    }

    template <typename Interval>
    QueuedPipe& zrevrangebylex(const StringView &key,
                                const Interval &interval,
                                const LimitOptions &opts) {
        return command(cmd::zrevrangebylex<Interval>, key, interval, opts);
    }

    template <typename Interval>
    QueuedPipe& zrevrangebylex(const StringView &key, const Interval &interval) {
        return zrevrangebylex(key, interval, {});
    }

    // See comments on *ZRANGE*.
    template <typename Interval>
    QueuedPipe& zrevrangebyscore(const StringView &key,
                                    const Interval &interval,
                                    const LimitOptions &opts,
                                    bool with_scores = false) {
        return command(cmd::zrevrangebyscore<Interval>, key, interval, opts, with_scores);
    }

    // See comments on *ZRANGE*.
    template <typename Interval>
    QueuedPipe& zrevrangebyscore(const StringView &key,
                                    const Interval &interval,
                                    bool with_scores = false) {
        return zrevrangebyscore(key, interval, {}, with_scores);
    }

    QueuedPipe& zrevrank(const StringView &key, const StringView &member) {
        return command(cmd::zrevrank, key, member);
    }

    QueuedPipe& zscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::zscan, key, cursor, pattern, count);
    }

    QueuedPipe& zscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern) {
        return zscan(key, cursor, pattern, 10);
    }

    QueuedPipe& zscan(const StringView &key,
                        long long cursor,
                        long long count) {
        return zscan(key, cursor, "*", count);
    }

    QueuedPipe& zscan(const StringView &key,
                        long long cursor) {
        return zscan(key, cursor, "*", 10);
    }

    QueuedPipe& zscore(const StringView &key, const StringView &member) {
        return command(cmd::zscore, key, member);
    }

    template <typename Input>
    QueuedPipe& zunionstore(const StringView &destination,
                                Input first,
                                Input last,
                                Aggregation type = Aggregation::SUM) {
        return command(cmd::zunionstore<Input>, destination, first, last, type);
    }

    template <typename T>
    QueuedPipe& zunionstore(const StringView &destination,
                                std::initializer_list<T> il,
                                Aggregation type = Aggregation::SUM) {
        return zunionstore(destination, il.begin(), il.end(), type);
    }

    // HYPERLOGLOG commands.

    QueuedPipe& pfadd(const StringView &key, const StringView &element) {
        return command(cmd::pfadd, key, element);
    }

    template <typename Input>
    QueuedPipe& pfadd(const StringView &key, Input first, Input last) {
        return command(cmd::pfadd_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& pfadd(const StringView &key, std::initializer_list<T> il) {
        return pfadd(key, il.begin(), il.end());
    }

    QueuedPipe& pfcount(const StringView &key) {
        return command(cmd::pfcount, key);
    }

    template <typename Input>
    QueuedPipe& pfcount(Input first, Input last) {
        return command(cmd::pfcount_range<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& pfcount(std::initializer_list<T> il) {
        return pfcount(il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& pfmerge(const StringView &destination, Input first, Input last) {
        return command(cmd::pfmerge<Input>, destination, first, last);
    }

    template <typename T>
    QueuedPipe& pfmerge(const StringView &destination, std::initializer_list<T> il) {
        return pfmerge(destination, il.begin(), il.end());
    }

    // GEO commands.

    QueuedPipe& geoadd(const StringView &key,
                        const std::tuple<StringView, double, double> &member) {
        return command(cmd::geoadd, key, member);
    }

    template <typename Input>
    QueuedPipe& geoadd(const StringView &key,
                        Input first,
                        Input last) {
        return command(cmd::geoadd_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& geoadd(const StringView &key, std::initializer_list<T> il) {
        return geoadd(key, il.begin(), il.end());
    }

    QueuedPipe& geodist(const StringView &key,
                            const StringView &member1,
                            const StringView &member2,
                            GeoUnit unit = GeoUnit::M) {
        return command(cmd::geodist, key, member1, member2, unit);
    }

    template <typename Input>
    QueuedPipe& geohash(const StringView &key, Input first, Input last) {
        return command(cmd::geohash_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& geohash(const StringView &key, std::initializer_list<T> il) {
        return geohash(key, il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& geopos(const StringView &key, Input first, Input last) {
        return command(cmd::geopos_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& geopos(const StringView &key, std::initializer_list<T> il) {
        return geopos(key, il.begin(), il.end());
    }

    // TODO:
    // 1. since we have different overloads for georadius and georadius-store,
    //    we might use the GEORADIUS_RO command in the future.
    // 2. there're too many parameters for this method, we might refactor it.
    QueuedPipe& georadius(const StringView &key,
                            const std::pair<double, double> &loc,
                            double radius,
                            GeoUnit unit,
                            const StringView &destination,
                            bool store_dist,
                            long long count) {
        _georadius_cmd_indexes.push_back(_cmd_num);

        return command(cmd::georadius_store,
                        key,
                        loc,
                        radius,
                        unit,
                        destination,
                        store_dist,
                        count);
    }

    // NOTE: *QueuedPipe::georadius*'s parameters are different from *Redis::georadius*.
    // *Redis::georadius* is overloaded by the output iterator, however, there's no such
    // iterator in *QueuedPipe::georadius*. So we have to use extra parameters to decide
    // whether we should send options to Redis. This also applies to *GEORADIUSBYMEMBER*.
    QueuedPipe& georadius(const StringView &key,
                            const std::pair<double, double> &loc,
                            double radius,
                            GeoUnit unit,
                            long long count,
                            bool asc,
                            bool with_coord,
                            bool with_dist,
                            bool with_hash) {
        return command(cmd::georadius,
                        key,
                        loc,
                        radius,
                        unit,
                        count,
                        asc,
                        with_coord,
                        with_dist,
                        with_hash);
    }

    QueuedPipe& georadiusbymember(const StringView &key,
                                    const StringView &member,
                                    double radius,
                                    GeoUnit unit,
                                    const StringView &destination,
                                    bool store_dist,
                                    long long count) {
        _georadius_cmd_indexes.push_back(_cmd_num);

        return command(cmd::georadiusbymember,
                        key,
                        member,
                        radius,
                        unit,
                        destination,
                        store_dist,
                        count);
    }

    // See the comments on *GEORADIUS*.
    QueuedPipe& georadiusbymember(const StringView &key,
                                    const StringView &member,
                                    double radius,
                                    GeoUnit unit,
                                    long long count,
                                    bool asc,
                                    bool with_coord,
                                    bool with_dist,
                                    bool with_hash) {
        return command(cmd::georadiusbymember,
                        key,
                        member,
                        radius,
                        unit,
                        count,
                        asc,
                        with_coord,
                        with_dist,
                        with_hash);
    }

    // SCRIPTING commands.

    QueuedPipe& eval(const StringView &script,
                        std::initializer_list<StringView> keys,
                        std::initializer_list<StringView> args) {
        return command(cmd::eval, script, keys, args);
    }

    QueuedPipe& evalsha(const StringView &script,
                            std::initializer_list<StringView> keys,
                            std::initializer_list<StringView> args) {
        return command(cmd::evalsha, script, keys, args);
    }

    template <typename Input>
    QueuedPipe& script_exists(Input first, Input last) {
        return command(cmd::script_exists_range<Input>, first, last);
    }

    template <typename T>
    QueuedPipe& script_exists(std::initializer_list<T> il) {
        return script_exists(il.begin(), il.end());
    }

    QueuedPipe& script_flush() {
        return command(cmd::script_flush);
    }

    QueuedPipe& script_kill() {
        return command(cmd::script_kill);
    }

    QueuedPipe& script_load(const StringView &script) {
        return command(cmd::script_load, script);
    }

    // PUBSUB commands.

    QueuedPipe& publish(const StringView &channel, const StringView &message) {
        return command(cmd::publish, channel, message);
    }

    // Stream commands.

    QueuedPipe& xack(const StringView &key, const StringView &group, const StringView &id) {
        return command(cmd::xack, key, group, id);
    }

    template <typename Input>
    QueuedPipe& xack(const StringView &key, const StringView &group, Input first, Input last) {
        return command(cmd::xack_range<Input>, key, group, first, last);
    }

    template <typename T>
    QueuedPipe& xack(const StringView &key, const StringView &group, std::initializer_list<T> il) {
        return xack(key, group, il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& xadd(const StringView &key, const StringView &id, Input first, Input last) {
        return command(cmd::xadd_range<Input>, key, id, first, last);
    }

    template <typename T>
    QueuedPipe& xadd(const StringView &key, const StringView &id, std::initializer_list<T> il) {
        return xadd(key, id, il.begin(), il.end());
    }

    template <typename Input>
    QueuedPipe& xadd(const StringView &key,
                        const StringView &id,
                        Input first,
                        Input last,
                        long long count,
                        bool approx = true) {
        return command(cmd::xadd_maxlen_range<Input>, key, id, first, last, count, approx);
    }

    template <typename T>
    QueuedPipe& xadd(const StringView &key,
                        const StringView &id,
                        std::initializer_list<T> il,
                        long long count,
                        bool approx = true) {
        return xadd(key, id, il.begin(), il.end(), count, approx);
    }

    QueuedPipe& xclaim(const StringView &key,
                        const StringView &group,
                        const StringView &consumer,
                        const std::chrono::milliseconds &min_idle_time,
                        const StringView &id) {
        return command(cmd::xclaim, key, group, consumer, min_idle_time.count(), id);
    }

    template <typename Input>
    QueuedPipe& xclaim(const StringView &key,
                const StringView &group,
                const StringView &consumer,
                const std::chrono::milliseconds &min_idle_time,
                Input first,
                Input last) {
        return command(cmd::xclaim_range<Input>,
                        key,
                        group,
                        consumer,
                        min_idle_time.count(),
                        first,
                        last);
    }

    template <typename T>
    QueuedPipe& xclaim(const StringView &key,
                const StringView &group,
                const StringView &consumer,
                const std::chrono::milliseconds &min_idle_time,
                std::initializer_list<T> il) {
        return xclaim(key, group, consumer, min_idle_time, il.begin(), il.end());
    }

    QueuedPipe& xdel(const StringView &key, const StringView &id) {
        return command(cmd::xdel, key, id);
    }

    template <typename Input>
    QueuedPipe& xdel(const StringView &key, Input first, Input last) {
        return command(cmd::xdel_range<Input>, key, first, last);
    }

    template <typename T>
    QueuedPipe& xdel(const StringView &key, std::initializer_list<T> il) {
        return xdel(key, il.begin(), il.end());
    }

    QueuedPipe& xgroup_create(const StringView &key,
                                const StringView &group,
                                const StringView &id,
                                bool mkstream = false) {
        return command(cmd::xgroup_create, key, group, id, mkstream);
    }

    QueuedPipe& xgroup_setid(const StringView &key,
                                const StringView &group,
                                const StringView &id) {
        return command(cmd::xgroup_setid, key, group, id);
    }

    QueuedPipe& xgroup_destroy(const StringView &key, const StringView &group) {
        return command(cmd::xgroup_destroy, key, group);
    }

    QueuedPipe& xgroup_delconsumer(const StringView &key,
                                    const StringView &group,
                                    const StringView &consumer) {
        return command(cmd::xgroup_delconsumer, key, group, consumer);
    }

    QueuedPipe& xlen(const StringView &key) {
        return command(cmd::xlen, key);
    }

    QueuedPipe& xpending(const StringView &key, const StringView &group) {
        return command(cmd::xpending, key, group);
    }

    QueuedPipe& xpending(const StringView &key,
                            const StringView &group,
                            const StringView &start,
                            const StringView &end,
                            long long count) {
        return command(cmd::xpending_detail, key, group, start, end, count);
    }

    QueuedPipe& xpending(const StringView &key,
                            const StringView &group,
                            const StringView &start,
                            const StringView &end,
                            long long count,
                            const StringView &consumer) {
        return command(cmd::xpending_per_consumer, key, group, start, end, count, consumer);
    }

    QueuedPipe& xrange(const StringView &key,
                        const StringView &start,
                        const StringView &end) {
        return command(cmd::xrange, key, start, end);
    }

    QueuedPipe& xrange(const StringView &key,
                        const StringView &start,
                        const StringView &end,
                        long long count) {
        return command(cmd::xrange, key, start, end, count);
    }

    QueuedPipe& xread(const StringView &key, const StringView &id, long long count) {
        return command(cmd::xread, key, id, count);
    }

    QueuedPipe& xread(const StringView &key, const StringView &id) {
        return xread(key, id, 0);
    }

    template <typename Input>
    auto xread(Input first, Input last, long long count)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return command(cmd::xread_range<Input>, first, last, count);
    }

    template <typename Input>
    auto xread(Input first, Input last)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return xread(first, last, 0);
    }

    QueuedPipe& xread(const StringView &key,
                        const StringView &id,
                        const std::chrono::milliseconds &timeout,
                        long long count) {
        return command(cmd::xread_block, key, id, timeout.count(), count);
    }

    QueuedPipe& xread(const StringView &key,
                        const StringView &id,
                        const std::chrono::milliseconds &timeout) {
        return xread(key, id, timeout, 0);
    }

    template <typename Input>
    auto xread(Input first,
                Input last,
                const std::chrono::milliseconds &timeout,
                long long count)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return command(cmd::xread_block_range<Input>, first, last, timeout.count(), count);
    }

    template <typename Input>
    auto xread(Input first,
                Input last,
                const std::chrono::milliseconds &timeout)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return xread(first, last, timeout, 0);
    }

    QueuedPipe& xreadgroup(const StringView &group,
                            const StringView &consumer,
                            const StringView &key,
                            const StringView &id,
                            long long count,
                            bool noack) {
        return command(cmd::xreadgroup, group, consumer, key, id, count, noack);
    }

    QueuedPipe& xreadgroup(const StringView &group,
                            const StringView &consumer,
                            const StringView &key,
                            const StringView &id,
                            long long count) {
        return xreadgroup(group, consumer, key, id, count, false);
    }

    template <typename Input>
    auto xreadgroup(const StringView &group,
                    const StringView &consumer,
                    Input first,
                    Input last,
                    long long count,
                    bool noack)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return command(cmd::xreadgroup_range<Input>, group, consumer, first, last, count, noack);
    }

    template <typename Input>
    auto xreadgroup(const StringView &group,
                    const StringView &consumer,
                    Input first,
                    Input last,
                    long long count)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return xreadgroup(group, consumer, first ,last, count, false);
    }

    template <typename Input>
    auto xreadgroup(const StringView &group,
                    const StringView &consumer,
                    Input first,
                    Input last)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return xreadgroup(group, consumer, first ,last, 0, false);
    }

    QueuedPipe& xreadgroup(const StringView &group,
                            const StringView &consumer,
                            const StringView &key,
                            const StringView &id,
                            const std::chrono::milliseconds &timeout,
                            long long count,
                            bool noack) {
        return command(cmd::xreadgroup_block,
                        group,
                        consumer,
                        key,
                        id,
                        timeout.count(),
                        count,
                        noack);
    }

    QueuedPipe& xreadgroup(const StringView &group,
                            const StringView &consumer,
                            const StringView &key,
                            const StringView &id,
                            const std::chrono::milliseconds &timeout,
                            long long count) {
        return xreadgroup(group, consumer, key, id, timeout, count, false);
    }

    QueuedPipe& xreadgroup(const StringView &group,
                            const StringView &consumer,
                            const StringView &key,
                            const StringView &id,
                            const std::chrono::milliseconds &timeout) {
        return xreadgroup(group, consumer, key, id, timeout, 0, false);
    }

    template <typename Input>
    auto xreadgroup(const StringView &group,
                    const StringView &consumer,
                    Input first,
                    Input last,
                    const std::chrono::milliseconds &timeout,
                    long long count,
                    bool noack)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return command(cmd::xreadgroup_block_range<Input>,
                        group,
                        consumer,
                        first,
                        last,
                        timeout.count(),
                        count,
                        noack);
    }

    template <typename Input>
    auto xreadgroup(const StringView &group,
                    const StringView &consumer,
                    Input first,
                    Input last,
                    const std::chrono::milliseconds &timeout,
                    long long count)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return xreadgroup(group, consumer, first, last, timeout, count, false);
    }

    template <typename Input>
    auto xreadgroup(const StringView &group,
                    const StringView &consumer,
                    Input first,
                    Input last,
                    const std::chrono::milliseconds &timeout)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    QueuedPipe&>::type {
        return xreadgroup(group, consumer, first, last, timeout, 0, false);
    }

    QueuedPipe& xrevrange(const StringView &key,
                            const StringView &end,
                            const StringView &start) {
        return command(cmd::xrevrange, key, end, start);
    }

    QueuedPipe& xrevrange(const StringView &key,
                            const StringView &end,
                            const StringView &start,
                            long long count) {
        return command(cmd::xrevrange, key, end, start, count);
    }

    QueuedPipe& xtrim(const StringView &key, long long count, bool approx = true) {
        return command(cmd::xtrim, key, count, approx);
    }

private:
    friend class Redis;

    friend class RedisCluster;

    template <typename ...Args>
    QueuedPipe(Connection* connection, Args &&...args);

    void _sanity_check() const;

    void _reset();

    void _invalidate();

    void _rewrite_replies(std::vector<ReplyUPtr> &replies) const;

    template <typename Func>
    void _rewrite_replies(const std::vector<std::size_t> &indexes,
                            Func rewriter,
                            std::vector<ReplyUPtr> &replies) const;

    Connection* _connection;

    Impl _impl;

    std::size_t _cmd_num = 0;

    std::vector<std::size_t> _set_cmd_indexes;

    std::vector<std::size_t> _georadius_cmd_indexes;

    bool _valid = true;
};

}
}

#endif