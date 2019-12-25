#ifndef QUEUE_PIPE_HPP
#define QUEUE_PIPE_HPP
namespace sw {

namespace redis {

template <typename Impl>
template <typename ...Args>
QueuedPipe<Impl>::QueuedPipe(Connection* connection, Args &&...args) :
            _connection(connection),
            _impl(std::forward<Args>(args)...) {
    assert(_connection);
}



template <typename Impl>
template <typename Cmd, typename ...Args>
auto QueuedPipe<Impl>::command(Cmd cmd, Args &&...args)
    -> typename std::enable_if<!std::is_convertible<Cmd, StringView>::value,
                                QueuedPipe<Impl>&>::type {
    try {
        _sanity_check();

        _impl.command(*_connection, cmd, std::forward<Args>(args)...);

        ++_cmd_num;
    } catch (const Error &e) {
        _invalidate();
        throw;
    }

    return *this;
}

template <typename Impl>
template <typename ...Args>
QueuedPipe<Impl>& QueuedPipe<Impl>::command(const StringView &cmd_name, Args &&...args) {
    auto cmd = [](Connection &connection, const StringView &cmd_name, Args &&...args) {
                    CmdArgs cmd_args;
                    cmd_args.append(cmd_name, std::forward<Args>(args)...);
                    connection.send(cmd_args);
    };

    return command(cmd, cmd_name, std::forward<Args>(args)...);
}

template <typename Impl>
template <typename Input>
auto QueuedPipe<Impl>::command(Input first, Input last)
    -> typename std::enable_if<IsIter<Input>::value, QueuedPipe<Impl>&>::type {
    if (first == last) {
        throw Error("command: empty range");
    }

    auto cmd = [](Connection &connection, Input first, Input last) {
                    CmdArgs cmd_args;
                    while (first != last) {
                        cmd_args.append(*first);
                        ++first;
                    }
                    connection.send(cmd_args);
    };

    return command(cmd, first, last);
}

template <typename Impl>
QueuedReplies QueuedPipe<Impl>::exec() {
    try {
        _sanity_check();

        auto replies = _impl.exec(*_connection, _cmd_num);

        _rewrite_replies(replies);

        _reset();

        return QueuedReplies(std::move(replies));
    } catch (const Error &e) {
        _invalidate();
        throw;
    }
}

template <typename Impl>
void QueuedPipe<Impl>::discard() {
    try {
        _sanity_check();

        _impl.discard(*_connection, _cmd_num);

        _reset();
    } catch (const Error &e) {
        _invalidate();
        throw;
    }
}

template <typename Impl>
void QueuedPipe<Impl>::_sanity_check() const {
    if (!_valid) {
        throw Error("Not in valid state");
    }

    if (_connection->broken()) {
        throw Error("Connection is broken");
    }
}

template <typename Impl>
inline void QueuedPipe<Impl>::_reset() {
    _cmd_num = 0;

    _set_cmd_indexes.clear();

    _georadius_cmd_indexes.clear();
}

template <typename Impl>
void QueuedPipe<Impl>::_invalidate() {
    _valid = false;

    _reset();
}

template <typename Impl>
void QueuedPipe<Impl>::_rewrite_replies(std::vector<ReplyUPtr> &replies) const {
    _rewrite_replies(_set_cmd_indexes, reply::rewrite_set_reply, replies);

    _rewrite_replies(_georadius_cmd_indexes, reply::rewrite_georadius_reply, replies);
}

template <typename Impl>
template <typename Func>
void QueuedPipe<Impl>::_rewrite_replies(const std::vector<std::size_t> &indexes,
                                            Func rewriter,
                                            std::vector<ReplyUPtr> &replies) const {
    for (auto idx : indexes) {
        assert(idx < replies.size());

        auto &reply = replies[idx];

        assert(reply);

        rewriter(*reply);
    }
}



}

}


#endif