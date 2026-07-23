#!/usr/bin/env escript
%%! -hidden -setcookie secret -sname xdnhttpkv
%% HTTP key-value frontend for the AntidoteDB reference service
%% (services/antidote-cluster). Runs as the entry sidecar in the member's
%% network namespace and talks to the local antidote node over distributed
%% Erlang rpc (the pb protocol would need a protobuf client; rpc is the
%% same channel the DC-link tooling already uses). Same uniform surface as
%% every XDN measurement shim:
%%
%%   GET  /            -> 200 once the local node answers rpc
%%   PUT  /kv/{key}    -> local LWW-register assign (commits with zero
%%                        synchronous coordination; replicates async)
%%   GET  /kv/{key}    -> local read
%%
%% Hand-rolled gen_tcp HTTP/1.1 loop (keep-alive, Content-Length framing)
%% because the antidote release ships without inets. XDN's forwarder pools
%% connections, so keep-alive handling is required, and every response
%% carries Content-Length so the frontend's keep-alive override cannot
%% leave a response unframed.
main(_) ->
    Node = list_to_atom("antidote@" ++ os:getenv("XDN_CLUSTER_SELF")),
    case os:getenv("XDN_CLUSTER_ORDINAL") of
        "0" -> spawn(fun link_dcs/0);
        _ -> ok
    end,
    {ok, LSock} = gen_tcp:listen(8080, [binary, {packet, raw}, {active, false},
                                        {reuseaddr, true}, {backlog, 64}]),
    io:format("xdn-antidote-http: serving :8080 for ~p~n", [Node]),
    accept_loop(LSock, Node).

%% Ordinal 0 links the DCs once every antidote node answers rpc: collect
%% each DC's connection descriptor, then subscribe every DC to the others'
%% update streams. Retries forever until it succeeds (nodes boot at their
%% own pace); folds the previously manual xdnlink step into the service.
link_dcs() ->
    Peers = string:split(os:getenv("XDN_CLUSTER_PEERS"), ",", all),
    Nodes = [list_to_atom("antidote@" ++ P) || P <- Peers],
    Descs = collect_descriptors(Nodes, #{}),
    lists:foreach(
      fun(N) ->
          Others = [maps:get(M, Descs) || M <- Nodes, M =/= N],
          R = rpc:call(N, antidote_dc_manager, subscribe_updates_from, [Others], 60000),
          io:format("xdn-antidote-http: ~p subscribed: ~p~n", [N, R])
      end, Nodes),
    io:format("xdn-antidote-http: DC linking complete~n").

collect_descriptors([], Acc) -> Acc;
collect_descriptors([N | Rest] = All, Acc) ->
    case rpc:call(N, antidote_dc_manager, get_connection_descriptor, [], 15000) of
        {ok, D} -> collect_descriptors(Rest, Acc#{N => D});
        _ -> timer:sleep(3000), collect_descriptors(All, Acc)
    end.

accept_loop(LSock, Node) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn(fun() -> conn(Sock, Node, <<>>) end),
    accept_loop(LSock, Node).

conn(Sock, Node, Buf0) ->
    case read_request(Sock, Buf0) of
        {ok, Method, Path, Headers, Body, Rest} ->
            {Code, RespBody} = handle(Method, Path, Body, Node),
            Keep = keepalive(Headers),
            ok = gen_tcp:send(Sock, response(Code, RespBody, Keep)),
            case Keep of
                true -> conn(Sock, Node, Rest);
                false -> gen_tcp:close(Sock)
            end;
        eof ->
            gen_tcp:close(Sock)
    end.

%% Accumulate until the header terminator, then read Content-Length bytes.
read_request(Sock, Buf) ->
    case binary:split(Buf, <<"\r\n\r\n">>) of
        [Head, Rest0] ->
            [ReqLine | HeaderLines] = binary:split(Head, <<"\r\n">>, [global]),
            case binary:split(ReqLine, <<" ">>, [global]) of
                [Method, Path | _] ->
                    Headers = [parse_header(H) || H <- HeaderLines],
                    CL = content_length(Headers),
                    {Body, Rest} = read_body(Sock, Rest0, CL),
                    {ok, Method, Path, Headers, Body, Rest};
                _ ->
                    eof   % readiness-gate CRLF probe or junk: just close
            end;
        [_] ->
            case gen_tcp:recv(Sock, 0, 60000) of
                {ok, More} -> read_request(Sock, <<Buf/binary, More/binary>>);
                {error, _} -> eof
            end
    end.

read_body(_Sock, Have, CL) when byte_size(Have) >= CL ->
    <<Body:CL/binary, Rest/binary>> = Have,
    {Body, Rest};
read_body(Sock, Have, CL) ->
    case gen_tcp:recv(Sock, 0, 60000) of
        {ok, More} -> read_body(Sock, <<Have/binary, More/binary>>, CL);
        {error, _} -> {Have, <<>>}
    end.

parse_header(Line) ->
    case binary:split(Line, <<":">>) of
        [K, V] -> {string:lowercase(K), string:trim(V)};
        _ -> {<<>>, <<>>}
    end.

content_length(Headers) ->
    case lists:keyfind(<<"content-length">>, 1, Headers) of
        {_, V} -> binary_to_integer(V);
        false -> 0
    end.

keepalive(Headers) ->
    case lists:keyfind(<<"connection">>, 1, Headers) of
        {_, V} -> string:lowercase(V) =/= <<"close">>;
        false -> true   % HTTP/1.1 default
    end.

obj(Key) -> {Key, antidote_crdt_register_lww, <<"bw">>}.

handle(<<"GET">>, <<"/kv/", Key/binary>>, _Body, Node) ->
    case rpc:call(Node, antidote, read_objects, [ignore, [], [obj(Key)]], 10000) of
        {ok, [Val], _} when is_binary(Val) -> {200, Val};
        {ok, [_], _} -> {404, <<"not found">>};
        Err -> {500, iolist_to_binary(io_lib:format("~p", [Err]))}
    end;
handle(M, <<"/kv/", Key/binary>>, Body, Node) when M =:= <<"PUT">>; M =:= <<"POST">> ->
    case rpc:call(Node, antidote, update_objects,
                  [ignore, [], [{obj(Key), assign, Body}]], 10000) of
        {ok, _} -> {200, <<"OK">>};
        Err -> {500, iolist_to_binary(io_lib:format("~p", [Err]))}
    end;
handle(<<"GET">>, _, _Body, Node) ->
    case rpc:call(Node, erlang, node, [], 3000) of
        {badrpc, _} -> {503, <<"warming up">>};
        _ -> {200, <<"ok antidote-http">>}
    end;
handle(_, _, _, _) ->
    {405, <<"method not allowed">>}.

response(Code, Body, Keep) ->
    Conn = case Keep of true -> <<"keep-alive">>; false -> <<"close">> end,
    [<<"HTTP/1.1 ">>, status(Code), <<"\r\nContent-Length: ">>,
     integer_to_binary(byte_size(Body)),
     <<"\r\nContent-Type: application/octet-stream\r\nConnection: ">>, Conn,
     <<"\r\n\r\n">>, Body].

status(200) -> <<"200 OK">>;
status(404) -> <<"404 Not Found">>;
status(405) -> <<"405 Method Not Allowed">>;
status(503) -> <<"503 Service Unavailable">>;
status(_) -> <<"500 Internal Server Error">>.
