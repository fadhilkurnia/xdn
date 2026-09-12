#!/usr/bin/env escript
%%! -hidden -setcookie secret -sname xdnlink
%% Control-plane helper for the XDN antidote-cluster reference service.
%% Runs inside a member container (docker cp + docker exec); talks to the
%% local antidote node over distributed Erlang (the release's nodetool reads
%% the unsubstituted vm.args and cannot find the node, so it is bypassed).
%%
%%   desc                  print this DC's connection descriptor (raw + b64)
%%   sub <b64> <b64>       subscribe this DC to the other DCs' update streams
%%   write <val>           probe write (lww register "probe" in bucket "bw")
%%   read                  probe read
%%   load <write|read> <ms>  timed load loop (~10 ops/s), prints OPS=<n>
%%
%% The rpc dist connection dials the node's own overlay alias, so it shows
%% up as a self-edge (replica-N -> replica-N) in the bandwidth profile;
%% analysis must ignore self-edges for antidote runs.

node_name() ->
    list_to_atom("antidote@" ++ os:getenv("XDN_CLUSTER_SELF")).

main(["desc"]) ->
    {ok, D} = rpc:call(node_name(), antidote_dc_manager, get_connection_descriptor, []),
    io:format("RAW=~p~n", [D]),
    io:format("B64=~s~n", [base64:encode(term_to_binary(D))]);
main(["sub" | B64s]) ->
    Ds = [binary_to_term(base64:decode(list_to_binary(B))) || B <- B64s],
    R = rpc:call(node_name(), antidote_dc_manager, subscribe_updates_from, [Ds], 60000),
    io:format("SUB=~p~n", [R]);
main(["write", Val]) ->
    R = rpc:call(node_name(), antidote, update_objects,
                 [ignore, [], [{{<<"probe">>, antidote_crdt_register_lww, <<"bw">>},
                                assign, list_to_binary(Val)}]]),
    io:format("W=~p~n", [R]);
main(["read"]) ->
    R = rpc:call(node_name(), antidote, read_objects,
                 [ignore, [], [{<<"probe">>, antidote_crdt_register_lww, <<"bw">>}]]),
    io:format("R=~p~n", [R]);
main(["load", Phase, MsStr]) ->
    Deadline = erlang:monotonic_time(millisecond) + list_to_integer(MsStr),
    Ops = loop(node_name(), Phase, Deadline, binary:copy(<<"x">>, 256), 0),
    io:format("OPS=~p~n", [Ops]).

loop(N, Phase, Deadline, Val, Acc) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        false ->
            Acc;
        true ->
            Key = integer_to_binary(Acc rem 64),
            case Phase of
                "write" ->
                    {ok, _} = rpc:call(N, antidote, update_objects,
                        [ignore, [],
                         [{{Key, antidote_crdt_register_lww, <<"bw">>}, assign, Val}]]);
                _ ->
                    {ok, _, _} = rpc:call(N, antidote, read_objects,
                        [ignore, [], [{Key, antidote_crdt_register_lww, <<"bw">>}]])
            end,
            timer:sleep(100),
            loop(N, Phase, Deadline, Val, Acc + 1)
    end.
