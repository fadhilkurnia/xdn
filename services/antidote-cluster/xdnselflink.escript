#!/usr/bin/env escript
%%! -hidden -setcookie secret -sname xdnselflink
%% Self-linking for the XDN antidote cluster, run in the background by the
%% member entrypoint on ordinal 0 (self-clustering is the service's own
%% job; XDN and its shims stay topology-dumb). Waits until every DC's
%% antidote node answers rpc, collects each DC's connection descriptor,
%% then subscribes every DC to the others' update streams. Subscribing an
%% already-linked DC again is harmless, so restarts just re-run this.
main(_) ->
    Peers = string:split(os:getenv("XDN_CLUSTER_PEERS"), ",", all),
    Nodes = [list_to_atom("antidote@" ++ P) || P <- Peers],
    Descs = collect(Nodes, #{}),
    lists:foreach(
      fun(N) ->
          Others = [maps:get(M, Descs) || M <- Nodes, M =/= N],
          R = rpc:call(N, antidote_dc_manager, subscribe_updates_from, [Others], 60000),
          io:format("[xdn-selflink] ~p subscribed: ~p~n", [N, R])
      end, Nodes),
    io:format("[xdn-selflink] DC linking complete~n").

collect([], Acc) -> Acc;
collect([N | Rest] = All, Acc) ->
    case rpc:call(N, antidote_dc_manager, get_connection_descriptor, [], 15000) of
        {ok, D} -> collect(Rest, Acc#{N => D});
        _ -> timer:sleep(3000), collect(All, Acc)
    end.
