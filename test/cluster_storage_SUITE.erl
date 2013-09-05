%%% vim: ts=4 sts=4 sw=4 expandtab

-module(cluster_storage_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).

-export([
    test/1
]).
 
all() -> [
    test
].

application_spec() ->
    {application, cluster_storage,
     [
      {applications, [kernel, stdlib]},
      {mod, { cluster_storage_app, []}},
      {env, [
              {storage_nodes, [node()]},
              {buckets, [cluster_storage_test]},
              {dump_dir, "."}
      ]}
     ]}.
 
test(_Config) ->
    application:load(application_spec()),
    ok = application:start(cluster_storage),
    true = whereis(cluster_storage_sup) =/= undefined,

    S = cluster_storage_test,

    {error, not_found} = cluster_storage:get(S, "key1"),
    {error, not_found} = cluster_storage:get(S, "key2"),
    cluster_storage:store(S, "key1", "value1"),
    cluster_storage:store(S, "key2", "value2", 3),
    timer:sleep(100),
    {ok, "value1"} = cluster_storage:get(S, "key1"),
    {ok, "value2"} = cluster_storage:get(S, "key2"),
    timer:sleep(3001),
    {ok, "value1"} = cluster_storage:get(S, "key1"),
    {error, not_found}, cluster_storage:get(S, "key2"),
    [gen_server:multi_call([node()], S, {delete, Key}) || Key <- ["key1", "key2"]],

    ok = application:stop(cluster_storage),
    application:unload(cluster_storage).
