
-module(cluster_storage_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, Buckets} = application:get_env(cluster_storage, buckets),
    {ok, { {one_for_one, 5, 10}, workers(Buckets) }}.

workers(Buckets) ->
    [{B, {cluster_storage, start_link, [B]},
            permanent, 5000, worker, [cluster_storage]} || B <- Buckets].
