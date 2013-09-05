%%% 
%%% Copyright (c) 2011 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%% 
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(cluster_storage).
-behaviour(gen_server).

-export([
        % public API
        start_link/1,

        delete/2,
        get/2,
        get/3,
        status/1,
        store/3,
        store/4,
        
        stop/1,

        selftest/0,

        % gen_server callbacks
        code_change/3,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        init/1,
        terminate/2,

        % private but needed to be exported
        dump_table/2
    ]).

-define(CLEANUP_INTERVAL_SECS, 3600 * 2).
-define(DUMPER_DELAY_SECS, 300).
-define(GOSSIP_INTERVAL_MSECS, 1000).

% We don't remove keys from table right off
% because some node can be down for some time
% and it must learn that a key was updated
% with earlier expiration date and then have been expired
% before the node is up again.
-define(CLEANUP_DEFER_SECS, 86400).

-record(state, {server_name,
                tab,
                idxtab,
                idxpool = [],
                maxidx=0,
                cleaner,
                dumper,
                gossip}).

% Main table, tab, schema is:
%   {Key, Value, ExpireTs, LastUpdateTs, Index}
% where Index is a key in the index table, idxtab, which schema is
%   {Index, MainTableKey}
% Index table helps to find random key in the main table

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(ServerName) ->
    case lists:member(node(), server_nodes(ServerName)) of
        true -> gen_server:start_link({local, ServerName}, ?MODULE, ServerName, []);
        _ -> ignore
    end.

store(ServerName, Key, Value) ->
    store(ServerName, Key, Value, 86400 * 365 * 20). % maximum expiration period is 20 years
store(_ServerName, _Key, _Value, ExpireSecs) when ExpireSecs =< 0 -> ok;
store(ServerName, Key, Value, ExpireSecs) ->
    gen_server:multi_call(server_nodes(ServerName), ServerName,
        {store, Key, Value, ExpireSecs}).

get(ServerName, Key) ->
    gen_server:call(ServerName, {get, Key}).

get(ServerName, Key, Default) ->
    case get(ServerName, Key) of
        {ok, Value} -> Value;
        _ -> Default
    end.

delete(ServerName, Key) ->
    gen_server:multi_call(server_nodes(ServerName), ServerName,
        {delete, Key}).

status(ServerName) ->
    gen_server:call(ServerName, status).

stop(ServerName) ->
    gen_server:multi_call(server_nodes(ServerName), ServerName, stop).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%

handle_call({get, Key}, _From, #state{tab=Tab} = State) ->
    Ts = jsk_common:unixtime(),
    Res = case ets:lookup(Tab, Key) of
        [{_, Value, ExpireTs, _, _}] -> 
            case ExpireTs > Ts of
                true -> {ok, Value};
                false -> {error, not_found}
            end;
        _ -> {error, not_found}
    end,
    {reply, Res, State};
handle_call({store, Key, Value, ExpireSecs}, _From,
        #state{server_name=Name, tab=Tab, dumper=Dumper, idxtab=IdxTab, idxpool=IdxPool, maxidx=MaxIdx} = State) ->
    Ts = jsk_common:unixtime(),
    {Idx, State1} = case IdxPool of
        [] ->
            I = MaxIdx + 1,
            {I, State#state{maxidx=I}};
        [I | Rest] ->
            {I, State#state{idxpool=Rest}}
    end,
    ets:insert(Tab, {Key, Value, Ts + ExpireSecs, Ts, Idx}),
    ets:insert(IdxTab, {Idx, Key}),
    NewState = case Dumper of
        undefined ->
            jerr:log_debug(?MODULE, "Server ~p: Starting new dumper process", [Name]),
            Pid = spawn_link(fun() ->
                timer:apply_after(?DUMPER_DELAY_SECS * 1000, ?MODULE, dump_table, [Name, Tab])
            end),
            State1#state{dumper=Pid};
        _ -> State1
    end,
    jerr:log_debug(?MODULE, "Server ~p: stored key ~p with value ~p expiring in ~p secs", [Name, Key, Value, ExpireSecs]),
    {reply, ok, NewState};
handle_call({delete, Key}, _From, #state{server_name=Name, tab=Tab, idxtab=IdxTab, idxpool=IdxPool} = State) ->
    NewState = case ets:lookup(Tab, Key) of
        [] -> State;
        [{_, _, _, _, Idx}] ->
            ets:delete(Tab, Key),
            ets:delete(IdxTab, Idx),
            State#state{idxpool=[Idx | IdxPool]}
    end,
    jerr:log_debug(?MODULE, "Server ~p: deleted key ~p", [Name, Key]),
    {reply, ok, NewState};
handle_call(status, _From, State) ->
    {reply, State, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {noreply, State}.

%%

handle_cast({gossip, Key, Value, ExpireTs, LastUpdateTs}, #state{server_name=Name, tab=Tab} = State) ->
    spawn(fun() ->
        Ts = jsk_common:unixtime(),
        case ets:lookup(Tab, Key) of
            [] ->
                jerr:log_debug(?MODULE, "Server ~p: gossip key ~p - unknown, storing...", [Name, Key]),
                ?MODULE:store(Name, Key, Value, ExpireTs - Ts);
            [{_, V, _, _, _}] when Value == V ->
                jerr:log_debug(?MODULE, "Server ~p: gossip key ~p - known", [Name, Key]),
                nop;
            [{_, _, _, T, _}] when T < LastUpdateTs ->
                jerr:log_debug(?MODULE, "Server ~p: gossip key ~p - known but value newer, storing...", [Name, Key]),
                ?MODULE:store(Name, Key, Value, ExpireTs - Ts);
            _ ->
                jerr:log_debug(?MODULE, "Server ~p: gossip key ~p - do not store", [Name, Key]),
                nop
        end
    end),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%%

handle_info(cleanup, #state{server_name=Name, tab=Tab, cleaner=undefined} = State) ->
    jerr:log_debug(?MODULE, "Server ~p: Starting new cleaner process", [Name]),
    Cleaner = spawn_link(fun() ->
        Ts = jsk_common:unixtime() + ?CLEANUP_DEFER_SECS,
        ets:safe_fixtable(Tab, true),
        ObsoleteKeys = ets:select(Tab,
                            [{{'$1', '_', '$2', '_', '_'},
                              [{'<', '$2', Ts}],
                              ['$1']
                            }]),
        ets:safe_fixtable(Tab, false),
        lists:foreach(fun(Key) ->
            timer:sleep(200),
            jerr:log_debug(?MODULE, "Server ~p: Deleting obsolete key ~p", [Name, Key]),
            gen_server:call(Name, {delete, Key})
        end, ObsoleteKeys)
    end),
    {noreply, State#state{cleaner=Cleaner}};
handle_info(cleanup, #state{server_name=Name} = State) ->
    jerr:log_warning(?MODULE, "Server ~p: Cleaner process is still running", [Name]),
    {noreply, State};
handle_info(gossip, #state{maxidx=0} = State) ->
    {noreply, State};
handle_info(gossip, #state{server_name=Name, tab=Tab, idxtab=IdxTab, maxidx=MaxIdx, gossip=undefined} = State) ->
    Gossip = spawn_link(fun() ->
        RandomIdx = rnd_server:uniform(MaxIdx),
        % Looking for RandomIdx in the index table
        % then returning the corresponding key of the main table
        Key = case ets:lookup(IdxTab, RandomIdx) of
            [] ->
                % if not exact match found, looking for smaller existing index
                case ets:lookup(IdxTab, ets:prev(IdxTab, RandomIdx)) of
                    [] -> ets:first(Tab);
                    [{_, K}] -> K
                end;
            [{_, K}] -> K
        end,
        jerr:log_debug(?MODULE, "Server ~p: gossip key = ~p", [Name, Key]),
        case Key of
            '$end_of_table' -> nop;
            _ ->
                case ets:lookup(Tab, Key) of
                    [] ->
                        jerr:log_debug(?MODULE, "Server ~p: lookup unexpectedly failed", [Name]),
                        nop;
                    [{_, V, ExpireTs, T, _}] ->
                        ThisNode = node(),
                        case rnd_server:random_element([N || N <- server_nodes(Name), N /= ThisNode]) of
                            undefined ->
                                jerr:log_debug(?MODULE, "Server ~p: only one node in the pool for the server", [Name]);
                            Node ->
                                gen_server:cast({Name, Node}, {gossip, Key, V, ExpireTs, T}),
                                jerr:log_debug(?MODULE, "Server ~p: gossipped key ~p to node ~p", [Name, Key, Node])
                        end;
                    _ -> nop
                end
        end
    end),
    {noreply, State#state{gossip=Gossip}};
handle_info({'EXIT', From, _Reason}, #state{server_name=Name, cleaner=From} = State) ->
    jerr:log_debug(?MODULE, "Server ~p: Cleaner process done", [Name]),
    {noreply, State#state{cleaner=undefined}};
handle_info({'EXIT', From, _Reason}, #state{server_name=Name, dumper=From} = State) ->
    jerr:log_debug(?MODULE, "Server ~p: Dumper process done", [Name]),
    {noreply, State#state{dumper=undefined}};
handle_info({'EXIT', From, _Reason}, #state{server_name=Name, gossip=From} = State) ->
    jerr:log_debug(?MODULE, "Server ~p: Gossip process done", [Name]),
    {noreply, State#state{gossip=undefined}};
handle_info(_Request, State) ->
    {noreply, State}.

%%

init(Name) ->
    process_flag(trap_exit, true),
    % loading table from file if exists
    Filename = dump_filename(Name),
    Tab = case ets:file2tab(Filename, [{verify, true}]) of
        {ok, Table} -> Table;
        {error, Reason} ->
            jerr:log_warning(?MODULE, "Server ~p: Failed to load dump file '~p'. Reason: ~p", [Name, Filename, Reason]),
            ets:new(list_to_atom("cluster_storage_tab-" ++ atom_to_list(Name)), [])
    end,
    % building index
    IdxTab = ets:new(list_to_atom("cluster_storage_idx_tab-" ++ atom_to_list(Name)), [ordered_set]),
    MaxIdx = ets:foldl(fun({Key, Value, ExpireTS, LastUpdateTS, _}, PrevI) ->
        I = PrevI + 1,
        ets:insert(Tab, {Key, Value, ExpireTS, LastUpdateTS, I}),
        ets:insert(IdxTab, {I, Key}),
        I
    end, 0, Tab),
    % setting up cleanup interval
    timer:send_interval(?CLEANUP_INTERVAL_SECS * 1000, cleanup),
    % setting up gossip  interval
    timer:send_interval(?GOSSIP_INTERVAL_MSECS, gossip),
    {ok, #state{server_name=Name, tab=Tab, idxtab=IdxTab, maxidx=MaxIdx}}.

%%

terminate(_Reason, #state{server_name=Name, tab=Tab}) ->
    dump_table(Name, Tab),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dump_filename(ServerName) ->
    "/js-kit/data/cluster-storage-" ++ jsk_common:to_list(ServerName) ++ ".dump".

dump_table(Name, Tab) ->
    ets:safe_fixtable(Tab, true),
    ets:tab2file(Tab, dump_filename(Name)),
    ets:safe_fixtable(Tab, false).

server_nodes(ServerName) -> jcfg:val({cluster_storage_nodes, ServerName}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

selftest() ->
    S = cluster_storage_test,
    {ok, _Pid} = ?MODULE:start_link(S),
    {error, not_found} = ?MODULE:get(S, "key1"),
    {error, not_found} = ?MODULE:get(S, "key2"),
    ?MODULE:store(S, "key1", "value1"),
    ?MODULE:store(S, "key2", "value2", 3),
    timer:sleep(100),
    {ok, "value1"} = ?MODULE:get(S, "key1"),
    {ok, "value2"} = ?MODULE:get(S, "key2"),
    timer:sleep(3001),
    {ok, "value1"} = ?MODULE:get(S, "key1"),
    {error, not_found} = ?MODULE:get(S, "key2"),
    [gen_server:multi_call(jcfg:val({cluster_storage_nodes, S}), S, {delete, Key}) || Key <- ["key1", "key2"]],
    ?MODULE:stop(S),
    ok.
