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
    case lists:member(node(), server_nodes()) of
        true -> gen_server:start_link({local, ServerName}, ?MODULE, ServerName, []);
        _ -> ignore
    end.

store(ServerName, Key, Value) ->
    store(ServerName, Key, Value, 86400 * 365 * 20). % maximum expiration period is 20 years
store(_ServerName, _Key, _Value, ExpireSecs) when ExpireSecs =< 0 -> ok;
store(ServerName, Key, Value, ExpireSecs) ->
    gen_server:multi_call(server_nodes(), ServerName,
        {store, Key, Value, ExpireSecs}).

get(ServerName, Key) ->
    gen_server:call(ServerName, {get, Key}).

get(ServerName, Key, Default) ->
    case get(ServerName, Key) of
        {ok, Value} -> Value;
        _ -> Default
    end.

delete(ServerName, Key) ->
    gen_server:multi_call(server_nodes(), ServerName,
        {delete, Key}).

status(ServerName) ->
    gen_server:call(ServerName, status).

stop(ServerName) ->
    gen_server:multi_call(server_nodes(), ServerName, stop).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%

handle_call({get, Key}, _From, #state{tab=Tab} = State) ->
    Ts = unixtime(),
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
    Ts = unixtime(),
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
            %error_logger:info_msg("Server ~p: Starting new dumper process~n", [Name]),
            Pid = spawn_link(fun() ->
                timer:apply_after(?DUMPER_DELAY_SECS * 1000, ?MODULE, dump_table, [Name, Tab])
            end),
            State1#state{dumper=Pid};
        _ -> State1
    end,
    %error_logger:info_msg("Server ~p: stored key ~p with value ~p expiring in ~p secs~n", [Name, Key, Value, ExpireSecs]),
    {reply, ok, NewState};
handle_call({delete, Key}, _From, #state{server_name=Name, tab=Tab, idxtab=IdxTab, idxpool=IdxPool} = State) ->
    NewState = case ets:lookup(Tab, Key) of
        [] -> State;
        [{_, _, _, _, Idx}] ->
            ets:delete(Tab, Key),
            ets:delete(IdxTab, Idx),
            State#state{idxpool=[Idx | IdxPool]}
    end,
    %error_logger:info_msg("Server ~p: deleted key ~p~n", [Name, Key]),
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
        Ts = unixtime(),
        case ets:lookup(Tab, Key) of
            [] ->
                %error_logger:info_msg("Server ~p: gossip key ~p - unknown, storing...~n", [Name, Key]),
                ?MODULE:store(Name, Key, Value, ExpireTs - Ts);
            [{_, V, _, _, _}] when Value == V ->
                %error_logger:info_msg("Server ~p: gossip key ~p - known~n", [Name, Key]),
                nop;
            [{_, _, _, T, _}] when T < LastUpdateTs ->
                %error_logger:info_msg("Server ~p: gossip key ~p - known but value newer, storing...~n", [Name, Key]),
                ?MODULE:store(Name, Key, Value, ExpireTs - Ts);
            _ ->
                %error_logger:info_msg("Server ~p: gossip key ~p - do not store~n", [Name, Key]),
                nop
        end
    end),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%%

handle_info(cleanup, #state{server_name=Name, tab=Tab, cleaner=undefined} = State) ->
    %error_logger:info_msg("Server ~p: Starting new cleaner process~n", [Name]),
    Cleaner = spawn_link(fun() ->
        Ts = unixtime() + ?CLEANUP_DEFER_SECS,
        ets:safe_fixtable(Tab, true),
        ObsoleteKeys = ets:select(Tab,
                            [{{'$1', '_', '$2', '_', '_'},
                              [{'<', '$2', Ts}],
                              ['$1']
                            }]),
        ets:safe_fixtable(Tab, false),
        lists:foreach(fun(Key) ->
            timer:sleep(200),
            %error_logger:info_msg("Server ~p: Deleting obsolete key ~p~n", [Name, Key]),
            gen_server:call(Name, {delete, Key})
        end, ObsoleteKeys)
    end),
    {noreply, State#state{cleaner=Cleaner}};
handle_info(cleanup, #state{server_name=Name} = State) ->
    error_logger:warning_msg("Server ~p: Cleaner process is still running~n", [Name]),
    {noreply, State};
handle_info(gossip, #state{maxidx=0} = State) ->
    {noreply, State};
handle_info(gossip, #state{server_name=Name, tab=Tab, idxtab=IdxTab, maxidx=MaxIdx, gossip=undefined} = State) ->
    Gossip = spawn_link(fun() ->
        RandomIdx = random:uniform(MaxIdx),
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
        %error_logger:info_msg("Server ~p: gossip key = ~p~n", [Name, Key]),
        case Key of
            '$end_of_table' -> nop;
            _ ->
                case ets:lookup(Tab, Key) of
                    [] ->
                        %error_logger:info_msg("Server ~p: lookup unexpectedly failed~n", [Name]),
                        nop;
                    [{_, V, ExpireTs, T, _}] ->
                        ThisNode = node(),
                        with_random([N || N <- server_nodes(), N /= ThisNode], fun(Node) ->
                            gen_server:cast({Name, Node}, {gossip, Key, V, ExpireTs, T})
                            %error_logger:info_msg("Server ~p: gossipped key ~p to node ~p~n", [Name, Key, Node])
                        end);
                    _ -> nop
                end
        end
    end),
    {noreply, State#state{gossip=Gossip}};
handle_info({'EXIT', From, _Reason}, #state{server_name=Name, cleaner=From} = State) ->
    %error_logger:info_msg("Server ~p: Cleaner process done~n", [Name]),
    {noreply, State#state{cleaner=undefined}};
handle_info({'EXIT', From, _Reason}, #state{server_name=Name, dumper=From} = State) ->
    %error_logger:info_msg("Server ~p: Dumper process done~n", [Name]),
    {noreply, State#state{dumper=undefined}};
handle_info({'EXIT', From, _Reason}, #state{server_name=Name, gossip=From} = State) ->
    %error_logger:info_msg("Server ~p: Gossip process done~n", [Name]),
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
        {error, _} ->
            error_logger:info_msg("Server ~p: Unable to load dump file '~p'. Creating new table...~n", [Name, Filename]),
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

unixtime() -> unixtime(now()).
unixtime({Mega, Secs, _Msecs}) -> Mega * 1000000 + Secs.

with_random([], _) -> nop;
with_random(Alternatives, Function) when is_function(Function, 1) ->
    N = length(Alternatives),
    R = element(3, now()),
    Element = lists:nth((R rem N) + 1, Alternatives),
    Function(Element).

dump_filename(ServerName) ->
    {ok, DumpDir} = application:get_env(cluster_storage, dump_dir),
    DumpDir ++ "/cluster-storage-" ++ atom_to_list(ServerName) ++ ".dump".

dump_table(Name, Tab) ->
    ets:safe_fixtable(Tab, true),
    ets:tab2file(Tab, dump_filename(Name)),
    ets:safe_fixtable(Tab, false).

server_nodes() ->
    {ok, Nodes} = application:get_env(cluster_storage, storage_nodes),
    Nodes.
