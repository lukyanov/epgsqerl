%%%-------------------------------------------------------------------
%% @doc epgsqerl API
%% @end
%%%-------------------------------------------------------------------

-module(epgsqerl).

%% API
-export([squery/2, equery/3]).

%%====================================================================
%% API functions
%%====================================================================

squery(PoolName, SQErl) when is_tuple(SQErl) ->
    SQL = sqerl:sql(SQErl, true),
    squery(PoolName, SQL);
squery(PoolName, SQL) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {squery, SQL})
    end).

equery(PoolName, SQErl, Params) when is_tuple(SQErl) ->
    Stmt = sqerl:unsafe_sql(SQErl, true),
    equery(PoolName, Stmt, Params);
equery(PoolName, Stmt, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {equery, Stmt, Params})
    end).

%%====================================================================
%% Internal functions
%%====================================================================
