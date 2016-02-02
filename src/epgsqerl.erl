%%%-------------------------------------------------------------------
%% @doc epgsqerl API
%% @end
%%%-------------------------------------------------------------------

-module(epgsqerl).

%% API
-export([squery/2, squery/3, equery/3, equery/4]).

%%====================================================================
%% API functions
%%====================================================================

squery(PoolName, SQErl) ->
    squery(PoolName, SQErl, []).
squery(PoolName, SQErl, Opts) when is_tuple(SQErl) ->
    SQL = sqerl:sql(SQErl, true),
    squery(PoolName, SQL, Opts);
squery(PoolName, SQL, Opts) ->
    Result = poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {squery, SQL})
    end),
    format_result(Result, Opts).

equery(PoolName, SQErl, Params) ->
    equery(PoolName, SQErl, Params, []).
equery(PoolName, SQErl, Params, Opts) when is_tuple(SQErl) ->
    Stmt = sqerl:unsafe_sql(SQErl, true),
    equery(PoolName, Stmt, Params, Opts);
equery(PoolName, Stmt, Params, Opts) ->
    Result = poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {equery, Stmt, Params})
    end),
    format_result(Result, Opts).

%%====================================================================
%% Internal functions
%%====================================================================
format_result(Result, Opts) ->
    case lists:member(return_map, Opts) of
        true  -> format_epgsql_to_map(Result);
        false -> Result
    end.

format_epgsql_to_map({ok, Columns, Rows}) ->
    Result = lists:map(fun(Row) ->
        maps:from_list([
            {F, V} || {{column, F, _, _, _, _}, V}
                <- lists:zip(Columns, tuple_to_list(Row))
        ])
    end, Rows),
    {ok, Result}.

%%====================================================================
%% Tests
%%====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

format_result_test_() -> [
    {"No special formatting",
        fun() ->
            Input = {ok,
                [
                    {column, <<"f1">>,varchar,-1,259,0},
                    {column, <<"f2">>,varchar,-1,259,0}
                ],
                [
                    {<<"v11">>, <<"v12">>},
                    {<<"v21">>, <<"v22">>}
                ]
            },
            Result = format_result(Input, []),
            ?assertEqual(Input, Result)
        end},
    {"Format as map",
        fun() ->
            Input = {ok,
                [
                    {column, <<"f1">>,varchar,-1,259,0},
                    {column, <<"f2">>,varchar,-1,259,0}
                ],
                [
                    {<<"v11">>, <<"v12">>},
                    {<<"v21">>, <<"v22">>}
                ]
            },
            Result = format_result(Input, [return_map]),
            Expected = {ok, [
                #{<<"f1">> => <<"v11">>, <<"f2">> => <<"v12">>},
                #{<<"f1">> => <<"v21">>, <<"f2">> => <<"v22">>}
            ]},
            ?assertEqual(Expected, Result)
        end}
].

-endif.
