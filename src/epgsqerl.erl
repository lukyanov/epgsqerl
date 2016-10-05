%%-------------------------------------------------------------------
%% @doc epgsqerl API
%%
%% @end
%%-------------------------------------------------------------------
-module(epgsqerl).

%% API
-export([connect/2,
         squery/2, squery/3,
         equery/3, equery/4,
         with_transaction/2, with_transaction/3]).

-define(DEFAULT_QUERY_TIMEOUT, 5000).

-record(opts, {
    timeout = ?DEFAULT_QUERY_TIMEOUT,
    no_maps = false
}).

%%===================================================================
%% API
%%===================================================================

connect(PoolName, Settings) ->
    pgapp:connect(PoolName, Settings).

squery(PoolName, SQErl) ->
    squery(PoolName, SQErl, []).
squery(PoolName, SQErl, Opts) when is_tuple(SQErl) ->
    SQL = sqerl:sql(SQErl, true),
    squery(PoolName, SQL, Opts);
squery(PoolName, SQL, Opts) ->
    ParsedOpts = parse_opts(Opts),
    Result = pgapp:squery(PoolName, SQL, ParsedOpts#opts.timeout),
    format_result(Result, ParsedOpts).

equery(PoolName, SQErl, Params) ->
    equery(PoolName, SQErl, Params, []).
equery(PoolName, SQErl, Params, Opts) when is_tuple(SQErl) ->
    Stmt = sqerl:unsafe_sql(SQErl, true),
    equery(PoolName, Stmt, Params, Opts);
equery(PoolName, Stmt, Params, Opts) ->
    ParsedOpts = parse_opts(Opts),
    Result = pgapp:equery(PoolName, Stmt, Params, ParsedOpts#opts.timeout),
    format_result(Result, ParsedOpts).

with_transaction(PoolName, Fun) ->
    with_transaction(PoolName, Fun, []).
with_transaction(PoolName, Fun, Opts) ->
    ParsedOpts = parse_opts(Opts),
    Result = pgapp:with_transaction(PoolName, Fun, ParsedOpts#opts.timeout),
    format_result(Result, ParsedOpts).

%%====================================================================
%% Internal functions
%%====================================================================

parse_opts(Opts) ->
    parse_opts(Opts, #opts{}).
parse_opts([], Acc) ->
    Acc;
parse_opts([no_maps | Opts], Acc) ->
    parse_opts(Opts, Acc#opts{ no_maps = true });
parse_opts([{timeout, Timeout} | Opts], Acc) ->
    parse_opts(Opts, Acc#opts{ timeout = Timeout }).

format_result({ok, Columns, Rows} = Result, Opts) ->
    case Opts#opts.no_maps of
        false -> {ok, format_epgsql_to_map(Columns, Rows)};
        true  -> Result
    end;
format_result({ok, N, Columns, Rows} = Result, Opts) ->
    case Opts#opts.no_maps of
        false -> {ok, N, format_epgsql_to_map(Columns, Rows)};
        true  -> Result
    end;
format_result({ok, _} = Result, _Opts) ->
    Result;
format_result({error, _} = Result, _Opts) ->
    Result.

format_epgsql_to_map(Columns, Rows) ->
    Length = length(Columns),
    Result = lists:map(fun(Row) ->
        maps:from_list([
            {fix_column_name(F, I), V} || {{column, F, _, _, _, _}, V, I}
                <- lists:zip3(Columns,
                        tuple_to_list(Row),
                        lists:seq(1, Length))
        ])
    end, Rows),
    Result.

fix_column_name(<<"?column?">>, Index) ->
    integer_to_binary(Index);
fix_column_name(Name, _Index) ->
    Name.

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
            Result = format_result(Input, #opts{ no_maps = true }),
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
            Result = format_result(Input, #opts{}),
            Expected = {ok, [
                #{<<"f1">> => <<"v11">>, <<"f2">> => <<"v12">>},
                #{<<"f1">> => <<"v21">>, <<"f2">> => <<"v22">>}
            ]},
            ?assertEqual(Expected, Result)
        end},
    {"Format as map when no fields names",
        fun() ->
            Input = {ok,
                [
                    {column, <<"?column?">>,varchar,-1,259,0},
                    {column, <<"?column?">>,varchar,-1,259,0}
                ],
                [
                    {<<"v11">>, <<"v12">>},
                    {<<"v21">>, <<"v22">>}
                ]
            },
            Result = format_result(Input, #opts{}),
            Expected = {ok, [
                #{<<"1">> => <<"v11">>, <<"2">> => <<"v12">>},
                #{<<"1">> => <<"v21">>, <<"2">> => <<"v22">>}
            ]},
            ?assertEqual(Expected, Result)
        end},
    {"Formatting when simple insert/update/delete",
        fun() ->
            Input = {ok, 1},
            Result = format_result(Input, []),
            ?assertEqual(Input, Result)
        end},
    {"Formatting when insert/update with returning",
        fun() ->
            Input = {ok, 1,
                [
                    {column, <<"f1">>,varchar,-1,259,0},
                    {column, <<"f2">>,varchar,-1,259,0}
                ],
                [
                    {<<"v11">>, <<"v12">>},
                    {<<"v21">>, <<"v22">>}
                ]
            },
            Result = format_result(Input, #opts{}),
            Expected = {ok, 1, [
                #{<<"f1">> => <<"v11">>, <<"f2">> => <<"v12">>},
                #{<<"f1">> => <<"v21">>, <<"f2">> => <<"v22">>}
            ]},
            ?assertEqual(Expected, Result)
        end},
    {"Formatting when error",
        fun() ->
            Input = {error,{error,error,<<"42P01">>,
                    <<"relation \"test\" does not exist">>,
                    [{position,<<"34">>}]}},
            Result = format_result(Input, []),
            ?assertEqual(Input, Result)
        end}
].

-endif.
