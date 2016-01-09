%%%-------------------------------------------------------------------
%% @doc epgsqerl poolboy worker
%% @end
%%%-------------------------------------------------------------------

-module(epgsqerl_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
            code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {conn}).

%%====================================================================
%% API functions
%%====================================================================

start_link(Args) ->
    gen_server:start_link(?SERVER, Args, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Args) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    Timeout  = proplists:get_value(timeout, Args, 5000),
    Port     = proplists:get_value(port, Args, 5432),
    {ok, Conn} = epgsql:connect(Hostname, Username, Password, [
        {database, Database},
        {port, Port},
        {timeout, Timeout}
    ]),
    {ok, #state{ conn = Conn }}.

%%====================================================================
%% Internal functions
%%====================================================================

handle_call({squery, Sql}, _From, #state{ conn = Conn } = State) ->
    {reply, epgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{ conn = Conn } = State) ->
    {reply, epgsql:equery(Conn, Stmt, Params), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{ conn = Conn }) ->
    ok = epgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
