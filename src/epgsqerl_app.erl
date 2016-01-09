%%%-------------------------------------------------------------------
%% @doc epgsqerl public API
%% @end
%%%-------------------------------------------------------------------

-module(epgsqerl_app).

-behaviour(application).

%% Application callbacks
-export([start/2 ,stop/1]).

%% Public API
-export([start/0, stop/0]).

%%====================================================================
%% API
%%====================================================================

start() ->
    application:start(epgsqerl).

stop() ->
    application:stop(epgsqerl).

start(_StartType, _StartArgs) ->
    epgsqerl_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
