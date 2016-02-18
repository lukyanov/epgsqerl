# epgsqerl

An Erlang PostgreSQL client, a mash-up of [epgsql](https://github.com/epgsql/epgsql), [poolboy](https://github.com/devinus/poolboy) and [sqerl](https://github.com/devinus/sqerl).

## Build

    $ ./rebar3 compile

## Try in shell

    $ ./rebar3 shell

Make sure you have set up `shell.config` properly first.

### Examples:

    % Using sqerl DSL:
    > epgsqerl:squery(pool1, {select, 1}).
    > epgsqerl:squery(pool1, {select, foo, {from, bar}, {where, {id,'=',1}}}).
    > epgsqerl:equery(pool1, {select, foo, {from, bar}, {where, "id=$1"}}, [1]).

    % Using raw SQL:
    > epgsqerl:squery(pool1, <<"SELECT 1">>).
    > epgsqerl:equery(pool1, <<"SELECT foo FROM bar WHERE id=$1">>, [1]).
