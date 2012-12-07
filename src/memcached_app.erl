-module(memcached_app).

-behaviour(application).

-export([start/0]).

%% Application callbacks
-export([start/2, stop/1]).

start() ->
  application:start(compiler),
  application:start(syntax_tools),
  application:start(lager),
  application:start(memcached).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  memcached_sup:start_link().

stop(_State) ->
  ok.
