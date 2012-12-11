-module(memcached_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
	 add_server/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_server(Server) ->
  PoolDefaults = [{worker_module, memcached_conn},
		  {name, {local, list_to_atom(Server)}}],
  PoolOptions = get_pool_options(),
  Spec = poolboy:child_spec(Server,
			    PoolDefaults ++ PoolOptions,
			    Server),
  supervisor:start_child(?MODULE, Spec).


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  {ok, { {one_for_one, 5, 10}, []} }.

get_pool_options() ->
  case application:get_env(memcached, pool_options) of
    {ok, Options} ->
      Options;
    _ ->
      []
  end.

