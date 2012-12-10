-module(memcached).

-behaviour(gen_server).

%% memcached commands

-export([get_ring/0,
	 get/1,
	 set/2,
	 set/3]).

-export([start_link/1,
	 state/1,
	 stop/1]).

-export([init/1,
	 handle_call/3,
    	 handle_cast/2,
    	 handle_info/2,
    	 terminate/2,
    	 code_change/3]).

-record(state, {
    ring,
    live_nodes,
    dead_nodes = []
  }).

%% Public API

start_link(Servers) ->
  memcached_sup:start_link(),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Servers], []).

stop(Pid) ->
  gen_server:call(Pid, stop).

state(Pid) ->
  gen_server:call(Pid, state).

get_ring() ->
  gen_server:call(?MODULE, get_ring).

get(Key) ->
  with_connection(Key, fun(Connection) ->
	memcached_conn:get(Connection, Key)
    end).

set(Key, Value) ->
  set(Key, Value, 0).

set(Key, Value, Expires) ->
  with_connection(Key, fun(Connection) ->
	memcached_conn:set(Connection, Key, Value, Expires)
    end).

%% gen_server callbacks

init([Servers]) ->
  lager:debug("init with servers: ~p", [Servers]),
  Ring = memcached_ring:create(Servers),
  lists:foreach(fun(Server) ->
	memcached_pool:create(Server)
    end, Servers),
  {ok, #state{ring=Ring,live_nodes=Servers}}.

handle_call(stop, _From, State) ->
  lager:debug("stopping by ~p, state was ~p.", [_From, State]),
  {stop, normal, stopped, State};

handle_call(state, _From, State) ->
  lager:debug("~p is asking for the state.", [_From]),
  {reply, State, State};

handle_call(get_ring, _From, State) ->
  lager:debug("~p is asking for the ring.", [_From]),
  {reply, State#state.ring, State};

handle_call(_Request, _From, State) ->
  lager:debug("call ~p, ~p, ~p.", [_Request, _From, State]),
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  lager:debug("cast ~p, ~p.", [_Msg, State]),
  {noreply, State}.

handle_info(_Info, State) ->
  lager:debug("info ~p, ~p.", [_Info, State]),
  {noreply, State}.

terminate(_Reason, _State) ->
  lager:debug("terminate ~p, ~p", [_Reason, _State]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  lager:debug("code_change ~p, ~p, ~p", [_OldVsn, State, _Extra]),
  {ok, State}.

%% Internal functions

with_connection(Key, Fun) ->
  Ring = get_ring(),
  Server = memcached_ring:get(Key, Ring),
  memcached_pool:with_connection(Server, Fun).
