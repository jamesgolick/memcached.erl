-module(memcached).

-behaviour(gen_server).

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
    ring
  }).

%% Public API

start_link(Servers) ->
  memcached_sup:start_link(),
  gen_server:start_link(?MODULE, [Servers], []).

stop(Pid) ->
  gen_server:call(Pid, stop).

state(Pid) ->
  gen_server:call(Pid, state).

%% gen_server callbacks

init([Servers]) ->
  lager:debug("init with servers: ~p", [Servers]),
  Ring = memcached_ring:create(Servers),
  lists:foreach(fun(Server) ->
	ok
    end, Servers),
  {ok, #state{ring=Ring}}.

handle_call(stop, _From, State) ->
  lager:debug("stopping by ~p, state was ~p.", [_From, State]),
  {stop, normal, stopped, State};

handle_call(state, _From, State) ->
  lager:debug("~p is asking for the state.", [_From]),
  {reply, State, State};

handle_call(get, _From, State) ->
  lager:debug("~p is asking for the state.", [_From]),
  {reply, State, State};

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
