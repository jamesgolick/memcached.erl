-module(memcached_multiget_coordinator).

-behaviour(gen_fsm).

%% public apis

-export([start/2,
	 get_result/1]).

%% gen_fsm callbacks

-export([init/1,
         waiting/3,
         finished/3,
	 handle_event/3,
	 handle_sync_event/4,
	 handle_info/3,
	 terminate/3,
	 code_change/4]).

-record(state, {
    waiting = [],
    results = [],
    waiter
  }).

%% public api

start(Keys, Ring) ->
  gen_fsm:start(?MODULE, [Keys, Ring], []).

get_result(Pid) ->
  gen_fsm:sync_send_event(Pid, get_result).

%% gen_fsm callbacks

init([Keys, Ring]) ->
  Nodes = lists:foldl(fun(Key, NodeList) ->
	Node = memcached_ring:get(Key, Ring),
	NodeKeys = proplists:get_value(Node, NodeList, []),
	[{Node, [Key | NodeKeys]} | proplists:delete(Node, NodeList)]
    end, [], Keys),
  Coordinator = self(),
  lager:debug("multiget to nodes: ~p", [Nodes]),
  lists:foreach(fun({Node, NodeKeys}) ->
	spawn(fun() -> multiget(Node, NodeKeys, Coordinator) end)
    end, Nodes),
  {ok, waiting, #state{waiting=[Node || {Node, _} <- Nodes]}}.

waiting(get_result, From, State) ->
  {next_state, waiting, State#state{waiter=From}};
waiting({result, Node, Value}, _From, State) ->
  NewWaiting = lists:delete(Node, State#state.waiting),
  NewResults = State#state.results ++ Value,
  NewState = State#state{waiting=NewWaiting,results=NewResults},
  case NewWaiting of
    [] when State#state.waiter =/= undefined ->
      gen_fsm:reply(State#state.waiter, NewResults),
      {stop, normal, stopped, NewState};
    [] ->
      {reply, ok, finished, NewState};
    _ ->
      {reply, ok, waiting, NewState}
  end.

finished(get_result, From, State) ->
  gen_fsm:reply(From, State#state.results),
  {stop, normal, stopped, State}.

handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
  {reply, ok, StateName, State}.

handle_info(_Info, StateName, State) ->
  {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
  ok.

code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%% Internal functions.

report(Pid, Node, Value) ->
  gen_fsm:sync_send_event(Pid, {result, Node, Value}).

multiget(Node, Keys, Coordinator) ->
  case catch do_multiget(Node, Keys) of
    {'EXIT', Reason} ->
      lager:info("mutliget to ~p failed with reason ~p", [Node, Reason]),
      memcached:mark_down(Node),
      report(Coordinator, Node, []);
    Value ->
      report(Coordinator, Node, Value)
  end.

do_multiget(Node, Keys) ->
  memcached_pool:with_connection(Node, fun(Connection) ->
	memcached_conn:multiget(Connection, Keys)
    end).
