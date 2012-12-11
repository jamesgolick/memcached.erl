-module(memcached).

-behaviour(gen_server).

%% memcached commands

-export([get_ring/0,
	 mark_down/1,
	 get/1,
	 multiget/1,
	 multiget/2,
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
    live_nodes :: set(),
    dead_nodes :: set()
  }).

-define(MAX_ATTEMPTS, 3).
-define(RETRY_TIME, 60).

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

mark_down(Server) ->
  gen_server:call(?MODULE, {mark_down, Server}).

get(Key) ->
  perform(Key, fun(Connection) ->
	memcached_conn:get(Connection, Key)
    end).

set(Key, Value) ->
  set(Key, Value, 0).

set(Key, Value, Expires) ->
  perform(Key, fun(Connection) ->
	memcached_conn:set(Connection, Key, Value, Expires)
    end).

multiget(Keys) ->
  case get_ring() of
    no_live_nodes ->
      lager:info("no live nodes!"),
      [];
    Ring ->
      {ok, Coordinator} = memcached_multiget_coordinator:start(Keys, Ring),
      memcached_multiget_coordinator:get_result(Coordinator)
  end.

multiget(Keys, MissFun) ->
  Result = multiget(Keys),
  HitKeys = [Key || {Key, _} <- Result],
  MissedKeys = Keys -- HitKeys,
  lager:debug("missed keys ~p", [MissedKeys]),
  MissedValues = MissFun(MissedKeys),
  lists:foreach(fun({Key, Value}) ->
	set(Key, Value)
    end, MissedValues),
  Result ++ MissedValues.

%% gen_server callbacks

init([Servers]) ->
  lager:debug("init with servers: ~p", [Servers]),
  TempState = #state{live_nodes=sets:from_list(Servers),dead_nodes=sets:new()},
  State = create_and_init_ring(TempState),
  {ok, State}.

handle_call(stop, _From, State) ->
  lager:debug("stopping by ~p, state was ~p.", [_From, State]),
  {stop, normal, stopped, State};

handle_call(state, _From, State) ->
  lager:debug("~p is asking for the state.", [_From]),
  {reply, State, State};

handle_call(get_ring, _From, State) ->
  lager:debug("~p is asking for the ring.", [_From]),
  {reply, State#state.ring, State};

handle_call({mark_up, Node}, _From, State) ->
  lager:info("marking ~p as up.", [Node]),
  DeadNodes = State#state.dead_nodes,
  case sets:is_element(Node, DeadNodes) of
    true ->
      NewLiveNodes = sets:add_element(Node, State#state.live_nodes),
      NewDeadNodes = sets:del_element(Node, DeadNodes),
      TempState = #state{live_nodes=NewLiveNodes,dead_nodes=NewDeadNodes},
      NewState = create_and_init_ring(TempState),
      {reply, ok, NewState};
    false ->
      {reply, ok, State}
  end;
handle_call({mark_down, Node}, _From, State) ->
  lager:info("marking ~p as down.", [Node]),
  LiveNodes = State#state.live_nodes,
  case sets:is_element(Node, LiveNodes) of
    true ->
      NewLiveNodes = sets:del_element(Node, LiveNodes),
      NewDeadNodes = sets:add_element(Node, State#state.dead_nodes),
      TempState = #state{live_nodes=NewLiveNodes,dead_nodes=NewDeadNodes},
      NewState = create_and_init_ring(TempState),
      schedule_retry_dead_node_message(Node),
      {reply, ok, NewState};
    false ->
      {reply, ok, State}
  end;

handle_call(_Request, _From, State) ->
  lager:debug("call ~p, ~p, ~p.", [_Request, _From, State]),
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  lager:debug("cast ~p, ~p.", [_Msg, State]),
  {noreply, State}.

handle_info({retry_dead_node, Node}, State) ->
  lager:debug("time to retry against dead node ~p", [Node]),
  retry_against_dead_node(Node),
  {noreply, State};
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

mark_up(Server) ->
  gen_server:call(?MODULE, {mark_up, Server}).

perform(Key, Fun) ->
  perform(Key, Fun, 1).

perform(Key, Fun, Attempt) ->
  case get_ring() of
    no_live_nodes ->
      erlang:throw(no_live_nodes);
    Ring ->
      Server = memcached_ring:get(Key, Ring),
      case catch memcached_pool:with_connection(Server, Fun) of
	{'EXIT', Reason} when Attempt =/= ?MAX_ATTEMPTS ->
	  lager:info("failed command against ~p reason: ~p", [Server, Reason]),
	  mark_down(Server),
	  perform(Key, Fun, Attempt + 1);
	Value ->
	  Value
      end
  end.

create_and_init_ring(State=#state{live_nodes=Nodes}) ->
  case sets:size(Nodes) of
    0 ->
      State#state{ring=no_live_nodes};
    _ ->
      NodesList = sets:to_list(Nodes),
      lists:foreach(fun(Node) ->
	    memcached_pool:create(Node)
	end, NodesList),
      State#state{ring=memcached_ring:create(NodesList)}
  end.

retry_against_dead_node(Node) ->
  spawn(fun() ->
	lager:info("Retrying against dead node ~p", [Node]),
	memcached_pool:create(Node),
	case catch run_retry_command_against(Node) of
	  {'EXIT', Reason} ->
	    lager:info("node ~p is still down. retry failed with ~p", [Node, Reason]),
	    schedule_retry_dead_node_message(Node);
	  _ ->
	    lager:info("node ~p is back up"),
	    mark_up(Node)
	end
    end).

run_retry_command_against(Node) ->
  memcached_pool:with_connection(Node, fun(Connection) ->
	memcached_conn:get(Connection, <<"a">>)
    end).

schedule_retry_dead_node_message(Node) ->
  erlang:send_after(?RETRY_TIME * 1000, ?MODULE, {retry_dead_node, Node}).
