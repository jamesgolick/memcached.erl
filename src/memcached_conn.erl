-module(memcached_conn).

-behaviour(gen_fsm).

%% public apis

-export([start_link/1]).

-export([get/2,
	 multiget/2,
	 set/3,
	 set/4]).

%% gen_fsm callbacks

%% state callbacks

-export([ready/3,
	 waiting/2,
         waiting_for_multiget/2]).

%% generic callbacks

-export([init/1,
	 handle_event/3,
	 handle_sync_event/4,
	 handle_info/3,
	 terminate/3,
	 code_change/4]).

-record(state, {
    socket :: pid(),
    waiter,
    framed = [],
    incomplete = <<>>,
    decoded = []
  }).

-record(packet, {
    op :: atom(),
    status :: atom(),
    key :: binary(),
    value = <<>> :: binary(),
    extra = <<>> :: binary()
  }).

-define(MAGIC_REQUEST, 16#80).
-define(MAGIC_RESPONSE, 16#81).

%% public api

start_link(Server) ->
  case string:tokens(Server, ":") of
    [Host] ->
      start_link(Host, 11211);
    [Host, Port] ->
      start_link(Host, list_to_integer(Port))
  end.

start_link(Host, Port) ->
  gen_fsm:start_link(?MODULE, [Host, Port], []).

get(Pid, Key) when is_binary(Key) ->
  gen_fsm:sync_send_event(Pid, {get, Key}).

multiget(Pid, Keys) when is_list(Keys) ->
  gen_fsm:sync_send_event(Pid, {multiget, Keys}).

set(Pid, Key, Value) when is_binary(Key) and is_binary(Value) ->
  set(Pid, Key, Value, 0).

set(Pid, Key, Value, Expires) when is_binary(Key) and is_binary(Value) ->
  gen_fsm:sync_send_event(Pid, {set, Key, Value, Expires}).

%% gen_fsm callbacks

init([Host, Port]) ->
  {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 0}]),
  {ok, ready, #state{socket=Socket}}.

ready({get, Key}, From, State) ->
  Packet = make_packet(get, Key),
  lager:debug("packet ~p", [Packet]),
  gen_tcp:send(State#state.socket, Packet),
  {next_state, waiting, State#state{waiter=From}};
ready({set, Key, Value, Expires}, From, State) ->
  Packet = make_packet(set, Key, Value, Expires),
  lager:debug("packet ~p", [Packet]),
  gen_tcp:send(State#state.socket, Packet),
  {next_state, waiting, State#state{waiter=From}};
ready({multiget, Keys}, From, State) ->
  Packets = make_multiget_packets(Keys),
  lager:debug("packets ~p", [Packets]),
  gen_tcp:send(State#state.socket, Packets),
  {next_state, waiting_for_multiget, State#state{waiter=From}}.

waiting({complete, Packets}, State = #state{socket=Sock,waiter=Waiter}) ->
  Response = memcached_proto:decode(State#state.framed ++ Packets),
  Reply = reply(Response, waiting),
  gen_fsm:reply(Waiter, Reply),
  {next_state, ready, #state{socket=Sock}}.

waiting_for_multiget({complete, Packets}, State) ->
  Decoded = memcached_proto:decode(State#state.framed ++ Packets),
  TotalDecoded = State#state.decoded ++ Decoded,
  Last = lists:last(Decoded),
  case Last#packet.op of
    getk ->
      Reply = reply(TotalDecoded, waiting_for_multiget),
      gen_fsm:reply(State#state.waiter, Reply),
      {next_state, ready, #state{socket=State#state.socket}};
    Op ->
      lager:debug("incomplete multiget. last op was ~p ~p.", [Op, Decoded]),
      NewState = State#state{incomplete= <<>>,framed=[],decoded=TotalDecoded},
      {next_state, waiting_for_multiget, NewState}
  end.

handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
  {reply, ok, StateName, State}.

handle_info({tcp,Sock,Message}, StateName, State) ->
  lager:debug("packet ~p", [Message]),
  #state{socket=Sock,incomplete=Incomplete,framed=Framed} = State,
  case memcached_proto:frame(<<Incomplete/binary, Message/binary>>) of
    {incomplete, Remaining, Packets} ->
      lager:debug("incomplete ~p ~p", [Remaining, Packets]),
      NewState = State#state{framed=Framed ++ Packets,incomplete=Remaining},
      {next_state, StateName, NewState};
    {complete, Packets} ->
      lager:debug("complete ~p ~p", [Packets, State]),
      ?MODULE:StateName({complete, Packets}, State)
  end;
handle_info({tcp_closed,_},_,State) ->
  lager:info("The connection closed on us. Exiting..."),
  {stop, normal, State};
handle_info(_Info, StateName, State) ->
  lager:debug("~p", [_Info]),
  {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
  lager:debug("terminating with reason ~p", [_Reason]),
  ok.

code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%% Internal functions.

make_header(#packet{op=Op,key=Key,extra=Extra,value=Value}) ->
  Opcode = memcached_proto:opcode(Op),
  KeyLength = size(Key),
  ExtraLength = size(Extra),
  TotalBody = KeyLength + ExtraLength + size(Value),
  <<?MAGIC_REQUEST/integer, Opcode:8/integer, KeyLength:16/integer,
    ExtraLength:8/integer, 0:8/integer, 0:16/integer, TotalBody:32/integer,
    0:32/integer, 0:64/integer>>.

make_packet(Command, Key) ->
  Header = make_header(#packet{op=Command,key=Key}),
  <<Header/binary, Key/binary>>.

make_packet(Command, Key, Value, Expires) ->
  Extra = <<16#deadbeef:32/integer, Expires:32/integer>>,
  Header = make_header(#packet{op=Command,key=Key,value=Value,extra=Extra}),
  <<Header/binary, Extra/binary, Key/binary, Value/binary>>.

make_multiget_packets(Keys) ->
  make_multiget_packets(Keys, <<>>).

make_multiget_packets([Key], Packets) ->
  Packet = make_packet(getk, Key),
  <<Packets/binary, Packet/binary>>;
make_multiget_packets([Key | Keys], Packets) ->
  Packet = make_packet(getkq, Key),
  make_multiget_packets(Keys, <<Packets/binary, Packet/binary>>).

reply(Packets, waiting_for_multiget) ->
  Filtered = lists:filter(fun(#packet{status=Status}) ->
	Status == ok
    end, Packets),
  [{P#packet.key, P#packet.value} || P <- Filtered];
reply([#packet{status=ok,op=set}], _) ->
  true;
reply([#packet{status=ok,value=Value}], _) ->
  Value;
reply([#packet{status=not_found}], _) ->
  undefined;
reply([#packet{status=Status,value=Value}], _) ->
  {Status, Value}.

