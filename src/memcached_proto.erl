-module(memcached_proto).

-export([frame/1,
	 decode/1,
	 make_packet/2,
	 make_packet/4,
	 make_multiget_packets/1,
	 op/1,
	 opcode/1,
	 status/1]).

-define(MAGIC_REQUEST, 16#80).
-define(MAGIC_RESPONSE, 16#81).

-include("include/memcached.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

frame(Packets) ->
  frame(Packets, []).

frame(Packets, Framed) when size(Packets) >= 24 ->
  <<Header:24/binary, _/binary>> = Packets,
  <<?MAGIC_RESPONSE/integer, _:8/integer, _:16/integer,
    _:8/integer, _:8/integer, _:16/integer,
    BodyLength:32/integer, _:32/integer, _:64/integer,
    BodyAndRemainder/binary>> = Packets,
  case size(BodyAndRemainder) of
    RemainderSize when RemainderSize < BodyLength ->
      {incomplete, Packets, lists:reverse(Framed)};
    RemainderSize when RemainderSize > BodyLength ->
      <<Body:BodyLength/binary, Remainder/binary>> = BodyAndRemainder,
      frame(Remainder, [<<Header/binary,Body/binary>> | Framed]);
    BodyLength ->
      AllFramed = [<<Header/binary,BodyAndRemainder/binary>> | Framed],
      {complete, lists:reverse(AllFramed)}
  end;
frame(Packets, Framed) ->
  {incomplete, Packets, Framed}.

decode(Packet) ->
  decode(Packet, []).

decode([], Packets) ->
  lists:reverse(Packets);
decode([ThisPacket | Remainder], Packets) ->
  <<?MAGIC_RESPONSE/integer, Opcode:8/integer, KeyLength:16/integer,
    ExtraLength:8/integer, _:8/integer, StatusCode:16/integer,
    BodyLength:32/integer, _:32/integer, _:64/integer,
    Body/binary>> = ThisPacket,
  ValueLength = BodyLength - KeyLength - ExtraLength,
  ExtraBits = ExtraLength * 8,
  <<Extra:ExtraBits/integer, Key:KeyLength/binary,
    Value:ValueLength/binary>> = Body,
  decode(Remainder, [#packet{
    op = op(Opcode),
    extra = Extra,
    status = status(StatusCode),
    key = Key,
    value = Value
  } | Packets]).

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


opcode(get) ->
  16#00;
opcode(set) ->
  16#01;
opcode(getk) ->
  16#0C;
opcode(getkq) ->
  16#0D.

op(16#00) ->
  get;
op(16#01) ->
  set;
op(16#0C) ->
  getk;
op(16#0D) ->
  getkq.

status(16#0000) ->
  ok;
status(16#0001) ->
  not_found;
status(16#0002) ->
  exists;
status(16#0003) ->
  too_large;
status(16#0004) ->
  invalid_arguments;
status(16#0005) ->
  not_stored;
status(16#0006) ->
  non_numeric.

-ifdef(TEST).

frame_test() ->
  Message = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>,
  ?assertEqual({complete, [Message]}, frame(Message)).

incomplete_header_frame_test() ->
  FirstPacket = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>,
  Message = <<FirstPacket/binary, 129>>,
  ?assertEqual({incomplete, <<129>>, [FirstPacket]}, frame(Message)).

incomplete_body_frame_test() ->
  FirstPacket = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>,
  Remainder = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10>>,
  Message = <<FirstPacket/binary, Remainder/binary>>,
  ?assertEqual({incomplete, Remainder, [FirstPacket]}, frame(Message)).

multiple_packets_frame_test() ->
  FirstPacket = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>,
  SecondPacket = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>,
  Message = <<FirstPacket/binary, SecondPacket/binary>>,
  Result = frame(Message),
  ?assertEqual({complete, [FirstPacket, SecondPacket]}, Result).

decode_error_test() ->
  Packets = [<<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>],
  [Result] = decode(Packets),
  ?assertEqual(ok, Result#packet.status),
  ?assertEqual(get, Result#packet.op),
  ?assertEqual(<<>>, Result#packet.key),
  ?assertEqual(<<"bsdf">>, Result#packet.value),
  ?assertEqual(16#deadbeef, Result#packet.extra).

-endif.
