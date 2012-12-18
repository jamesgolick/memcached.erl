-module(memcached_proto).

-export([frame/1]).

-define(MAGIC_RESPONSE, 16#81).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

frame(Packets) ->
  frame(Packets, []).

frame(Packets, Framed) ->
  case size(Packets) of
    Size when Size >= 24 ->
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
    _ ->
      {incomplete, Packets, Framed}
  end.


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

-endif.
