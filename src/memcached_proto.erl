-module(memcached_proto).

-export([frame_packets/2]).

-define(MAGIC_RESPONSE, 16#81).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

frame_packets(Packets, Framed) ->
  case size(Packets) of
    Size when Size >= 24 ->
      <<Header:24/binary, _/binary>> = Packets,
      <<?MAGIC_RESPONSE/integer, _:8/integer, _:16/integer,
	_:8/integer, _:8/integer, _:16/integer,
	BodyLength:32/integer, _:32/integer, _:64/integer,
	BodyAndRemainder/binary>> = Packets,
      case size(BodyAndRemainder) of
	RemainderSize when RemainderSize < BodyLength ->
	  {incomplete, Packets, Framed};
	RemainderSize when RemainderSize > BodyLength ->
	  <<Body:BodyLength/binary, Remainder/binary>> = BodyAndRemainder,
	  frame_packets(Remainder, [<<Header/binary,Body/binary>> | Framed]);
	BodyLength ->
	  AllFramed = [<<Header/binary,BodyAndRemainder/binary>> | Framed],
	  {complete, lists:reverse(AllFramed)}
      end;
    _ ->
      {incomplete, Packets, Framed}
  end.

-ifdef(TEST).

frame_packets_test() ->
  Message = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>,
  ?assertEqual({complete, [Message]}, frame_packets(Message, [])).

incomplete_frame_packets_test() ->
  FirstPacket = <<129,0,0,0,4,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,10,222,173,190,239,98,115,100,102>>,
  Message = <<FirstPacket/binary, 129>>,
  ?assertEqual({incomplete, <<129>>, [FirstPacket]}, frame_packets(Message, [])).

-endif.
