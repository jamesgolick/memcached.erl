-module(memcached_ring).

-export([
    create/1,
    add/3,
    smallest/1,
    find_nearest/2,
    get/2
  ]).

-define(RING_SIZE, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

create(Servers) ->
  create(Servers, undefined).

create([], Ring) ->
  Ring;
create([Server | Servers], Ring) ->
  ServerBinary = list_to_binary(Server),
  NewRing = lists:foldl(fun(I, InnerRing) ->
	IString = list_to_binary(integer_to_list(I)),
	Key = <<ServerBinary/binary, "-", IString/binary>>,
	<<Hash1:40/integer, Hash2:40/integer,
	  Hash3:40/integer, Hash4:40/integer>> = crypto:sha(Key),
	add(Hash1, Server,
	  add(Hash2, Server,
	    add(Hash3, Server,
	      add(Hash4, Server, InnerRing))))
      end, Ring, lists:seq(0, 39)),
  create(Servers, NewRing).

add(R, Server, Node) when is_integer(R) ->
  add(<<R:40/integer>>, Server, Node);
add(Remaining = <<Bit:1/bits, Bits/bits>>, Server, Node) ->
  case Node of
    {B, L, R, C, V} when Bit < B ->
      {B, add(Remaining, Server, L), R, C, V};
    {B, L, R, C, V} when Bit > B ->
      {B, L, add(Remaining, Server, R), C, V};
    undefined when Bits == <<>> ->
      {Bit, undefined, undefined, undefined, Server};
    undefined ->
      {Bit, undefined, undefined, add(Bits, Server, undefined), undefined};
    {B, L, R, C, V} when Bit == B ->
      {B, L, R, add(Bits, Server, C), V}
  end.


get(Key, Ring) ->
  <<Hash:5/binary, _/binary>> = crypto:sha(Key),
  case find_nearest(Hash, Ring) of
    undefined ->
      smallest(Ring);
    Node ->
      Node
  end.

find_nearest(_, undefined) ->
  undefined;
find_nearest(Remaining = <<Bit:1/bits, Bits/bits>>, Node = {B, L, R, C, V}) ->
  case Bit of
    Bit when Bit == B andalso V =/= undefined andalso Bits == <<>> ->
      V;
    Bit when Bit == B ->
      case find_nearest(Bits, C) of
	undefined ->
	  smallest(R);
	RS ->
	  RS
      end;
    Bit when Bit < B ->
      case find_nearest(Remaining, L) of
	undefined ->
	  smallest(Node);
	RS ->
	  RS
      end;
    Bit when Bit > B ->
      find_nearest(Remaining, R)
  end.

smallest(undefined) ->
  undefined;
smallest({_, L, _, _, undefined}) when L =/= undefined ->
  smallest(L);
smallest({_, _, _, _, V}) when V =/= undefined ->
  V;
smallest({_, _, _, C, _}) when C =/= undefined ->
  smallest(C).


-ifdef(TEST).

smallest_test() ->
  Tree = add(<<5:8/integer>>, <<"five">>,
	  add(<<10:8/integer>>, <<"ten">>,
	    add(<<1:8/integer>>, <<"one">>, undefined))),
  ?assertEqual(<<"one">>, smallest(Tree)),
  Tree2 = add(<<5:8/integer>>, <<"five">>,
	  add(<<10:8/integer>>, <<"ten">>,
	    add(<<7:8/integer>>, <<"seven">>, undefined))),
  ?assertEqual(<<"five">>, smallest(Tree2)),
  Tree3 = add(<<1:8/integer>>, <<"one">>,
	    add(<<2000:8/integer>>, <<"twothousand">>, undefined)),
  ?assertEqual(<<"one">>, smallest(Tree3)).

find_nearest_test() ->
  Tree = add(<<5:8/integer>>, <<"five">>,
	  add(<<10:8/integer>>, <<"ten">>,
	    add(<<1:8/integer>>, <<"one">>, undefined))),

  ?assertEqual(<<"one">>, find_nearest(<<1:8/integer>>, Tree)),
  ?assertEqual(<<"one">>, find_nearest(<<0:8/integer>>, Tree)),
  ?assertEqual(<<"five">>, find_nearest(<<2:8/integer>>, Tree)),
  ?assertEqual(<<"five">>, find_nearest(<<3:8/integer>>, Tree)),
  ?assertEqual(<<"five">>, find_nearest(<<4:8/integer>>, Tree)),
  ?assertEqual(<<"five">>, find_nearest(<<5:8/integer>>, Tree)),
  ?assertEqual(<<"ten">>, find_nearest(<<6:8/integer>>, Tree)),
  ?assertEqual(<<"ten">>, find_nearest(<<7:8/integer>>, Tree)),
  ?assertEqual(<<"ten">>, find_nearest(<<8:8/integer>>, Tree)),
  ?assertEqual(<<"ten">>, find_nearest(<<9:8/integer>>, Tree)),
  ?assertEqual(<<"ten">>, find_nearest(<<10:8/integer>>, Tree)),
  ?assertEqual(undefined, find_nearest(<<11:8/integer>>, Tree)).

get_test() ->
  Ring = create(["localhost:11211", "localhost:22122"]),
  memcached_ring:get(<<"asdf">>, Ring).

-endif.
