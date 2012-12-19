-module(memcached_ring).

-export([
    create/1,
    get/2
  ]).

-define(RING_SIZE, 1000).

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
add(Remaining = <<Byte:1/binary, Bytes/binary>>, Server, Node) ->
  case Node of
    {B, L, R, C, V} when Byte < B ->
      {B, add(Remaining, Server, L), R, C, V};
    {B, L, R, C, V} when Byte > B ->
      {B, L, add(Remaining, Server, R), C, V};
    undefined when Bytes == <<>> ->
      {Byte, undefined, undefined, undefined, Server};
    undefined ->
      {Byte, undefined, undefined, add(Bytes, Server, undefined), undefined};
    {B, L, R, C, V} ->
      {B, L, R, add(Remaining, Server, C), V}
  end.


get(Key, Ring) ->
  <<Int:40/integer, _/binary>> = crypto:sha(Key),
  Hash = <<Int/integer>>,
  case find_nearest(Hash, Ring) of
    undefined ->
      smallest(Ring);
    Node ->
      Node
  end.

find_nearest(_, undefined) ->
  undefined;
find_nearest(<<>>, Node) ->
  next_value(Node);
find_nearest(Remaining = <<Byte:1/binary, Bytes/binary>>, Node = {B, L, R, C, _}) ->
  case Byte of
    Byte when Byte == B andalso Remaining == <<>> ->
      next_value(Node);
    Byte when Byte == B ->
      find_nearest(Bytes, C);
    Byte when Byte < B ->
      find_nearest(Remaining, L);
    Byte when Byte > B ->
      find_nearest(Remaining, R)
  end.

next_value({_, _, _, C, undefined}) ->
  next_value(C);
next_value({_, _, _, _, V}) ->
  V.

smallest({_, undefined, _, C, _}) ->
  next_value(C);
smallest({_, L, _, _, _}) ->
  smallest(L).
