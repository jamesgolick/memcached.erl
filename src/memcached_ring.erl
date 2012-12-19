-module(memcached_ring).

-export([
    create/1,
    get/2
  ]).

-define(RING_SIZE, 1000).

create(Servers) ->
  create(Servers, gb_trees:empty()).

create([], Ring) ->
  Ring;
create([Server | Servers], Ring) ->
  ServerBinary = list_to_binary(Server),
  NewRing = lists:foldl(fun(I, InnerRing) ->
	IString = list_to_binary(integer_to_list(I)),
	Key = <<ServerBinary/binary, "-", IString/binary>>,
	<<Hash1:40/integer, Hash2:40/integer,
	  Hash3:40/integer, Hash4:40/integer>> = crypto:sha(Key),
	gb_trees:enter(Hash1, Server,
	  gb_trees:enter(Hash2, Server,
	    gb_trees:enter(Hash3, Server,
	      gb_trees:enter(Hash4, Server, InnerRing))))
      end, Ring, lists:seq(0, 39)),
  create(Servers, NewRing).

get(Key, {_, Tree}) ->
  <<Hash:40/integer, _/binary>> = crypto:sha(Key),
  {_, Node} = find_nearest(Hash, Tree, {0, 0}),
  Node.

find_nearest(Hash, {Key, Value, Smaller, Bigger}, {RK, RV}) ->
  Result2 = case Key of
    Key when RK == 0 ->
      {Key, Value};
    Key when Key > Hash andalso Key < RK ->
      {Key, Value};
    _ ->
      {RK, RV}
  end,
  {RK2, RV2} = Result2,
  {SK, SV} = case Smaller of
    nil ->
      Result2;
    {SmallerKey, _, _, _} when SmallerKey > RK2 ->
      Result2;
    _ ->
      find_nearest(Hash, Smaller, Result2)
  end,
  {BK, BV} = case Bigger of
    nil ->
      Result2;
    {BiggerKey, _, _, _} when BiggerKey > RK2 ->
      Result2;
    _ ->
      find_nearest(Hash, Bigger, Result2)
  end,
  case SK of
    SK when SK > Key ->
      {Key, Value};
    SK when SK < BK ->
      {SK, SV};
    _ ->
      {BK, BV}
  end.
