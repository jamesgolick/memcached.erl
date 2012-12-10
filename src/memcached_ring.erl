-module(memcached_ring).

-export([
    create/1,
    get/2
  ]).

-define(RING_SIZE, 1000).

create([Server | Servers]) ->
  create(Servers, memcached_chash:fresh(?RING_SIZE, Server)).

create([], Ring) ->
  Ring;
create([Server | Servers], Ring) ->
  OtherRing = memcached_chash:fresh(?RING_SIZE, Server),
  create(Servers, memcached_chash:merge_rings(Ring, OtherRing)).

get(Key, Ring) ->
  KeyHash = memcached_chash:key_of(Key),
  [{_Hash, Node}] = memcached_chash:successors(KeyHash, Ring, 1),
  Node.
