-module(memcached_pool).

-export([create/1,
	 with_connection/2]).

create(Server) ->
  memcached_sup:add_server(Server).

with_connection(Server, Fun) ->
  poolboy:transaction(list_to_atom(Server), Fun).
