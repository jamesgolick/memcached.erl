-module(memcached_pool).

-export([create/1,
	 with_connection/2]).

create(Server) ->
  pooler:new_pool(get_pool_options(Server)).

get_pool_options(Server) ->
  UserOptions = case application:get_env(memcached, pool_options) of
    {ok, Options} ->
      Options;
    _ ->
      []
  end,
  DefaultOptions = [{name, list_to_atom(Server)},
		    {group, memcached},
		    {max_count, 5},
		    {init_count, 2},
		    {start_mfa,
		      {memcached_conn,
		       start_link, [Server]}}],
  UserOptions ++ DefaultOptions.

with_connection(Server, Fun) ->
  transaction(list_to_atom(Server), fun(Conn) ->
	{status, _, _, [_,_,_,_,[_,{data,[_,_,_,{"StateName", State}]},_]]} = sys:get_status(Conn),
	case State of
	  ready ->
	    Fun(Conn);
	  SomethingElse ->
	    lager:error("Checked out a connection with state ~p.", SomethingElse),
	    exit(Conn, normal),
	    with_connection(Server, Fun)
	end
    end).

transaction(Server, Fun) ->
  PoolID = Server,
  Conn = pooler:take_member(PoolID),
  case catch Fun(Conn) of
    Error = {'EXIT', _} ->
      pooler:return_member(PoolID, Conn, Error),
      throw(Error);
    Value ->
      pooler:return_member(PoolID, Conn, ok),
      Value
  end.

