-module(memcached_pool).

-export([create/1,
	 with_connection/2]).

create(Server) ->
  memcached_sup:add_server(Server).

with_connection(Server, Fun) ->
  poolboy:transaction(list_to_atom(Server), fun(Conn) ->
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
