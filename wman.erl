-module (wman).
-export ([start/0, require/1, release/2, start_workers/2]).

start() ->
	spawn(fun() -> worker_manager_run(queue:new()) end).

require(Manager) ->
	Manager ! {self(), require},
	receive
		{value, Worker} -> Worker;
		empty	-> empty
	end.

release(Manager, Worker) ->
	Manager ! {self(), release, Worker}.

start_workers(Manager, List) -> 
	Manager ! {self(), start, List}.

worker_manager_run(Idle_queue) ->
	receive
		{Pid, require} ->
			io:format("~p~n", [Idle_queue]),
			{Res, Q} = queue:out(Idle_queue),
			Pid ! {value, Res},
			worker_manager_run(Q);
		{_, release, Worker} ->
			Q = queue:in(Worker, Idle_queue),
			worker_manager_run(Q);
		{_, start, L} when is_list(L) ->
			S = self(),
			spawn(fun() -> start_workers_by_list(S, L) end),
			worker_manager_run(Idle_queue);
		_ -> 
			worker_manager_run(Idle_queue)
	end.
	
start_workers_by_list(Manager, L) ->
	lists:map(
		fun(Item) ->
			spawn(fun() -> start_workers_in_node(Item, Manager) end)
		end, L).

start_workers_in_node({_, 0}, _) -> ok;
start_workers_in_node({Node, Num}, Manager) ->
	Worker = rpc:call(Node, worker, start, [Manager]),
	release(Manager, Worker),
	io:format("~p~n", [Worker]),
	start_workers_in_node({Node, Num-1}, Manager).

