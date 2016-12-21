-module(emqtt).
-export([start/0,parser/2,publishLoop/3,timestamp/0]).

%%====================================================================
%% API functions
%%====================================================================

parser(Payload,Key) -> 
	DecodedToMap = jsx:decode(Payload, [return_maps]),
	GetValue = maps:get(Key, DecodedToMap),
	GetValue.

publishLoop([],C,Newtopic) -> ok;
publishLoop([X|Xs],C,Newtopic) -> 
		V = [<<"name">>,<<"message">>,<<"timestamp">>],
		L = lists:zip(V,X),
		Encode = jsx:encode(L),
		emqttc:publish(C, Newtopic,Encode),
		publishLoop(Xs,C,Newtopic).

%%https://gist.github.com/DimitryDushkin/5532071
timestamp() ->
	{Mega, Sec, Micro} = os:timestamp(),
	(Mega*1000000 + Sec)*1000 + round(Micro/1000). 

	
start() -> 
	
	spawn(fun () -> init() 
	end).

init() ->
	{ok, C} = emqttc:start_link([{host, "prata.technocreatives.com"},{client_id, <<"leCobra">>}]),
	emqttc:subscribe(C, <<"ConnectingSpot/Chatroom/#">>, 1),
	emqttc:subscribe(C, <<"ConnectingSpot/Database/#">>, 1),
	{ok, Db} = dbclient_app:start(),
	loop({C, Db}).
	%mysql:query(Db, <<"SELECT * FROM (SELECT * FROM room ORDER BY idroom DESC LIMIT 10) sub ORDER BY idroom ASC">>, [Variable]),
	
loop({C,Db}) ->
	receive
	
		{publish, Topic, Payload} when Topic == <<"ConnectingSpot/Database/select">>  ->
		
			Newtopic = parser(Payload,<<"id">>),
			Variable = parser(Payload,<<"room">>),
			{ok,GH,D} = mysql:query(Db, <<"SELECT user, message, time FROM room where roomName = ?">>, [Variable]),
			publishLoop(D,C,Newtopic),
			Encode = jsx:encode([{<<"name">>,<<"ConnectingSpot">>},{<<"message">>,<<"END OF THE CHAT HISTORY">>},{<<"timestamp">>,timestamp()}]),
			emqttc:publish(C, Newtopic,Encode),
		
			loop({C,Db});
				
		{publish, Topic, Payload}  -> 
	
			Nick = parser(Payload,<<"name">>),
			Tim = parser(Payload,<<"timestamp">>),
			Messag = parser(Payload,<<"message">>),
			
			mysql:query(Db, "INSERT INTO room (user, message,time,roomName) VALUES (?,?,?,?)", [Nick, Messag, Tim, Topic]),
		
		
			loop({C,Db});
 	
	  
		unknown ->
		
			loop({C,Db})
	end.
					
