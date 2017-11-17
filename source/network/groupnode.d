module network.groupnode;

import protocol.Msg;

import raft.Raft;
import raft.Rawnode;
import raft.Storage;
import raft.Node;
import std.conv;

import network.client;
import network.base;
import wal.kvstore;
import zhang2018.common.Log;
import zhang2018.common.Serialize;
import zhang2018.dreactor.event;
import zhang2018.dreactor.time.Timer;
import zhang2018.dreactor.aio.AsyncTcpServer;

class NodeInfo
{
	ulong 				ID;
	MemoryStorage 		storage;
	RawNode				node;
	bool				join;
	ulong				lastIndex;
	ConfState			confstate;
	ulong				snapshotIndex;
	ulong				appliedIndex;
	kvstore				store;

	this(ulong ID , MemoryStorage storage , RawNode node , kvstore store ,
		ulong lastIndex , ConfState confstate , ulong snapshotIndex , ulong appliedIndex)
	{
		this.ID = ID;
		this.storage = storage;
		this.node = node;
		this.store = store;
		this.lastIndex = lastIndex;
		this.confstate = confstate;
		this.snapshotIndex = snapshotIndex;
		this.appliedIndex = appliedIndex;
	}

	void publishSnapshot(Snapshot snap)
	{
		if(IsEmptySnap(snap))
			return;
		
		if(snap.Metadata.Index <= appliedIndex)
		{
			log_error(log_format("snapshot index [%d] should > progress.appliedIndex [%d] + 1", 
					snap.Metadata.Index, appliedIndex));
		}
		
		confstate = snap.Metadata.CS;
		snapshotIndex = snap.Metadata.Index;
		appliedIndex = snap.Metadata.Index;
	}

	Entry[] entriesToApply(Entry[] ents)
	{
		if(ents.length == 0)
			return null;
		
		auto firstIdx = ents[0].Index;
		if(firstIdx > appliedIndex + 1)
		{
			log_error(log_format("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1",
					firstIdx, appliedIndex));
		}
		
		if(appliedIndex - firstIdx + 1 < ents.length)
			return ents[appliedIndex - firstIdx + 1 .. $];
		
		return null;
	}

	bool publishEntries(Entry[] ents)
	{
		for(auto i = 0 ; i < ents.length ;i++)
		{
			switch(ents[i].Type)
			{
				case EntryType.EntryNormal:
					if(ents[i].Data.length == 0)
						break;
					
				//	RequestCommand command = deserialize!RequestCommand(cast(byte[])ents[i].Data);
					
				//	string value;
				//	if(command.Method == RequestMethod.METHOD_GET)
				//		value = store.Lookup(command.Key);
				//	else
				//		store.SetValue(command.Key , command.Value);
					
					//if leader
				/*	if(_node.isLeader())
					{
						auto http = (command.Hash in group._request);
						if(http != null)
						{
							http.do_response(value ~ " action done");
							http.close();
						}
					}*/
					
					
					break;
					//next
				case EntryType.EntryConfChange:
					ConfChange cc = deserialize!ConfChange(cast(byte[])ents[i].Data);
					confstate = node.ApplyConfChange(cc);


					switch(cc.Type)
					{
						case ConfChangeType.ConfChangeAddNode:
							if( cc.Context.length > 0)
							{	
								log_info("add node " , cc.NodeID);
							//	addPeer(cc.NodeID , cc.Context);
							}
							break;
						case ConfChangeType.ConfChangeRemoveNode:
							if(cc.NodeID == ID)
							{
								log_warning(ID , " I've been removed from the cluster! Shutting down.");
								return false;
							}
							log_warning(ID , " del node " , cc.NodeID);
							//delPeer(cc.NodeID);
							break;
						default:
							break;
					}
					break;
				default:
					
			}
			
			appliedIndex = ents[i].Index;
			
		}
		
		return true;
	}

	void maybeTriggerSnapshot()
	{
		if(appliedIndex - snapshotIndex <= defaultSnapCount)
			return;
		
		log_info(log_format("start snapshot [applied index: %d | last snapshot index: %d]",
				appliedIndex, snapshotIndex));
		
		auto data = store.getSnapshot();
		Snapshot snap;
		auto err = storage.CreateSnapshot(appliedIndex ,&confstate , cast(string)data , snap);
		if(err != ErrNil)
		{
			log_error(err);
		}
		
		store.savesnap(snap);

		long compactIndex = 1;
		if(appliedIndex > snapshotCatchUpEntriesN)
			compactIndex = appliedIndex - snapshotCatchUpEntriesN;
		
		storage.Compact(compactIndex);
		log_info("compacted log at index " , compactIndex);
		snapshotIndex = appliedIndex;
	}


}

class ClusterClient
{
	ulong 				firstID;
	string				host;
	ushort				port;
	string				apihost;
	ushort				apiport;
}


enum defaultSnapCount = 10;
enum snapshotCatchUpEntriesN = 10000;

class groupnode
{

	__gshared groupnode _ggroupnode;
	this()
	{
	
	}


	static groupnode instance()
	{
		if(_ggroupnode is null)
			_ggroupnode = new groupnode();
		return _ggroupnode;
	}


	//node1 [1,2,3]
	//node2 [1,2,4]
	//node3 [2,3,4]
	//node4 [1,2,3]




	void start(ulong firstID , ulong[][ulong] regions , ClusterClient[] clients)
	{
		_poll = new Epoll();
		_buffer = new byte[4096];

		foreach(c ; clients)
		{
			//server
			if(firstID == c.firstID)
			{
				_server = new AsyncTcpServer!(base ,ulong ,byte[])(_poll , firstID , _buffer);
				_server.open(c.host , c.port);
				log_info(firstID ,  " server open " , c.host , " " , c.port);
			}
			//client
			else
			{
				auto cli = new client(_poll , c.firstID , firstID);
				cli.open(c.host , c.port);
				log_info(firstID, " client connect node " , c.firstID , " " , c.host , " " , c.port );
				_clients[c.firstID] = cli;
			}
		}



		foreach(k , v ; regions)
		{
			Config conf = new Config();
			auto kvs = new kvstore();
			auto store = new MemoryStorage();
			auto ID = (firstID * 10 + k);
			Peer[] peers;
			foreach( id ; v)
			{
				Peer p = {ID: id * 10 +k};
				peers ~= p;
			}
			log_info(ID ," " , peers , v);

			

			Snapshot *shot = null;

			ConfState confState;
			ulong snapshotIndex;
			ulong appliedIndex;
			ulong lastIndex;
			RawNode	node;


			HardState hs;
			Entry[] ents;
			bool exist = kvs.load("snap.log" ~ to!string(ID) , "entry.log" ~ to!string(ID) , shot , hs , ents);
			if(shot != null)
			{
				store.ApplySnapshot(*shot);
				confState = shot.Metadata.CS;
				snapshotIndex = shot.Metadata.Index;
				appliedIndex = shot.Metadata.Index;
			}

			store.setHadrdState(hs);
			store.Append(ents);

			if(ents.length > 0)
			{
				lastIndex = ents[$ - 1].Index;
			}
			conf._ID 		   	= ID;
			conf._ElectionTick 	= 10;
			conf._HeartbeatTick = 1;
			conf._storage		= store;
			conf._MaxSizePerMsg = 1024 * 1024;
			conf._MaxInflightMsgs = 256;

			if(exist)
			{
				node = new RawNode(conf);
			}
			else
			{
				node = new RawNode(conf , peers);
			}

			_groupraft[ID] = new NodeInfo(ID , store , node , kvs , lastIndex ,confState , snapshotIndex , appliedIndex);
		
		}

		log_info(_clients , _groupraft);

		_poll.addFunc(&ready);
		_poll.addTimer(&onTimer , 100 , WheelType.WHEEL_PERIODIC);
		_poll.start();
		_poll.wait();

	}


	void onTimer(TimerFd fd)
	{
		foreach( r ; _groupraft)
		{
			r.node.Tick();
		}

	}

	void ready()
	{
	
		foreach(r ; _groupraft)
		{


			Ready rd = r.node.ready();
			if(!rd.containsUpdates())
			{
				continue;
			}

			r.store.save(rd.hs , rd.Entries);
			if(!IsEmptySnap(rd.snap))
			{
				r.store.savesnap(rd.snap);
				r.storage.ApplySnapshot(rd.snap);
				r.publishSnapshot(rd.snap);
			}

			r.storage.Append(rd.Entries);
			send(rd.Messages);
			if(!r.publishEntries( r.entriesToApply(rd.CommittedEntries)))
			{
				log_error(" stop " , r.ID);
				continue;
			}
			r.maybeTriggerSnapshot();
			r.node.Advance(rd);

		}
	}

	void send(Message[] msg)
	{
		foreach(m ; msg)
		{
			ulong ID = m.To / 10;
			_clients[ID].send(m);
		}
	}

	void Step(Message msg)
	{

		_groupraft[msg.To].node.Step(msg);
	}



	byte[]									_buffer;
	Poll									_poll;
	AsyncTcpServer!(base,ulong ,byte[])		_server;
	client[ulong]							_clients;
	NodeInfo[ulong]							_groupraft;
//	http[ulong]								_request;

}

