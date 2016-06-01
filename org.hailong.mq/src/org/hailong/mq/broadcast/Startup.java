package org.hailong.mq.broadcast;

import java.io.IOException;
import java.util.regex.Pattern;
import org.hailong.mq.MQ;
import org.hailong.mq.Message;
import org.hailong.mq.Recipient;
import org.hailong.mq.broadcast.RemoteService;
import org.hailong.mq.tcp.TCPClient;
import org.hailong.mq.tcp.TCPServer;

public final class Startup {

	public static void main(String[] args) throws Exception {
		
		int port = 8800;
		String remoteAddress = null;
		
		for(int i=0;i<args.length;i++) {
		
			if("-p".equals(args[i]) && i +1 <args.length) {
				port = Integer.valueOf(args[i + 1]);
				i ++;
			}
			else if("-remote".equals(args[i]) && i + 1 < args.length) {
				remoteAddress = args[i + 1];
				i ++;
			}
		}

		TCPServer server = new TCPServer(port);
		
		BroadcastService bs = new BroadcastService();
		
		server.on(Pattern.compile("^broadcast\\."), bs);
		server.on(Pattern.compile("^disconnect$"), bs);
		server.on(Pattern.compile("^connect$"), bs);
		
		server.on(Pattern.compile("^connect$"), new Recipient() {
			
			@Override
			public void onMessage(Message message, MQ mq, MQ from) {
				
				System.out.println(new String(message.bytes,Message.charset));
				
			}
		});
		
		TCPClient remote = null;
		RemoteService remoteService = null;
		
		if(remoteAddress != null) {
			
			{
			
				String[] vs = remoteAddress.split(":");
				
				if(vs.length > 1) {
					remote = new TCPClient(vs[0],Integer.valueOf(vs[1]));
				}
				else {
					remote = new TCPClient(vs[0],8800);
				}
				
				remoteService = new RemoteService(remote);
				
				server.on(Pattern.compile("^broadcast\\."), remoteService);
				
				System.out.println("Remote Connect : " + remoteAddress);
				
			}
			
		}

		while(true) {
			
			if(remote != null) {
				
				try {
					remote.select(300);
				}
				catch(IOException ex) {
					
					remote.destory();
					
					server.off(null, remoteService);
					
					System.out.println("Remote Disconnect : " + remoteAddress);
					
					String[] vs = remoteAddress.split(":");
					
					if(vs.length > 1) {
						remote = new TCPClient(vs[0],Integer.valueOf(vs[1]));
					}
					else {
						remote = new TCPClient(vs[0],8800);
					}
					
					remoteService = new RemoteService(remote);
					
					server.on(Pattern.compile("^broadcast\\."), remoteService);
					
					System.out.println("Remote Connect : " + remoteAddress);
					
				}
 			}
			
			server.select(300);

		}
		
	}

}
