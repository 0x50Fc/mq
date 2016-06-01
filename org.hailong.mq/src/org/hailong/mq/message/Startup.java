package org.hailong.mq.message;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.hailong.mq.broadcast.RemoteService;
import org.hailong.mq.tcp.TCPClient;
import org.hailong.mq.tcp.TCPServer;

public final class Startup {

	private static ClassLoader loader;

	public static void main(String[] args) throws Exception {
		
		List<URL> libs = new ArrayList<URL>(4);

		File dir = new File("./lib");
		
		if(dir.exists() && dir.isDirectory()) {
			
			for(File f : dir.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".jar");
				}
			})){
				libs.add(f.toURI().toURL());
			}
		}
		
		loader = new URLClassLoader(libs.toArray(new URL[libs.size()]));
		
		MessageDB db = null;
		int port = 8800;
		String remoteAddress = null;
		
		for(int i=0;i<args.length;i++) {
		
			if("-db".equals(args[i]) && i + 1< args.length) {
				
				db = (MessageDB) loader.loadClass(args[i + 1]).newInstance();
				
				i ++;
			}
			else if("-p".equals(args[i]) && i +1 <args.length) {
				port = Integer.valueOf(args[i + 1]);
				i ++;
			}
			else if("-remote".equals(args[i]) && i + 1 < args.length) {
				remoteAddress = args[i + 1];
				i ++;
			}
		}
		
		if(db == null) {
			throw new Exception("未找到消息数据库");
		}

		TCPServer server = new TCPServer(port);
		
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
		
		MessageService ms = new MessageService(db);
		
		server.on(Pattern.compile("^message\\."), ms);
		server.on(Pattern.compile("^broadcast\\.message\\."), ms);
		server.on(Pattern.compile("^disconnect$"), ms);
		
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
