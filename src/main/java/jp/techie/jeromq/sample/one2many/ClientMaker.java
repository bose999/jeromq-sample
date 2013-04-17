package jp.techie.jeromq.sample.one2many;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.msgpack.MessagePack;
import org.msgpack.type.RawValue;
import org.msgpack.type.Value;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * JeroMQ Sample Client
 * @author bose999
 * 
 */
public class ClientMaker {
	
	/**
	 * Clientの生成とキューのやり取りをする
	 * ファイルの擬似ストリーム転送試行バージョン
	 * @param args none
	 */
	public static void main(String[] args) {
		Context context = ZMQ.context(1);
		Socket request = context.socket(ZMQ.REQ);
		String requestAddress = "tcp://127.0.0.1:9001";
		request.connect(requestAddress);
		System.out.println("client - requestAddress:" + requestAddress);
		for (int i = 0; i < 1; i++) {
			long startTime = System.nanoTime();
			MessagePack msgpack = new MessagePack();
			String sendString = "Hello ( requestAddress: " + requestAddress + ")";
			byte[] sendStringBytes;
			try {
				// MessagePackでシリアライズして送る
				sendStringBytes = msgpack.write(sendString);
				request.send(sendStringBytes);
				long endTime = System.nanoTime();
				String outputFilePath = "/Users/matakeda/Desktop/001-01.jpg";
				FileSystem fileSystem = FileSystems.getDefault();
				Path path = fileSystem.getPath(outputFilePath);
				OutputStream outputStream = Files.newOutputStream(path, CREATE, APPEND);
				
				while (!Thread.currentThread().isInterrupted()) {
					if (request.getEvents() == 2) {
						System.out.println(request.getEvents());
						break;
					}
					if (request.getEvents() == 1) {
						System.out.println(request.getEvents());
						// MessagePackで返答を文字列にデシリアライズ
						byte[] replyBytes = request.recv();
						System.out.println(replyBytes);
						
						Value valueReply = msgpack.read(replyBytes, Value.class);
						if (valueReply == null || valueReply.isNilValue()) {
							outputStream.close();
							System.out.println("close");
						} else {
							RawValue rawValue = (RawValue) valueReply;
							byte[] fileBytes = rawValue.getByteArray();
							outputStream.write(fileBytes);
							System.out.println("write");
						}
						long executeTime = endTime - startTime;
						System.out.println("Received reply ( requestAddress:" + requestAddress + ") forCount:" + i
								+ " [" + "reply" + "] " + executeTime + "ns");
					}
					
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		request.close();
		context.term();
	}
}
