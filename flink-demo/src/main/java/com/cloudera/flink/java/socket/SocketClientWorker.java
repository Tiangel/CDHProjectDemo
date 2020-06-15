package com.cloudera.flink.java.socket;


import java.io.IOException;
import java.net.Socket;

/**
 * @description: 客户端工作线程
 */
public class SocketClientWorker implements Runnable {

    //通信socket
    private Socket clientSocket;
    //客户端名称
    private String clientName;
    //发送消息间隔
    private long sleepTime;
    //发送的消息内容
    private String message;

    public SocketClientWorker(String host, int port, String clientName, long sleepTime, String message) {
        try {
            clientSocket = new Socket(host, port);
            this.clientSocket = clientSocket;
            this.clientName = clientName;
            this.sleepTime = sleepTime;
            this.message = message;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        {
            while (true) {
                try {
                    clientSocket.getOutputStream().write(message.getBytes());
                    System.out.println(clientName + "客户端向服务器发送消息: " + message);
                    Thread.sleep(sleepTime);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        //服务端IP和端口 创建客户端服务端socket
        new Thread(new SocketClientWorker("127.0.0.1", 6666, "ClientA", 5000, "A发送消息。。")).start();
        new Thread(new SocketClientWorker("127.0.0.1", 6666, "ClientB", 4000, "B发送消息。。")).start();
        new Thread(new SocketClientWorker("127.0.0.1", 6666, "ClientC", 3000, "C发送消息。。")).start();
    }
}

