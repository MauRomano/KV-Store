package SisDis;

import com.google.gson.Gson;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Random;

public class Cliente {
    static KeyValueAccess kv = new KeyValueAccess();
    static int innerPort;



    public static void main(String[] args) throws IOException {
        BufferedReader inInnerPort = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Qual a porta deste cliente? ");
        innerPort = Integer.parseInt(inInnerPort.readLine());
        ServerSocket serverSocket = new ServerSocket(innerPort);
        Thread th = new ThreadClient();
        th.start();
        while (true){
            System.out.println("Esperando conn");
            Socket no = serverSocket.accept(); //BLOCKING
            System.out.println("Conn aceita");
            Thread th2 = new ThreadClientServer(no);
            th2.start();
        }
    }
}

class ThreadClient extends Thread {
    BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
    Server[] servers = new Server[3];
    Server s;
    Mensagem mensagem = new Mensagem();
    Mensagem msgAux = new Mensagem();
    long marcoZero = System.currentTimeMillis();
    Random rnd = new Random();
    Gson gson = new Gson();

    public void run() {
        try {


            System.out.println("Olá." +
                    "\nPor favor, selecione (digite) uma das opções abaixo" +
                    "\nINIT (0) - inicialize este cliente, insrindo ip e portas dos servidores" +
                    "\nPUT (1) - insira um atributo chave-valor no servidor" +
                    "\nGET (2) - recupere um valor através de uma chave " +
                    "\n ");

            while (true) {
                String cmd = null;
                try {
                    cmd = inFromUser.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                switch (cmd) {
                    case "INIT":
                    case "0":
                        System.out.println("Inicializando sistema");
                        for (int i = 0; i < 1; i++) { //alterar para 3 servidores
                            try {
                                String ip = "127.0.0.1";
                                int porta = 10098;
                                servers[i] = new Server(ip, porta);
                            } catch (Exception e) {
                                System.out.printf("Server %d não inicializado.", i + 1);
                                System.out.println(e.getMessage());
                                i = i - 1;
                            }
                        }
                        break;
                    case "PUT":
                    case "1":
                        System.out.println("Inserindo valor.");
                        System.out.println("Insira a chave:");
                        mensagem.key = inFromUser.readLine();
                        System.out.println("Insira o valor:");
                        mensagem.value = inFromUser.readLine();
                        mensagem.method = "PUT";
                        mensagem.ts = System.currentTimeMillis() - marcoZero;
                        mensagem.portFrom = Cliente.innerPort;
                        //s = servers[rnd.nextInt(servers.length)];
                        s = servers[0];
                        s.enviaMenssagem(mensagem);

                        break;
                    case "GET":
                    case "2":
                        System.out.println("Recuperando valor");
                        System.out.println("Insira a chave:");
                        mensagem.key = inFromUser.readLine();
                        mensagem.method = "GET";
                        mensagem.ts = System.currentTimeMillis() - marcoZero;
                        s = servers[0];
                        msgAux = gson.fromJson(s.getValue(mensagem), Mensagem.class);
                        if (! msgAux.equals("WAIT_FOR_RESPONSE")){
                            putThread(msgAux);
                        }
                        break;
                    case "3":
                        System.out.println("Printing all elements of hashmap");
                        for (String name : Cliente.kv.kv.keySet()) {
                            System.out.println("Key: " + name + ", Value: " + Cliente.kv.kv.get(name).value + ", TimeStamp: " + Cliente.kv.kv.get(name).ts);
                        }
                        break;
                    case "4":
                        mensagem.method = "PRINTALL";
                        for (int i=10097; i<10100;i++) {
                            Socket s = new Socket("127.0.0.1", i);
                            OutputStream os = s.getOutputStream();
                            DataOutputStream writer = new DataOutputStream(os);

                            writer.writeBytes(gson.toJson(mensagem) + "\n");
                            s.close();
                        }
                    //System.out.println("Select server to print elements from: ");
                    //int portCmd = Integer.parseInt(inFromUser.readLine());
                        break;

                    default:
                        System.out.println("Comando não encontrado");
                }
            }
        } catch (Exception e){
            System.out.println("Error on thread.run: "+e.getMessage());
        }
    }
    private void putThread(Mensagem msg){
        boolean found = false;
        MapHelper mhAux;

        for (String name : Cliente.kv.kv.keySet()) {
            if (name.equals(msg.key)){
                found = true;
                break;
            }
        }
        mhAux = new MapHelper(msg.value, msg.ts);
        if (found){
            Cliente.kv.kv.replace(msg.key, mhAux);
        }
        else {
            Cliente.kv.kv.put(msg.key, mhAux);
        }
    }
}


class ThreadClientServer extends Thread{
    static long marcoZero = System.currentTimeMillis();
    static int port = 10101;
    static String ip = "127.0.0.1";
    static int[] ports = {10097,10098, 10099};
    static Mensagem m;
    public static MapHelper mh;
    public static String key;
    Gson gson = new Gson();
    Socket no;
    ServerSocket serverSocket;

    public ThreadClientServer(Socket node){
        no = node;
    }

    public void run() {
        try {
            System.out.println("Entrando na thread ThreadClientServer");
            InputStreamReader is = new InputStreamReader(no.getInputStream());
            BufferedReader r = new BufferedReader(is);

            m = receiveMessage(no);
            System.out.println("Messagage received");
            switch (m.method) {
                case "PUT_OK":
                    System.out.println("PUT_OK received.");
                    put(m);
                    break;
                default:
                    System.out.println("Método desconhecido.");


            }
        }
        catch (Exception e){
            System.out.println("Error on thread.run: "+e.getMessage());
        }

    }
    private void put(Mensagem msg){
        boolean found = false;
        MapHelper mhAux;

        for (String name : Cliente.kv.kv.keySet()) {
            if (name.equals(msg.key)){
                found = true;
                break;
            }
        }
        mhAux = new MapHelper(msg.value, msg.ts);
        if (found){
            Cliente.kv.kv.replace(msg.key, mhAux);
        }
        else {
            Cliente.kv.kv.put(msg.key, mhAux);
        }
    }

    public Mensagem receiveMessage(Socket noServer) throws IOException {
        InputStreamReader is = new InputStreamReader(noServer.getInputStream());
        BufferedReader r = new BufferedReader(is);
        String jsonStr = r.readLine();
        return gson.fromJson(jsonStr, Mensagem.class);
    }
}

class Server {
    String ip;
    int port;
    Socket s;

    public Server(String ipServer, int portServer) throws IOException {
        ip = ipServer;
        port = portServer;
    }
    public void enviaMenssagem(Mensagem msg) throws IOException {
        Gson gson = new Gson();
        s = new Socket(ip, port);
        OutputStream os = s.getOutputStream();
        DataOutputStream writer = new DataOutputStream(os);

        writer.writeBytes(gson.toJson(msg) + "\n");
        s.close();
    }
    public String getValue(Mensagem msg) throws IOException {
        Gson gson = new Gson();
        s = new Socket(ip, port);
        OutputStream os = s.getOutputStream();
        DataOutputStream writer = new DataOutputStream(os);

        InputStreamReader is = new InputStreamReader(s.getInputStream());
        BufferedReader reader = new BufferedReader(is);

        writer.writeBytes(gson.toJson(msg) + "\n");
        String ret = reader.readLine();
        s.close();
        return ret;
    }
}

class MapHelperClient{
    public String value;
    public long ts;

    public MapHelperClient(String val, long t){
        value = val;
        ts = t;
    }
}

class KeyValueAccessClient {
    public HashMap<String, MapHelper> kv = new HashMap<>();
}
