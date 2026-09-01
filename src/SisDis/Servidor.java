package SisDis;

/* Bibliotecas utilizadas pelo Servidor:
 - Gson: transformar classe Mensagem em json e json em classe Mensagem;
 - Socket e ServerSocket: comunicação TCP;
 - ArrayList: Controle de mensagens em fila;
 - HashMap: criação da estrutura chave-valor
 */
import com.google.gson.Gson;

import java.awt.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Servidor {
    //Inicialização das variáveis
    static long marcoZero = System.currentTimeMillis(); //TimeStamp utilizado pelo Servidor
    static String ip = "127.0.0.1"; //IP do servidor hardcoded - máquina local
    static int port; //porta utilizada pelo serverSocket - capturada pelo teclado
    static String leaderIp = "127.0.0.1"; //IP do servidor líder - máquina local
    static int leaderPort = 10097;//Porta do servidor líder
    static int[] otherPorts = {10098, 10099};//Porta dos outros dois servidores
    static boolean initialized = false; //flag auxiliar de inicialização
    static boolean isLeader = false;//flag verifica ser Servidor é líder
    static ArrayList<Mensagem> listMenssagem = new ArrayList<>(); //Controle de fila de mensagens
    static KeyValueAccess kv = new KeyValueAccess(); //Estrutura chave-valor. Descrição na classe KeyValueAccess

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        ServerSocket serverSocket = null;

        //Inicialização do Servidor. Porta capturada pelo teclado.
        //Identifica se Servidor é líder (porta == 10097)
        while (!initialized) {
            try {
                System.out.println("Ip deste servidor: " + ip);
                System.out.println("Por favor entre com a porta deste servidor:");
                port = Integer.parseInt(inFromUser.readLine());
                isLeader = port == leaderPort;
                if (isLeader){System.out.println("Este servidor é líder, porta " + port);} else {System.out.println("Servidor não líder, porta " + port);}
                serverSocket = new ServerSocket(port);
                initialized = true;
            }
            catch (Exception e){
                System.out.println("Servidor não inicializado.");
                System.out.println(e.getMessage());
            }
        }

        //Loop infinito. Inicialização do serverSocket.
        //A cada conexão, uma thread é criada.
        //Thread responsável por gerenciar mensagens e responder clientes.
        while (true){
            System.out.println("Esperando conn");
            Socket no = serverSocket.accept(); //BLOCKING
            System.out.println("Conn aceita");
            Thread th = new ThreadServer(no, marcoZero, isLeader, otherPorts);
            th.start();
        }
    }
}

class ThreadServer extends Thread{
    private Socket no;
    public volatile static MapHelper mh;
    public volatile static String key;
    public static KeyValueAccess kvThread;
    public long mz;
    boolean isleader;
    int[] ohterports;
    Mensagem msgAux;
    Gson gson = new Gson();

    public ThreadServer(Socket node, long marcoZero, boolean leader, int[] ports) {
        no = node;
        mz = marcoZero;
        isleader = leader;
        ohterports = ports;

    }

    public void run(){
        try {
            InputStreamReader is = new InputStreamReader(no.getInputStream());
            BufferedReader reader = new BufferedReader(is);

            OutputStream os = no.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);

            Mensagem m = receiveMessage(reader);

            System.out.println(m.key);

            switch (m.method){
                case "PUT":
                    /*
                    Recebe Requisição PUT
                    Caso seja líder:
                      1. Insere a informação em uma tabela de hash local, associando a um timeStamp para essa key (função put(Menssagem m)).
                      2. Replica a informação para outros servidores (função replicate(Menssagem m)).
                      3 OBS.: O envio do PUT_OK para o cliente acontece em outra etapa do código, respeitando a lógica exigida pelo projeto.
                    Caso não seja líder: Encaminha requisição para o líder (função forwardPutToLeader(Menssagem m)). O líder segue as mesmas
                      etapas descritas anteriormente.
                     */
                    if(isleader) {
                        //System.out.println("PUT Received");
                        m.ts = put(m);
                        System.out.println("Antes da func replicate");
                        replicate(m);
                    } else {
                        System.out.println("Foward to leader");
                        forwardPutToLeader(m);
                    }
                    break;
                case "REPLICATION":
                    System.out.println("RECEIVING REPLICATION");
                    /*
                    Mensagem recebida pelos servidores não líderes quando o líder usa o método replicate(Menssagem m).
                      1. Insere informação em uma tabela de hash local, já com o timeStamp fornecido pelo líder (função replicate(Menssagem m)).
                      2. Envia REPLICATION_OK para o líder (função sendReplicationOk(Menssagem m)).
                    * */
                    m.ts = put(m);
                    sendReplicationOk(m);
                    for (Mensagem msg: Servidor.listMenssagem){
                        if (msg.key.equals(m.key) && (msg.ts < m.ts) && msg.method.equals("WAIT_FOR_RESPONSE")){
                            msgAux.method = "PUT_OK";
                            msgAux.key = msg.key;
                            msgAux.ts = m.ts;
                            msgAux.portFrom = msg.portFrom;
                            sendPutOk(msgAux);
                            break;
                        }
                    }
                    break;
                case "GET":
                    get(m, writer);
                    break;
                case "REPLICATION_OK":
                    System.out.println("RECEIVING REPLICATION OK");
                    /* Mensagem REPLICATION_OK enviadas pelos Servidores não líders como resposta a mensagem REPLICATION
                       Cada vez que o líder recebe essa mensagem, ele verifica se a mensagem já existe e quantos servidores
                       já a enviaram. Esse monitoramento é feito através da lista "Servidor.listMenssagem", utilizando o
                       campo count da classe Mensagem. Caso o campo count seja igual ao número de servidores não líderes
                       (Servidor.otherPorts.length, que nesse caso é igual a dois), o líder remove a mensagem da lista de
                       mensagens e envia PUT_OK para o Cliente (função sendPutOk(Menssagem m)).

                    */
                    System.out.println("Before loop msg");
                    for (Mensagem msg: Servidor.listMenssagem){
                        System.out.println("msg.key: " + msg.key);
                        if (msg.key.equals(m.key) && (msg.ts == m.ts)){
                            msg.count = msg.count + 1;
                            msgAux = msg;
                        }
                    }
                    System.out.println("After loop msg");
                    if (msgAux == null){
                        m.count = 1;
                        Servidor.listMenssagem.add(m);
                        msgAux = m;
                    }else if (msgAux.count>=Servidor.otherPorts.length){
                        sendPutOk(m);
                        Servidor.listMenssagem.remove(msgAux);
                    }

                    break;
                case "PRINTALL":
                    System.out.println("Printing all elements of hashmap");
                    for (String name : Servidor.kv.kv.keySet()) {
                        System.out.println("Key: " + name + ", Value: " + Servidor.kv.kv.get(name).value + ", TimeStamp: " + Servidor.kv.kv.get(name).ts);
                    }
                    break;
                default:
                    System.out.println("Command not found: " + m.method);
            }

        }
        catch (Exception e){
            System.out.println("Error on thread.run: "+e.getMessage());
            System.out.println(e.getMessage());
            System.out.println(e.getLocalizedMessage());
            System.out.println(e.getStackTrace()[0]);
        }
    }

    private Mensagem receiveMessage(BufferedReader r) throws Exception{
        String jsonStr = r.readLine();
        return gson.fromJson(jsonStr, Mensagem.class);
    }

    private long put(Mensagem msg){
        boolean found = false;
        MapHelper mhAux;
        long tsAux;
        if (Servidor.isLeader){
            tsAux = System.currentTimeMillis()-Servidor.marcoZero;
        } else {tsAux = msg.ts;}

        for (String name : Servidor.kv.kv.keySet()) {
            if (name.equals(msg.key)){
                found = true;
                break;
            }
        }
        mhAux = new MapHelper(msg.value, tsAux);
        if (found){
            Servidor.kv.kv.replace(msg.key, mhAux);
        }
        else {
            Servidor.kv.kv.put(msg.key, mhAux);
        }
        return tsAux;
    }

    private void forwardPutToLeader(Mensagem msg) throws IOException {
        Socket s = new Socket("127.0.0.1", Servidor.leaderPort);
        OutputStream os = s.getOutputStream();
        DataOutputStream writer = new DataOutputStream(os);
        Mensagem mensagem = msg;
        mensagem.method = "PUT";
        writer.writeBytes(gson.toJson(mensagem) + "\n");
        s.close();
    }

    private void sendPutOk(Mensagem msg) throws IOException {
        System.out.println("Sending PUT_OK");
        Socket s = new Socket("127.0.0.1", msg.portFrom);
        OutputStream os = s.getOutputStream();
        DataOutputStream writer = new DataOutputStream(os);
        Mensagem mensagem = msg;
        mensagem.method = "PUT_OK";
        writer.writeBytes(gson.toJson(mensagem) + "\n");
        System.out.println("PUT_OK sent");
        s.close();

    }

    private void get(Mensagem msg, DataOutputStream w) throws IOException {
        boolean found = false;
        for (String name : Servidor.kv.kv.keySet()) {
            if (name.equals(msg.key)){
                found = true;
                break;
            }
        }
        Mensagem mensagem = new Mensagem();
        if (found){// && msg.ts<=Servidor.kv.kv.get(msg.key).ts) {
            mensagem.key = msg.key;
            mensagem.value = Servidor.kv.kv.get(msg.key).value;
            mensagem.ts = Servidor.kv.kv.get(msg.key).ts;
        }
        else{
            mensagem.key = msg.key;
            mensagem.value = msg.value;
            mensagem.ts = msg.ts;
        }
        if(msg.ts<mensagem.ts) {
            w.writeBytes(gson.toJson(mensagem) + "\n");
        }
        else {
            mensagem.method = "WAIT_FOR_RESPONSE";
            Servidor.listMenssagem.add(mensagem);
            w.writeBytes(gson.toJson(mensagem) + "\n");
        }


    }

    private void replicate(Mensagem msg) throws IOException {
        for (int i = 0; i<Servidor.otherPorts.length; i++){
            Socket s = new Socket("127.0.0.1", ohterports[i]);
            OutputStream os = s.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);
            Mensagem mensagem = msg;
            mensagem.method = "REPLICATION";
            mensagem.key = msg.key;
            mensagem.value = msg.value;//Servidor.kv.kv.get(key).value;
            mensagem.ts = msg.ts;//Servidor.kv.kv.get(key).ts;
            writer.writeBytes(gson.toJson(mensagem) + "\n");

            s.close();
        }
    }

    private void sendReplicationOk(Mensagem msg) throws IOException {
        Socket s = new Socket(Servidor.leaderIp, Servidor.leaderPort);
        OutputStream os = s.getOutputStream();
        DataOutputStream writer = new DataOutputStream(os);
        msg.method = "REPLICATION_OK";
        writer.writeBytes(gson.toJson(msg) + "\n");
        s.close();

    }

}

class MapHelper{
    public String value;
    public long ts;

    public MapHelper(String val, long t){
        value = val;
        ts = t;
    }
}

class KeyValueAccess {
    public HashMap<String, MapHelper> kv = new HashMap<>();
}



