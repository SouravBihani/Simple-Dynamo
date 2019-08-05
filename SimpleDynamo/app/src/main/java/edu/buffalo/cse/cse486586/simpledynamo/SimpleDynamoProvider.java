package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

    private String PREF_FILE = "default";
    String myPort;
    static final int SERVER_PORT = 10000;
    private ArrayList<String> portlist = new ArrayList<String>();
    private ArrayList<String> cache_values;
    private HashMap<String,String> DB = new HashMap<String, String>();
    MatrixCursor cursor = null;
    int counter = 0;
    private Map<String, Cursor> locks = new HashMap<String, Cursor>();
    private final Semaphore mainlock = new Semaphore(1, true);
    private HashMap<String,ArrayList<String>> cache = new HashMap<String, ArrayList<String>>();
    private final Semaphore templock = new Semaphore(1, true);

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("@")){
//            DB.clear();
            SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sharedPreferences.edit();
            editor.clear();
            editor.apply();
        }
        else if(selection.equals("*")){
            for(int i = 0 ; i < portlist.size() ; i++){
                String del_all = "DeleteStar" + ":" + "not" + ":" + portlist.get(i);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , del_all);
            }
        }
        else{
            ArrayList<Integer> indexes;
            indexes = destinationPort(selection);
            for(int i = 0 ; i < indexes.size() ; i++){
//            Log.e(TAG,"dest port" + portlist.get(indexes.get(i)));
                String message = "Delete" + ":" + "not" + ":" + portlist.get(indexes.get(i)) + ":" + selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , message );
            }
        }
        return 0;
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.get("key").toString();
        String value = values.get("value").toString();
//        Log.i(TAG,"key to insert" + "--" + key);
//        Log.i(TAG,"value to insert" + "--" + value);
        if(only_node()){
            Log.e(TAG, "inserting only node");
//            DB.put(key,value);
            dbPut(key,value);
            return uri;
        }
        ArrayList<Integer> indexes;
        indexes = destinationPort(key);
        Log.e(TAG,"index" + portlist.get(indexes.get(0)) + "::" + key);
        for(int i = 0 ; i < indexes.size() ; i++){
//            try {
//                templock.acquire();
//            } catch (InterruptedException e) {
//                Log.e(TAG, "Sync Exception");
//            }
//            Log.e(TAG,"dest port" + portlist.get(indexes.get(i)));
            String message = "InsertKey" + ":" + getPort() + ":" + portlist.get(indexes.get(i)) + ":" + key + ":" + value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , message );
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        define_portlist();
        port_alive();
//		Log.e(TAG,"print portlist");
//		for(int i = 0 ; i < portlist.size() ; i++)
//		    Log.e(TAG,"list elem" + portlist.get(i));

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        Log.e(TAG,"Query type" + "--" + selection);
        cursor = new MatrixCursor(new String[] {"key","value"});

        String portCur = getPort();
        if(selection.equals("@")){
//            try{
//                Thread.sleep(5000);
//            }catch (Exception e){
//                e.printStackTrace();
//            }

//            try {
//                templock.acquire();
//            } catch (InterruptedException e) {
//                Log.e(TAG, "Sync Exception");
//            }

//            Log.e(TAG, "inside if @ size = " + DB.size());
//            for (Map.Entry<String,String> entry : DB.entrySet()) {
//                String key = entry.getKey();
//                String value = entry.getValue();
//                cursor.addRow(new String[]{key, value});
//            }
            SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
            Map<String, ?> allEntries = sharedPreferences.getAll();
            for (Map.Entry<String, ?> entry : allEntries.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();
                cursor.addRow(new String[]{key, value});
            }
//            templock.release();
            return cursor;
        }
        else if(selection.equals("*")) {


            SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
            Map<String, ?> allEntries = sharedPreferences.getAll();
            for (Map.Entry<String, ?> entry : allEntries.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();
                cursor.addRow(new String[]{key, value});
            }

            if (!only_node()) {
                for (String remotePort : portlist) {
                    if (remotePort.equals(portCur)) continue;
                    String message_star = "QueryStar" + ":" + portCur + ":" + remotePort;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , message_star);
                    synchronized (cursor){
                        try {
                            cursor.wait();
                        }catch (Exception e){
                            Log.e(TAG, "Lock Exception ");
                            e.printStackTrace();
                        }
                    }
                }
            }


        }
        else{
            ArrayList<Integer> indexes;
            indexes = destinationPort(selection);
//            if(portCur.equals(portlist.get(indexes.get(0))) || DB.containsKey(selection)){
//                Log.e(TAG,"key in my avd" + "::" + selection);
//                String key = selection;
//                String value = DB.get(selection);
////                cursor = new MatrixCursor(new String[] {"key","value"});
//                cursor.addRow(new String[]{key, value});
//                return cursor;
//            }
//            else{
            Cursor cursor1 = new MatrixCursor(new String[] {"key","value"});
//                Object o = new Object();
            locks.put(selection, cursor1);
            String message = "QueryRequest" + ":" + portCur + ":" + portlist.get(indexes.get(0)) + ":" + selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , message);
            synchronized (cursor1){
                try{
                    Log.e(TAG, "Going into wait");
                    cursor1.wait();
                    locks.remove(selection);
                    return cursor1;
                }catch (Exception e){
                    Log.e(TAG, "Lock Exception ");
                    e.printStackTrace();
                }
            }
//             }


        }

        return cursor;

    }


    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private void dbPut(String key, String value) {
        SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString(key, value);
        editor.apply();
    }

    private void cachePut(String key, ArrayList<String> t) {
        SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        Set<String> set = new HashSet<String>();
        set.addAll(t);
        editor.putStringSet(key,set);
        editor.apply();
    }

    private String dbGet(String key) {
        SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
        return sharedPreferences.getString(key, "null");
    }

    private Set<String> cacheGet(String key) {
        SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
        Set<String> set = sharedPreferences.getStringSet("key", null);
        return set;
    }

    private void port_alive(){
        String curport = getPort();
        for (int i = 0; i < portlist.size(); i++) {
            if(portlist.get(i).equals(curport))
                continue;
            String message = "RetrieveKey" + ":" + curport + ":" + portlist.get(i);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getHash(String value){

        String hash_val = "";
        try{
            hash_val = genHash(value);
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return hash_val;
    }

    private String getPort(){
        TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e(TAG, "Get Own Port = " + myPort);
        return myPort;
    }

    public class Compared implements Comparator<String> {

        @Override
        public int compare(String lhs, String rhs) {
            String l, r;
            try {
                l = getHash(String.valueOf(Integer.parseInt(lhs)/2));
            } catch (NumberFormatException e) {
                l = getHash(lhs);
            }
            try {
                r = getHash(String.valueOf(Integer.parseInt(rhs)/2));
            } catch (NumberFormatException e) {
                r = getHash(rhs);
            }
            return l.compareTo(r);
        }
    }



    private ArrayList destinationPort(String Keys){
//        Log.e(TAG,"destination function");
        ArrayList<String> temp1 = new ArrayList<String>();
        temp1.addAll(portlist);
        ArrayList<Integer> indices;
        //String hash_key = getHash(key);
        temp1.add(Keys);
        Collections.sort(temp1,new Compared());
        int index = temp1.indexOf(Keys);
//        Log.e(TAG,"index" + index + "of key" + Keys);
        indices = get_index(index);
//        Log.e(TAG,"print indexes");
//        for(int i = 0 ; i < indices.size(); i++)
//            Log.e(TAG,"indices rcvd" + indices.get(i));
        return indices;

    }

    public ArrayList get_index(int index){
        ArrayList<Integer> indexes = new ArrayList<Integer>();
        if(index == 5){
            indexes.add(0);
            indexes.add(1);
            indexes.add(2);
        }
        else if(index == 4){
            indexes.add(4);
            indexes.add(0);
            indexes.add(1);
        }
        else if(index == 3){
            indexes.add(3);
            indexes.add(4);
            indexes.add(0);
        }

        else {
            indexes.add(index);
            indexes.add(index + 1);
            indexes.add(index + 2);
        }


        return indexes;
    }

    public boolean only_node(){

        return (portlist.size() == 1);
    }

    private void define_portlist(){
        portlist.add("11108");
        portlist.add("11112");
        portlist.add("11116");
        portlist.add("11120");
        portlist.add("11124");
        Collections.sort(portlist,new Compared());
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.e(TAG, "Socket Accepted");
//            String msgClient = null;
            try {
                while (true) {
                    Log.e(TAG, "In Server");
                    Socket s = serverSocket.accept();
                    InputStream in = s.getInputStream();
                    DataInputStream data = new DataInputStream(in);
                    String msgClient = data.readUTF();
//                    InputStream is = s.getInputStream();
//                    InputStreamReader isr = new InputStreamReader(is);
//                    BufferedReader br = new BufferedReader(isr);
//                    msgClient = br.readLine();
                    Log.e(TAG,"Message recvd from client" + "--" + msgClient);
                    OutputStream out = s.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF("ack");
                    d.flush();
                    String message[] = msgClient.split(":");
                    String task = message[0];

                    if(task.equals("InsertKey")){
                        Log.e(TAG,"InsertKey in db");
                        Context context = getContext();
                        String key = message[3];
                        String value = message[4];
//                        DB.put(key,value);
                        dbPut(key,value);
                        String m = "InsertFulfilled" + ":" + message[2] + ":" + message[1];
//                        query_done(m);
//                        for (String name: DB.keySet()){
//
//                            String k =name.toString();
//                            String v = DB.get(name).toString();
//                            Log.e(TAG,"DB key val pair" +"--" + k + "::" + v);
//                        }

                    }

                    else if(task.equals("InsertFulfilled")){
                        //  templock.release();
                    }

                    else if(task.equals("QueryRequest")){
                        Log.e(TAG, "QueryRequest");
                        String search = message[3];
                        String portQuery = message[1];
//                        String line = DB.get(search);
                        String line = dbGet(search);
                        if(line.equals("null")){
                            Log.e(TAG,"ask other alt node" + message[4] + "for key" + search);
                            String alt_message = "QueryRequest" + ":" + message[1] + ":" + message[4] + ":" + message[3];
                            query_done(alt_message);
                        }
                        else{
                            String querysuccess = "QueryFullfilled" + ":" + "not" + ":" + portQuery + ":" + search + ":" + line;
                            query_done(querysuccess);
                        }


                    }

                    else if(task.equals("QueryFullfilled")){
                        Log.e(TAG, "QueryFullfilled****");
                        String key = message[3];
                        String val = message[4];
                        Log.e(TAG, "QueryFullfilled for " + key);
                        MatrixCursor c = (MatrixCursor) locks.get(key);
                        synchronized (c) {
                            c.addRow(new String[]{  key,val });
                            c.notify();
                        }


                    }

                    else if(task.equals("QueryStar")){
                        Log.e(TAG, "QueryStar");
                        String port_seek = message[1];
                        String query_return = "StarDone" + ":" + "not" + ":" + port_seek + ":";
//                        if(DB.size() != 0){
//                            for (Map.Entry<String,String> entry : DB.entrySet()){
//
//                                String key = entry.getKey();
//                                String value = entry.getValue();
//                                query_return += key + "#" + value + "@";
////                            query_done(query_return);
//                            }
//                            query_done(query_return);
//                        }
//                        else{
//                            String no_query_return = "StarDone" + ":" + "not" + ":" + port_seek + ":" + "DBEmpty";
//                            query_done(no_query_return);
//                        }

                        if(!PREF_FILE.isEmpty()){
                            SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
                            Map<String, ?> allEntries = sharedPreferences.getAll();
                            for (Map.Entry<String, ?> entry : allEntries.entrySet()) {
//                                Log.d("map values", entry.getKey() + ": " + entry.getValue().toString());
                                String key = entry.getKey();
                                String value = entry.getValue().toString();
                                query_return += key + "#" + value + "@";
                            }
                            query_done(query_return);
                        }
                        else{
                            String no_query_return = "StarDone" + ":" + "not" + ":" + port_seek + ":" + "DBEmpty";
                            query_done(no_query_return);
                        }


                    }

                    else if(task.equals("StarDone")){
                        counter ++;
                        Log.e(TAG, "StarDone");
                        String key_val = message[3];
                        Log.e(TAG,"keyval ka msgg" + "--" + key_val);
                        if(key_val.equals("DBEmpty")){
                            synchronized (cursor){
                                cursor.notify();
                            }
                        }
                        else{
                            String keyvalPair[] = key_val.split("@");
                            Log.e(TAG,"key val pair data" + "--" + keyvalPair[0]);
                            for(int i = 0 ; i < keyvalPair.length ; i++){
                                String splitter[] = keyvalPair[i].split("#");

                                String key = splitter[0];
                                String val = splitter[1];
                                Log.e(TAG,"key and value obtained" + "--" + key + "--" + val);
                                //synchronized (cursor) {
                                cursor.addRow(new String[]{  key,val });
                                //counter ++;
                                //}
                            }
                            Log.e(TAG,"portlist size" + portlist.size());
                            Log.e(TAG,"counter" + counter);

                            synchronized (cursor){
                                cursor.notify();
                            }
                        }


                    }
//
                    else if(task.equals("Delete")){
//                        DB.remove(message[3]);
                        SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
                        SharedPreferences.Editor editor = sharedPreferences.edit();
                        editor.remove(message[3]);
                        editor.apply();

                    }

                    else if(task.equals("DeleteStar")){
//                        DB.clear();
                        SharedPreferences sharedPreferences = getContext().getSharedPreferences(PREF_FILE, Context.MODE_PRIVATE);
                        SharedPreferences.Editor editor = sharedPreferences.edit();
                        editor.clear();
                        editor.apply();
                    }

                    else if(task.equals("RetrieveKey")){
                        String port_seek = message[1];
                        String retrieve_keys = "RetrieveDone" + ":" + "not" + ":" + port_seek + ":";
                        if(cache.size()!= 0 && cache.containsKey(port_seek)){
                            ArrayList<String> temp = cache.get(port_seek);
                            for(int i = 0 ; i < temp.size() ; i++){
                                String key_val = temp.get(i);
                                String split[] = key_val.split("::");
                                String key_to_send = split[0];
                                String value_to_send = split[1];
                                retrieve_keys += key_to_send + "#" + value_to_send + "@";
                            }
                            query_done(retrieve_keys);
                        }
                        else{
                            retrieve_keys = "RetrieveDone" + ":" + "not" + ":" + port_seek + ":" + "CacheEmpty";
                            query_done(retrieve_keys);
                        }

                    }
                    else if(task.equals("RetrieveDone")){
                        String ret_keys = message[3];
                        if(!ret_keys.equals("CacheEmpty")){
                            String retPair[] = ret_keys.split("@");
                            Log.e(TAG,"retpair data" + "--" + retPair[0]);
                            for(int i = 0 ; i < retPair.length ; i++) {
                                String splitter[] = retPair[i].split("#");
                                String key = splitter[0];
                                String val = splitter[1];
//                                DB.put(key,val);
                                dbPut(key,val);
                            }
                        }

//                        templock.release();
                    }
//
                }
            } catch (IOException e) {
                Log.e(TAG, "Server Socket IO exception ");
                e.printStackTrace();
            }
            return null;
        }


        public void query_done(String msg){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR , msg );
        }

    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                mainlock.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception");
            }
            String msg = msgs[0];
            String message[] = msg.split(":");
            try {
                Log.e(TAG, "In Client");

                String destPort = message[2];
                Log.e(TAG, "Data send from client to dest port" + "--" + destPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(destPort));
                OutputStream out = socket.getOutputStream();
                DataOutputStream d = new DataOutputStream(out);
                Log.e(TAG,"message to send from client" + "=" + msgs[0]);
                socket.setSoTimeout(2000);
                d.writeUTF(msg);
                d.flush();
//                OutputStream os = socket.getOutputStream();
//                OutputStreamWriter osw = new OutputStreamWriter(os);
//                BufferedWriter bw = new BufferedWriter(osw);
//                bw.write(msg);
//                bw.flush();
                InputStream in = socket.getInputStream();
                DataInputStream data = new DataInputStream(in);
                String ack = data.readUTF();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (Exception e) {
                Log.e(TAG, "ClientTask socket Exception");
                if(message[0].equals("InsertKey")){
                    if(!cache.containsKey(message[2])){
                        cache.put(message[2],new ArrayList<String>());
                    }
                    cache.get(message[2]).add(message[3] + "::" + message[4]);
                }

                else if(message[0].equals("QueryRequest")){
//                    try {
//                        templock.acquire();
//                    } catch (InterruptedException e2) {
//                        Log.e(TAG, "Sync Exception");
//                    }
                    try{
                        Thread.sleep(3000);
                    }catch (Exception e2){
                        e.printStackTrace();
                    }
                    ArrayList<Integer> ind;
                    ind = destinationPort(message[3]);
//                    for(int i = 1 ; i < indexes.size() ; i++){
//                        String alt_message = "QueryRequest" + ":" + message[1] + ":" + portlist.get(indexes.get(i)) + ":" + message[3];
//                        try{
//                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                    Integer.parseInt(portlist.get(indexes.get(i))));
//                            OutputStream out = socket.getOutputStream();
//                            DataOutputStream d = new DataOutputStream(out);
//                            Log.e(TAG,"alternate message to send from client" + "=" + msgs[0]);
//                            socket.setSoTimeout(2000);
//                            d.writeUTF(alt_message);
//                            d.flush();
//                        }catch (Exception e1){
//                            e1.printStackTrace();
//                        }
//                    }
                    int alt_index;
                    int index = portlist.indexOf(message[2]);
                    if(index == 4)
                        alt_index = 0;
                    else
                        alt_index = index + 1;
                    String alt_port = portlist.get(alt_index);
                    String alt_message = "QueryRequest" + ":" + message[1] + ":" + portlist.get(ind.get(2)) + ":" + message[3] + ":" + portlist.get(ind.get(1));
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(alt_port));
                        OutputStream out = socket.getOutputStream();
                        DataOutputStream d = new DataOutputStream(out);
                        Log.e(TAG,"alternate message to send from client" + "=" + msgs[0]);
                        socket.setSoTimeout(2000);
                        d.writeUTF(alt_message);
                        d.flush();
                    }catch (Exception e1){
                        e1.printStackTrace();
                    }


                }

                else if(message[0].equals("QueryStar")){
                    synchronized (cursor){
                        cursor.notify();
                    }
                }
            }
            finally {
                mainlock.release();
            }


            return null;
        }
    }


}