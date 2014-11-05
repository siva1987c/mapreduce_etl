package com.xxx.algo.ad.mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Common Data And Function
 */
public class CommonDataAndFunc {

  public static final String CTRL_A = "\u0001";
  public static final String CTRL_B = "\u0002";
  public static final String CTRL_C = "\u0003";
  public static final String CTRL_D = "\u0004";
  public static final String CTRL_E = "\u0005";
  public static final String CTRL_F = "\u0006";
  public static final String TAB = "\t";
  public static final String COMMA = ",";
  public static final String SPACE = " ";
  public static final String COLON = ":";

  /**
   * 将Collection对象用分隔符连接成StringBuilder
   */
  public static StringBuilder join(Collection<?> follows, String sep) {
    StringBuilder sb = new StringBuilder();

    if (follows == null || sep == null) {
      return sb;
    }

    Iterator<?> it = follows.iterator();
    while (it.hasNext()) {
      sb.append(it.next());
      if (it.hasNext()) {
        sb.append(sep);
      }
    }
    return sb;
  }

  /**
   * 将一个可以序列化的对象写入文件
   */
  public static void writeObjToFile(Object obj, String fileName) {
    try {
      FileOutputStream fos = new FileOutputStream(fileName);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(obj);
      oos.flush();
      oos.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 从文件中读取一个被序列化的对象
   */
  public static Object getObjFromFile(String fileName) {
    Object obj = null;
    try {
      FileInputStream fis = new FileInputStream(fileName);
      ObjectInputStream ois = new ObjectInputStream(fis);
      obj = ois.readObject();
      ois.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return obj;
  }

  /**
   * 从文件中读取每一行，用sep分隔后，去index位置构成Set返回
   */
  public static Set<String> readSets(String fileName, String sep, 
      int index, String encoding) {
    Set<String> res = new HashSet<String>();
    if (index < 0) {
      return res;
    }

    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(fileName), encoding));
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#") || line.length() < 1) {
          // 跳过注释行
          continue;
        }
        String[] arr = line.split(sep, -1);
        if (arr.length <= index) {
          continue;
        }
        res.add(arr[index]);
        System.out.println("success put " + arr[index]);
      }
      reader.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return res;
  }

  /**
   * 从文件中读取每一行，用sep分隔后，取kInd和vInd位置元素构成map返回
   */
  public static Map<String, String> readMaps(String fileName, String sep,
      int kInd, int vInd, String encoding) {
    Map<String, String> res = new HashMap<String, String>();
    if (kInd < 0 || vInd < 0) {
      return res;
    }

    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(fileName), encoding));
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#") || line.length() < 1) {
          // 跳过注释行
          continue;
        }
        String[] arr = line.split(sep, -1);
        if (arr.length <= kInd || arr.length <= vInd) {
          continue;
        }
        res.put(arr[kInd], arr[vInd]);
        System.out.println("success put " + arr[kInd] + " " + arr[vInd]);
      }
      reader.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return res;
  }
  
  /**
   * read line from file and split by sep, make line number mapping to value
   */
  public static Map<Integer, String> readOrderMap(String fileName, String sep,
		  int vInd, String encoding) {
    Map<Integer, String> res = new HashMap<Integer, String>();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(fileName), encoding));
      String line;
      Integer idx = 0;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#") || line.length() < 1) {
          continue;
        }
        String[] arr = line.split(sep, -1);
        if (arr.length <= vInd) {
          continue;
        }
        res.put(idx, arr[vInd]);
        //System.out.println("success put " + idx + " " + arr[vInd]);
        idx += 1;
      }
      reader.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return res;
  }
  
  
  
}
