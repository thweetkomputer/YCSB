/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.ByteArrayByteIterator;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCommands jedis;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      jedis = new JedisCluster(jedisClusterNodes);
    } else {
      String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
      if (redisTimeout != null){
        jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
      } else {
        jedis = new Jedis(host, port);
      }
      ((Jedis) jedis).connect();
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  private interface RedisOp<T> {
    T run() throws Exception;
  }

  private <T> T retryForever(RedisOp<T> op) {
    for (;;) {
      try {
        return op.run();
      } catch (Exception e) {
        try {
          Thread.sleep(5 * 60 * 1000L);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  private byte[] serialize(Map<String, ByteIterator> values) {
    int n = values.size();
  
    List<byte[]> keys = new ArrayList<>(n);
    List<byte[]> vals = new ArrayList<>(n);
  
    int total = 4; // N
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      byte[] k = e.getKey().getBytes();
      byte[] v = e.getValue().toArray();
      keys.add(k);
      vals.add(v);
  
      total += 4 + k.length; // k_len + k
      total += 4 + v.length; // v_len + v
    }
  
    byte[] buf = new byte[total];
    int pos = 0;
  
    pos = writeInt(buf, pos, n);
  
    for (int i = 0; i < n; i++) {
      byte[] k = keys.get(i);
      byte[] v = vals.get(i);
  
      pos = writeInt(buf, pos, k.length);
      System.arraycopy(k, 0, buf, pos, k.length);
      pos += k.length;
  
      pos = writeInt(buf, pos, v.length);
      System.arraycopy(v, 0, buf, pos, v.length);
      pos += v.length;
    }
    return buf;
  }
  
  private void deserialize(byte[] raw, Map<String, ByteIterator> out) {
    int pos = 0;
    int n = readInt(raw, pos);
    pos += 4;
  
    for (int i = 0; i < n; i++) {
      int klen = readInt(raw, pos);
      pos += 4;
      String k = new String(raw, pos, klen);
      pos += klen;
  
      int vlen = readInt(raw, pos);
      pos += 4;
      byte[] v = new byte[vlen];
      System.arraycopy(raw, pos, v, 0, vlen);
      pos += vlen;
  
      out.put(k, new ByteArrayByteIterator(v));
    }
  }
  
  private int writeInt(byte[] buf, int pos, int v) {
    buf[pos]     = (byte)(v >>> 24);
    buf[pos + 1] = (byte)(v >>> 16);
    buf[pos + 2] = (byte)(v >>>  8);
    buf[pos + 3] = (byte)(v);
    return pos + 4;
  }
  
  private int readInt(byte[] buf, int pos) {
    return ((buf[pos]     & 0xFF) << 24) |
           ((buf[pos + 1] & 0xFF) << 16) |
           ((buf[pos + 2] & 0xFF) <<  8) |
           (buf[pos + 3] & 0xFF);
  }


  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      return retryForever(() -> {
          byte[] data = serialize(values);
          String reply = ((Jedis) jedis).set(key.getBytes(), data);
          return "OK".equals(reply) ? Status.OK : Status.ERROR;
        });
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
  
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      return retryForever(() -> {
          byte[] raw = ((Jedis) jedis).get(key.getBytes());
          deserialize(raw, result);
  
          if (fields != null) {
            result.entrySet().removeIf(e -> !fields.contains(e.getKey()));
          }
          return Status.OK;
        });
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startKey, int recordCount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
