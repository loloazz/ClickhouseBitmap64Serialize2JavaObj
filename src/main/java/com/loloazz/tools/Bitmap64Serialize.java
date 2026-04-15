package com.loloazz.tools;

import io.opencensus.implcore.internal.VarInt;
import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Base64;
import java.util.NavigableMap;


public class Bitmap64Serialize {

    private Bitmap64Serialize() {
        throw new IllegalStateException("工具类！");
    }

    public static ByteBuffer serialize2CK(Roaring64NavigableMap rb) throws Exception {
        ByteBuffer bos;
        if (rb.getLongCardinality() <= 32) {
            ByteBuffer preBuffer = ByteBuffer.allocate(1 + 1 + 8 * rb.getIntCardinality());
            if (preBuffer.order() == ByteOrder.LITTLE_ENDIAN) {
                bos = preBuffer;
            } else {
                bos = preBuffer.slice().order(ByteOrder.LITTLE_ENDIAN);
            }
            // 第一部分 标记RBM的类型 基数小于32时，使用smallSet（0），否则标记为1
            bos.put((byte) 0);
            // 第二部分 VarInt存储RBM的所占用字节长度
            bos.put((byte) rb.getIntCardinality());
            // 第三部分 数据
            for (long l : rb.toArray()) {
                bos.putLong(l);
            }
        } else {
            // 共有两种序列化mode，第二种mode，源码中做了字节序翻转，Java数据结构默认大端序，翻转后就成了小端序
            // 另一种Mode下，前缀是单个long，8个字节，需要在未来序列化时，删除
            int rbmPrefixBytes = 8;
            int serializedSizeInBytes = (int) rb.serializedSizeInBytes();
            int rbTotalSize = serializedSizeInBytes - rbmPrefixBytes + 8;
            int varIntLen = VarInt.varLongSize(rbTotalSize);
            // roaringbitmap序列化结构ck：Byte(1), VarInt(SerializedSizeInBytes),Long(highToBitmapSize) ByteArray(RoaringBitmap)
            ByteBuffer preBuffer = ByteBuffer.allocate(1 + varIntLen + rbTotalSize);
            if (preBuffer.order() == ByteOrder.LITTLE_ENDIAN) {
                bos = preBuffer;
            } else {
                bos = preBuffer.slice().order(ByteOrder.LITTLE_ENDIAN);
            }

            bos.put((byte) 1);
            VarInt.putVarInt(rbTotalSize, bos);

            bos.putLong(getHighToBitmap(rb).size());

            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            try {
                rb.serializePortable(new DataOutputStream(bas));
            } catch (IOException e) {
                throw new Exception("序列化Roaring64NavigableMap 为新输出时错误", e);
            }

            byte[] outPutPre = bas.toByteArray();
            byte[] outPutRes = Arrays.copyOfRange(outPutPre, rbmPrefixBytes, serializedSizeInBytes);

            bos.put(outPutRes);
        }
        return bos;
    }

    public static byte[] serialize2Java(Roaring64NavigableMap rb) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        rb.serializePortable(dataOutputStream);
        return outputStream.toByteArray();
    }

    /**
     *
     * @param buffer 字节对象
     * @return 转换后的 roaring64NavigableMap
     * @throws IOException ioe
     */
    public static Roaring64NavigableMap deserializeFromJava(byte[] buffer) throws IOException {
        Roaring64NavigableMap r2 = generateEmptyPortableBitmap();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer);
        r2.deserializePortable(new DataInputStream(inputStream));
        return r2;
    }

    public static Roaring64NavigableMap deserializeFromJava(InputStream inputStream) throws IOException {
        Roaring64NavigableMap r2 = generateEmptyPortableBitmap();
        r2.deserializePortable(new DataInputStream(inputStream));
        return r2;
    }


    public static String generateBase64EncodeStr(Roaring64NavigableMap roaring64NavigableMap) throws Exception {
        ByteBuffer rb = serialize2CK(roaring64NavigableMap);
        return Base64.getEncoder().encodeToString(rb.array());
    }

    /**
     * Decode ClickHouse bitmap bytes (base64Encode(toString(bitmap...))) into Java Roaring64NavigableMap.
     *
     * Format supported (ClickHouse 24.5 groupBitmap):
     * - type=0 (smallSet): [u8 type=0][u8 count][count * u64 little-endian values]
     * - type=1 (roaringSet): [u8 type=1][varint payloadLen][u64 little-endian highToBitmapSize][portable_bytes_without_8B_prefix]
     */
    public static Roaring64NavigableMap deserializeFromCKBase64(String ckBitmapBase64) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(ckBitmapBase64);
        return deserializeFromCKBytes(bytes);
    }

    public static Roaring64NavigableMap deserializeFromCKBytes(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length < 2) {
            throw new IOException("CK bitmap bytes too short");
        }

        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int type = buf.get() & 0xff;
        if (type == 0) {
            int count = buf.get() & 0xff;
            Roaring64NavigableMap rb = generateEmptyPortableBitmap();
            for (int i = 0; i < count; i++) {
                if (buf.remaining() < 8) {
                    throw new IOException("CK smallSet truncated, remaining=" + buf.remaining());
                }
                long v = buf.getLong();
                rb.add(v);
            }
            return rb;
        }

        if (type != 1) {
            throw new IOException("Unknown CK bitmap type: " + type);
        }

        // Read payload length varint (we don't strictly need it, but it advances buffer correctly).
        int payloadLen = readVarInt32(buf);
        if (payloadLen < 8) {
            throw new IOException("CK roaringSet payload too short: " + payloadLen);
        }

        long highToBitmapSize = buf.getLong(); // little-endian
        // Reconstruct Java portable stream: [8-byte prefix][rest bytes]
        // In our serialize2CK we wrote highToBitmapSize separately and removed the first 8 bytes of portable stream.
        int restLen = payloadLen - 8;
        if (buf.remaining() < restLen) {
            throw new IOException("CK roaringSet truncated, need=" + restLen + ", remaining=" + buf.remaining());
        }

        byte[] portable = new byte[8 + restLen];
        ByteBuffer prefix = ByteBuffer.wrap(portable).order(ByteOrder.LITTLE_ENDIAN);
        prefix.putLong(highToBitmapSize);
        buf.get(portable, 8, restLen);

        return deserializeFromJava(portable);
    }

    private static int readVarInt32(ByteBuffer buf) throws IOException {
        int result = 0;
        int shift = 0;
        while (shift < 32) {
            if (!buf.hasRemaining()) {
                throw new IOException("Unexpected end while reading varint");
            }
            int b = buf.get() & 0xff;
            result |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new IOException("VarInt32 too long");
    }

    public static NavigableMap<Integer, BitmapDataProvider> getHighToBitmap(Roaring64NavigableMap rb) throws Exception {
        try {
            Method method = Roaring64NavigableMap.class.getDeclaredMethod("getHighToBitmap");
            method.setAccessible(true);
            return (NavigableMap<Integer, BitmapDataProvider>) method.invoke(rb, null);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new Exception("反射获取Roaring64NavigableMap的HighToBitmap时错误！", e);
        }
    }

    public static Roaring64NavigableMap generateEmptyPortableBitmap() {
        Roaring64NavigableMap.SERIALIZATION_MODE = Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
        return new Roaring64NavigableMap();
    }

    public static void main(String[] args) throws Exception {

        Roaring64NavigableMap r1 = generateEmptyPortableBitmap();
        for (int i = 1; i <= 32; i++) {
            r1.add(i);
        }
        r1.add(33333333333333L);

        System.out.println(generateBase64EncodeStr(r1));

        byte[] r1Arr = serialize2Java(r1);

        Roaring64NavigableMap r2 = deserializeFromJava(r1Arr);


        System.out.println(Arrays.toString(r1.toArray()));
        System.out.println(Arrays.toString(r2.toArray()));
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream2 = new DataOutputStream(outputStream2);
        r2.serializePortable(dataOutputStream2);

        System.out.println(Arrays.toString(r1Arr));
        System.out.println(Arrays.toString(outputStream2.toByteArray()));

       /* Roaring64NavigableMap r3 = new Roaring64NavigableMap();
        r1.or(r3);
        System.out.println(Arrays.toString(r1.toArray()));*/
    }

}
