package nasa.nccs.edas.workers;

import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.DataType;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TransVar {
    String _header;
    String _id;
    String _nodeId;
    byte[] _data;
    int[] _origin = null;
    int[] _shape = null;
    int _offset = 0;
    Map<String, String> _metadata;

    public TransVar( String header, byte[] data, int offset ) {
        _header = header;
        _data = data;
        _offset = offset;
        String[] header_items = header.split("[|]");
        _nodeId = header_items[0].split("[-]")[1];
        _id = header_items[1];
        _origin = s2ia( header_items[2] );
        _shape = s2ia( header_items[3] );
        _metadata = s2m( header_items[4] );
    }

    public String toString() {
        return String.format("TransVar: id=%s, header=%s", _id, _header );
    }

    public int[] getOrigin() { return _origin; }
    public int[] getShape() { return _shape; }
    public float[] getFloatArray() {
        ArrayFloat ucarArray = (ArrayFloat) Array.factory( DataType.FLOAT, _shape, getDataBuffer() );
        return (float[]) ucarArray.getStorage();
    }
    public String id() { return _id; }
    public ByteBuffer getDataBuffer() { return ByteBuffer.wrap( _data, _offset,_data.length-_offset ); }
    public Map<String, String> getMetaData() { return _metadata; }


    private int[] s2ia( String s ) {
        String[] items = s.split("[,]");
        int[] results = new int[items.length];
        for (int i = 0; i < items.length; i++) {
            try {
                results[i] = Integer.parseInt(items[i]);
            } catch (NumberFormatException nfe) { results[i] = Integer.MAX_VALUE; };
        }
        return results;
    }

    private Map<String, String>  s2m( String s ) {
        String[] items = s.split("[;]");
        Map<String, String>  results = new HashMap();
        for (int i = 0; i < items.length; i++) {
            String[] subitems = items[i].split("[:]");
            try{ results.put( subitems[0], subitems[1] ); } catch( ArrayIndexOutOfBoundsException err ) {;}
        }
        return results;
    }
}
