package water.csv;

import java.io.InputStream;

public interface IObjectParser {
	//void addByte(byte b);
	Object parse(byte [] arr, int from, int to);
	Object parse(InputStream is);
	//Object getValue();
	//void reset();
}
