package sz.cluster.spark.test;

import java.nio.file.Path;



public class FilePair {
Path p1,p2;

public FilePair(Path p12, Path path) {
	super();
	this.p1 = p12;
	this.p2 = path;
}
public Path getP1() {
	return p1;
}
public Path getP2() {
	return p2;
}
}
