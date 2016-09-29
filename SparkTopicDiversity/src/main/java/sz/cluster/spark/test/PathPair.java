package sz.cluster.spark.test;

import org.apache.hadoop.fs.Path;

public class PathPair {
Path p1,p2;

public PathPair(Path p1, Path p2) {
	super();
	this.p1 = p1;
	this.p2 = p2;
}
public Path getP1() {
	return p1;
}
public Path getP2() {
	return p2;
}
}
