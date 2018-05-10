package g500inpcj;

public interface Kernel1 {

	int findLocalVertexNumberForGlobalVertexNumber(long globalVertexNumber);

	int findTaskForGlobalVertexNumber(long globalVertexNumber);

	long findGlobalVertexNumberForLocalVertexNumber(long taskId,
			long localVertexNumber);

	long getNv();
}
