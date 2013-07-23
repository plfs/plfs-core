#include <string.h>
#include "plfs.h"
#include "container_internals.h"
#include <stdlib.h>
#include <unistd.h>
#include <mpi.h>
#include <stdio.h>
#include <math.h>
#include <errno.h>
#include <float.h>

/* use the plfs_query to get the number of indices we have so we can
 * use that in the parsing - the user will decide the number of processes
 * that are reading but we must know when we have read all of the indices */
int 
getNumberOfIndexFiles(const char* query) 
{
	FILE *fp = NULL;
	int number = 0; 
	fp = fopen(query, "r");
	if (fp != NULL) 
	{
		char buffer[128]; 
		while (fgets(buffer, sizeof(buffer), fp) != NULL)
		{
			/* this is an index file */
			if (strstr(buffer, "index") != NULL) {
				number++; 
			}
		}
		fclose(fp); 
	}
	if (fp == NULL)
	{
		printf("Could not open the query file:%s\n", query); 
	}
	return number;
}

/* this is so we can find the time that each bin will be for the graphs
 * it uses MPI_reduce to find the maximum and minimum times of all the 
 * given IO entries. This also finds the final time that each proc wrote until
 * to be used in the per processor graphs if requested. Finally calculates the
 * sum of the end times for the standard deviation and average calculations*/
int 
getMaxMinTimes(int numIndexFiles, int size, const char* mount, double* minMax,
		 double* endSum, double* endTimes)
{
	double sendMinMax[2]; 
	double *sendEndTimes = (double *)calloc(numIndexFiles, sizeof(double)); 
	int rank, i, retv; 
	char buffer[128];
	char name[50];
	FILE* tmp; 
	int id, offset, length, tail; 
	char io; 
	double beg, end;
	char* id2, chunk; 
	sendMinMax[0] = DBL_MAX; 
	sendMinMax[1] = DBL_MIN; 
	MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
	double sendEndSum = 0; 
	double fileEnd = DBL_MIN; 
	for (i = 0; i<numIndexFiles; i++) {
		if (i % size == rank) {
			/* Create a temporary file */
			int k = sprintf(name, "tempFile%d.txt", rank); 
			tmp = fopen(name, "w+");
			if (tmp == NULL) {
				printf("ERROR: Could not create temp file\n"); 
				return -1; 
			}
			retv = container_dump_index(tmp, mount, 0, 1, i); 
			fclose(tmp); 
			if (retv == 0) {
				tmp = fopen(name, "r"); 
				if (tmp == NULL) {
					printf("ERROR: Could not open temp file\n"); 
					return -1; 
				}
				while( fgets(buffer, sizeof(buffer), tmp) != NULL)
				{
					if (buffer[0] != '#') 
					{
						int n = sscanf(buffer, 
								"%d %c %d %d %lf %lf %d %s %s", 
								&id, &io, &offset, &length, 
								&beg, &end, &tail, id2, chunk); 
						/* if either are -1, then it hasn't been set */
						if (fileEnd < end) {
							fileEnd = end; 
						}
						if (sendMinMax[0] > beg) {
							sendMinMax[0] = beg; 
						}
						if (sendMinMax[1] < end) {
							sendMinMax[1] = end; 
						}
					}
				}
				fclose(tmp); 
			}
			sendEndSum += fileEnd; /* this is the max for proc i */
			sendEndTimes[i] = fileEnd; 
			fileEnd = DBL_MIN; 
		}
		else {
			continue;
		}
	}
	/* use MPI to get the global max and min  */
	double min, max; 
	double localMin = sendMinMax[0]; 
	double localMax = sendMinMax[1];
	MPI_Allreduce(&localMin, &min, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);
	MPI_Allreduce(&localMax, &max, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
	MPI_Allreduce(&sendEndSum, endSum, 1, MPI_DOUBLE, MPI_SUM,
				MPI_COMM_WORLD); 
	MPI_Allreduce(sendEndTimes, endTimes, numIndexFiles, MPI_DOUBLE, MPI_SUM, 
				MPI_COMM_WORLD); 
	minMax[0] = min; 
	minMax[1] = max; 
	free(sendEndTimes); 
	return 0; 
}

/* This finds the right bin index for the write size by dividing by two
 * and seeing how many powers of two are within the input number */
int
powersOfTwo(int input) 
{
	int powers = 0; 
	while (input > 2)  
	{
		input /= 2; 
		powers ++; 
	}
	return powers; 
}

/* Gains all the information needed for the graphs and finds the 
 * standard deviation of the end times*/
int
parseData(int numIndexFiles, int size, const char* mount, double binSize,
		int numBins, double* bandwidths, int* iosTime, int* iosFin, 
		int* writeCount, double min, double average, double* stdev)
{
	double* sendBandwidths = (double *)calloc(numBins, sizeof(double)); 
	int* sendIOsTime = (int *)calloc(numBins, sizeof(int)); 
	int* sendIOsFin = (int *)calloc(numBins, sizeof(int)); 	
	int* sendWriteCount = (int *)calloc(51, sizeof(int)); 
	double sendSumDiffSquare = 0; 
	double sumDiffSquare = 0; 
	if (sendBandwidths == NULL | sendIOsTime == NULL | sendIOsFin == NULL 
			| sendWriteCount == NULL) 
	{
		printf("Could not allocate enough memory\n"); 
		return -1; 
	}
	char buffer[128];
	char name[50];
	int rank, i, retv; 
	FILE* tmp; 
	int id, offset, length, tail; 	
	char io; 
	double beg, end, delta, averageBan; 
	char* id2, chunk; 
	int startBin, binsSpanned; 
	MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
	double fileEnd = DBL_MIN; 
	for (i = 0; i<numIndexFiles; i++) {
		if (i % size == rank) {
			/*Create a temporary file */
			int k = sprintf(name, "tempFile%d.txt", rank); 
			tmp = fopen(name, "w+"); 
			if (tmp == NULL) {
				printf("ERROR:Could not open temp file\n"); 
				return -1; 
			}
			retv = container_dump_index(tmp, mount, 0, 1, i); 
			fclose(tmp); 
			if (retv == 0) {
				tmp = fopen(name, "r"); 
				if (tmp == NULL) {
					printf("ERROR:Could not open temp file\n"); 
					return -1; 
				}
				while( fgets(buffer, sizeof(buffer), tmp) != NULL)
				{
					if (buffer[0] != '#') 
					{
						int n = sscanf(buffer, 
								"%d %c %d %d %lf %lf %d %s %s",
								&id, &io, &offset, &length, 
								&beg, &end, &tail, id2, chunk);
						/* this id was arbitrary so I will change it to
					 	* match the index id, which is i */
						id = i; 
						delta = end - beg; 
						binsSpanned = ceil((delta/binSize));
						averageBan = length/(delta*1024*1024); 
						startBin = floor((beg-min)/binSize); 
						for (int j =0; j<binsSpanned; j++) 
						{
							sendBandwidths[startBin+j] += averageBan; 
							if (j < (binsSpanned - 1))
							{
								sendIOsTime[startBin+j] += 1; 
							}
							if (j == (binsSpanned - 1))
							{
								sendIOsFin[startBin+j] += 1; 
							}
						}
						int writeIndex = powersOfTwo(length); 
						/* The last bin is for all that is above 1 PiB */
						if (writeIndex >= 51)
						{
							sendWriteCount[50] ++; 
						}
						else {
							sendWriteCount[writeIndex] ++; 
						}
						if (fileEnd < end) 
						{
							fileEnd = end; 
						}
					}
				}
				fclose(tmp); 
			}
			double diffAvg = fileEnd - average;
			double squareDiff = diffAvg * diffAvg;
			sendSumDiffSquare += squareDiff;
			fileEnd = DBL_MIN; 
		}
		else {
			continue; 
		}
	}

	/* use MPI Reduce to get the sums of each of the arrays */
	MPI_Barrier(MPI_COMM_WORLD); 
	MPI_Reduce(sendBandwidths, bandwidths, numBins, MPI_DOUBLE, 
				MPI_SUM, 0, MPI_COMM_WORLD); 
	MPI_Reduce(sendIOsTime, iosTime, numBins, MPI_INT, MPI_SUM, 
				0, MPI_COMM_WORLD);
	MPI_Reduce(sendIOsFin, iosFin, numBins, MPI_INT, 
				MPI_SUM, 0, MPI_COMM_WORLD);
	MPI_Reduce(sendWriteCount, writeCount, 51, MPI_INT, 
				MPI_SUM, 0, MPI_COMM_WORLD); 
	MPI_Allreduce(&sendSumDiffSquare, &sumDiffSquare, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD); 
	if (rank == 0) {
		/* calcuate standard deviation */
		*stdev = sqrt(sumDiffSquare/numIndexFiles); 
	}
	free(sendBandwidths); 
	free(sendIOsTime); 
	free(sendIOsFin); 
	free(sendWriteCount); 
	unlink(name); 
}

/* utilizes MPI-IO in order to write out the data required to graph
 * the per processor graphs. Writes to a given filesystem so that it
 * can be read by python */
int
writeProcessorData(int numIndexFiles, int size, const char* mount,
					char* offsetMPI, double average, double stdev, 
					double* endTimes, char* timeMPI, double start, 
					bool above, bool below, int numStdDev)
{
	int i, rank, retv; 
	char name[50];
	char buffer[128]; 
	FILE * tmp; 
	long long offset, length, tail, id; 
	char io; 
	double beg, end; 
	const char* id2; const char* chunk;	
	MPI_File offsetsFile; 
	MPI_File timeFile; 
	MPI_Request request; 
	MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
	MPI_File_open(MPI_COMM_WORLD, offsetMPI, 
		MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_INFO_NULL, &offsetsFile); 
	MPI_File_open(MPI_COMM_WORLD, timeMPI, 
		MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_INFO_NULL, &timeFile); 
	int writesOffset = 0; /* the number of offset writes this rank has done */
	int writesTime = 0; 
	double topCutoff, bottomCutoff; 
	if (above) {
		topCutoff = average + numStdDev*stdev; 
		/* any proc that ends above this will be drawn */
	}
	if (below) {
		bottomCutoff = average - numStdDev*stdev; 
		/* any proc that ends before this will be drawn */
	}
	int offsetSize = 3*sizeof(long long); 
	int timeSize = 3*sizeof(double); 
	for (i = 0; i < numIndexFiles; i++) {
		if (i % size == rank) {	
			/* Create a temporary file */
			int k = sprintf(name, "tempFile%d.txt", rank); 
			tmp = fopen(name, "w+"); 
			if (tmp == NULL) {
				printf("ERROR:Could not open temp file\n"); 
				return -1; 
			}
			retv = container_dump_index(tmp, mount, 0, 1, i); 
			fclose(tmp); 
			if (retv == 0) {
				tmp = fopen(name, "r"); 
				if (tmp == NULL) {
					printf("ERROR:Could not open temp file\n"); 
					return -1; 
				}
				while( fgets(buffer, sizeof(buffer), tmp) != NULL)
				{
					if(buffer[0] != '#')
					{
						int n = sscanf(buffer, 
								"%lld %c %lld %lld %lf %lf %lld %s %s", 
								&id, &io, &offset, &length, 
								&beg, &end, &tail, id2, chunk); 
						/* this id was arbitrary so I will change
						* it to match the index id */
						id = (long long)i; 
						MPI_File_write_at(offsetsFile, 
										rank*offsetSize + 
										writesOffset*size*offsetSize, 
										&id, 1, MPI_LONG_LONG, 
										MPI_STATUS_IGNORE); 
						MPI_File_write_at(offsetsFile, rank*offsetSize +
										writesOffset*size*offsetSize +
										sizeof(long long), 
										&offset, 1, MPI_LONG_LONG, 
										MPI_STATUS_IGNORE); 
						MPI_File_write_at(offsetsFile, rank*offsetSize + 
										writesOffset*size*offsetSize + 
										2*sizeof(long long), &tail, 1, 
										MPI_LONG_LONG, MPI_STATUS_IGNORE);
						writesOffset ++; 
						/* see if we need to write this time out */
						if ((numIndexFiles <= 16) ||
							((above) && (endTimes[i] > topCutoff)) || 
							((below) && (endTimes[i] < bottomCutoff))) {
							double newId = (double)i; 
							beg -= start; 
							end -= start; 
							MPI_File_write_at(timeFile, rank*timeSize +
											writesTime*size*timeSize, 
											&newId, 1, MPI_DOUBLE, 
											MPI_STATUS_IGNORE); 
							MPI_File_write_at(timeFile, rank*timeSize + 
											writesTime*size*timeSize +
											sizeof(double), &beg, 1, MPI_DOUBLE,
											MPI_STATUS_IGNORE); 
							MPI_File_write_at(timeFile, rank*timeSize +
											writesTime*size*timeSize + 
											2*sizeof(double), &end,
											1, MPI_DOUBLE, MPI_STATUS_IGNORE);
							writesTime ++; 
						}
					}
				}
				fclose(tmp); 
			}
		}
	}
	unlink(name); 
	MPI_File_close(&offsetsFile); 
	MPI_File_close(&timeFile); 
}

int 
init ( int argc, char *argv[] )
{
	int size = -1; 
	if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
		printf("ERROR: Unable to initialize MPI (MPI_Init). \n");
		return -1; 
	}
	/* return the number of processes */
	MPI_Comm_size(MPI_COMM_WORLD, &size); 
	return size; 
}

int
writeOutputText(char* outputFile, int numBins, double* minMax,
				double binSize, double* bandwidths, int* iosTime,
				int* iosFin, int* writeCount, double average)
{
	FILE* fp = fopen(outputFile, "w"); 	
	if (fp == NULL) {
		printf("Cannot open output file:%s\n", outputFile); 
		return -1; 
	}
	// will use this to separate the sections
	char str[] = "##########################";
	/* write these out so that we can calculate the times array */
	fprintf(fp, "%d %lf %lf %lf\n",numBins, minMax[0], average, binSize); 
	fprintf(fp, "%s\n", str); 
	/* write the bandwidths out */
	for(int i = 0; i < numBins; i++) 
	{
		fprintf(fp, "%lf\n", bandwidths[i]); 
	}
    fprintf(fp, "%s\n", str); 
	/* write the iosTime */
	for (int i = 0; i < numBins; i++) 
	{
		fprintf(fp, "%d\n", iosTime[i]);
	}
    fprintf(fp, "%s\n", str);
	/* write iosFin */
	for (int i = 0; i < numBins; i++)
	{
		fprintf(fp, "%d\n", iosFin[i]);
	}
	fprintf(fp, "%s\n", str); 
	/* write the write Counts */
	for (int i = 0; i < 51; i++) 
	{
		fprintf(fp, "%d\n", writeCount[i]); 
	}
	fclose(fp);
	return 0;
}

int
main( int argc, char *argv[] )
{
	const char* queryFile; 	
	const char* mount; 
	char* jobId; 
	char offsetMPI[128]; 
	char timeMPI[128];
	int c;
	/* with the -p flag, this is set to true and the processor graphs
	 * will be generated */
	bool processorGraph = false; 
	char* outputFile;
	int numBins = 500; 
	int numIndexFiles, index, size; 
	double binSize; 
	double minMax[2];
	double endSum, stdev;
	/* get cutoffs for processor graphs: default is one above */
	bool above = false; 
	bool below = false; 
	int numStdDev = 1;
	while ((c = getopt(argc, argv, "m:q:n:o:pabs:j:")) != -1) {
		switch(c)
			{
			case 'm':
				mount = optarg;
				break;
			case 'q':
				queryFile = optarg; 
				break; 
			case 'o':
				outputFile = optarg;
				break;
			case 'n':
				numBins = atoi(optarg); 
				break;
			case 'p':
				processorGraph = true; 
				break;
			case 'j':
				jobId = optarg; 
				break;
			case 'a':
				above = true; 
				break; 
			case 'b':
				below = true; 
				break; 
			case 's':
				numStdDev = atoi(optarg); 
				break; 
			case '?':
				printf("Unknown option %c\n", optopt); 
				return -1; 
			}
	}
	/* if both below and above are false, make default where above = true */
	if (below == false && above == false) 
	{
		above = true; 
	}
	if (processorGraph)
	{
		strcpy(offsetMPI, outputFile); 
		strcat(offsetMPI, "offsets");
		strcat(offsetMPI, jobId); 
		strcpy(timeMPI, outputFile); 
		strcat(timeMPI, "times"); 
		strcat(timeMPI, jobId); 
	}
	numIndexFiles = getNumberOfIndexFiles(queryFile); 
	if (numIndexFiles == 0) 
	{
		printf("ERROR:Number of Index Files found was %d\n", numIndexFiles);
		return -1; 
	}
	size = init(argc, argv); 
	/* there was a problem so exit */
	if (size == -1) 
	{
		printf("ERROR:The size of the mpi comm world was -1\n"); 
		MPI_Finalize();
		return -1; 
	}
	double* endTimes = (double *)calloc(numIndexFiles, sizeof(double)); 
	if (endTimes == NULL) 
	{
		printf("ERROR:Could not allocate memory for ending Times\n"); 
		return -1; 
	}
	getMaxMinTimes(numIndexFiles, size, mount, minMax, &endSum, endTimes); 
	int rank; 
	double average; 
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	if (rank == 0) {
		average = endSum/numIndexFiles;
	}
	MPI_Bcast(&average, 1, MPI_LONG_DOUBLE, 0, MPI_COMM_WORLD); 
	if (rank == 0) {
		binSize = (minMax[1] - minMax[0])/numBins;
	}
	MPI_Barrier(MPI_COMM_WORLD); 
	MPI_Bcast(&binSize, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD); 
	/* allocate data to place final counts in  */
	double * bandwidths = (double *)calloc(numBins, sizeof(double)); 
	int * iosTime = (int *)calloc(numBins, sizeof(int)); 
	int * iosFin = (int *)calloc(numBins, sizeof(int)); 
	/* we need 51 bins here because there are 10 bins between each 
    size and the last is for those above PiB */
	int * writeCount = (int *)calloc(51, sizeof(int)); 
	if (bandwidths == NULL | iosTime == NULL | iosFin == NULL 
		| writeCount == NULL) 
	{
		printf("Could not allocate data placements\n"); 
		return -1; 
	}
	parseData(numIndexFiles, size, mount, binSize, numBins, 
				bandwidths, iosTime, iosFin, writeCount, minMax[0], 
				average, &stdev); 
	MPI_Bcast(&stdev, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD); 
	int retv; 
	if (processorGraph) 
	{
		writeProcessorData(numIndexFiles, size, mount, offsetMPI,average, stdev,
							endTimes, timeMPI, minMax[0], above, below, 
							numStdDev); 
	}
	if (rank == 0) {
		strcat(outputFile, jobId); 
		strcat(outputFile, "output.txt"); 
		retv = writeOutputText(outputFile,numBins,minMax,binSize,bandwidths,
								iosTime,iosFin,writeCount, average);
		if (retv == -1) {
			return -1; 
		}
	}
	free(bandwidths); 
	free(iosTime); 
	free(iosFin); 
	free(writeCount);
	MPI_Finalize(); 
	return 0; 
}