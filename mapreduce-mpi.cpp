#include <iostream>
#include <string>
#include <omp.h>
#include <map>
#include <vector>
#include <stdio.h>
#include <fstream>
#include <algorithm>
#include <cstdio>
#include <dirent.h>
#include <unistd.h>
#include <stdlib.h>
#include <queue>
#include <mpi.h>
#include <string.h>
#include <mutex>
#include <time.h>
#include <sys/stat.h>

#define MAX_REDUCERS 16

#define BUCKET_SIZE 500 //500 buckets for storing the hashMap.

using namespace std;

struct hashMapEntry
{
	string word;
	int count;
};

vector<string> filenames;
queue<struct hashMapEntry> reducerQueue[MAX_REDUCERS];
int maxThreads;
int startReducer[MAX_REDUCERS];
int mapperDone[MAX_REDUCERS];
int sendCompleted;
std::mutex m;

//Can optimize further by ensuring that the sizes are distributed well. 


struct hme_mpi
{
	char word[64];
	int count;
};

MPI_Datatype makeType()
{
	struct hme_mpi temp;
	MPI_Datatype hme_struct;
	int blocklens[2];
	MPI_Aint indices[2];
	MPI_Datatype old_types[2];
	blocklens[0] = 32;
	blocklens[1] = 1;
	old_types[0] = MPI_CHAR;
	old_types[1] = MPI_INT;
	MPI_Address(&temp.word, &indices[0]);
	MPI_Address(&temp.count, &indices[1]);
	indices[1] = indices[1] - indices[0];
	indices[0] = 0;
	MPI_Type_struct(2, blocklens, indices, old_types, &hme_struct);
	MPI_Type_commit(&hme_struct);
	return hme_struct;
}

int hashFunction(string word)
{
	int hashVal = 0;
	for(int i = 0; i < word.length(); i++)
	{
		hashVal += word.at(i);
	}
	if(hashVal < 0)
		hashVal = hashVal * -1;
	//cout << "Word = " << word << ", HashVal = " << to_string(hashVal) << endl;
	return hashVal % BUCKET_SIZE;
}

void readFile(string filename)
{
	int pid;
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);
	cout << "Thread = " << omp_get_thread_num() << " is reading file " << filename << endl;
	#if 1
	std::ifstream infile;
	map<string, int> wordCount;
	map<int, vector<struct hashMapEntry>> hashMap; //Local HashMap
	infile.open(filename);
	string word;
	int count = 0;
	clock_t start, end;
	start = clock();
	while(infile >> word)
	{
		//Removing the punctuations
		word.erase(std::remove_if(word.begin(), word.end(), [](unsigned char c) { return std::ispunct(c);}), word.end());
		//count+=1;
		//Converting to small case to maintain uniformity
		std::transform(word.begin(), word.end(), word.begin(), ::tolower);
		++wordCount[word];
	}
	infile.close();
	end = clock();
	//cout << "Total words in file " << filename << " = " << count << endl;
	cout << "Pid = " << pid << ", tid : " << omp_get_thread_num() << ": wordcoutn size = " << wordCount.size() << endl;
	cout << "Tiem for reading " << filename << " = " << (double)(end - start) / (double)CLOCKS_PER_SEC << endl;
//	map<std::string, int>::iterator itr;
//	itr = wordCount.begin();
	
	//for(auto itr : wordCount)
	//for(itr = wordCount.begin(); itr != wordCount.end(); itr++)
	//#pragma omp parallel for num_threads(2)
	start = clock();
	for(int i = 0; i < wordCount.size(); i++)
	{
		
		map<std::string, int>::iterator itr = wordCount.begin();
		std::advance(itr, i);
		//cout << itr.first , ", " << itr.second << endl;
		
		int hashVal = hashFunction(itr->first);
		struct hashMapEntry hme;
		hme.word.assign(itr->first);
		hme.count = itr->second;
		MPI_Datatype tempData = makeType() ;

		struct hme_mpi temp ; 
		memset(temp.word, 0, 64 * sizeof(char));
		strncpy(temp.word, hme.word.c_str(), strlen(hme.word.c_str()));
		temp.count = hme.count;
		#pragma omp critical
		{
			hashMap[hashVal].push_back(hme);
			int reducer_id = hashVal % maxThreads;
			MPI_Send(&temp, sizeof(temp), tempData, reducer_id, 1, MPI_COMM_WORLD);
			//Putting to the corresponding reducer
			
			//cout << "Sedning now" << endl;
			
			//cout << "Send" << endl;
			//cout << "reducer_id = " << reducer_id << ", hashVal = " << to_string(hashVal) << endl;
			//reducerQueue[reducer_id].push(hme);
			//startReducer[omp_get_thread_num()] = 1;
		}
		
		
		
		//cout << "Word = " << itr->first << ", Count = " << itr->second << endl;
	}
	end = clock();
	cout << "Tiem for communicating the contents of the file " << filename << " to reducers = " << (double)(end - start) / (double)CLOCKS_PER_SEC << endl;
	cout << "pid = " << pid << ",tid : " << omp_get_thread_num() << ": Hashmap size = " << hashMap.size() << endl;
	mapperDone[omp_get_thread_num()] = 1;
	//int testHashVal = hashFunction("you");
	//cout << "testHashVal = " << testHashVal << endl;
	//for(int i = 0; i < hashMap[testHashVal].size(); i++)
	//{
//		cout << "Word = " << hashMap[testHashVal].at(i).word << ", count = " << hashMap[testHashVal].at(i).count << endl;
//	}
	cout << "pid = " << pid << "Thread " << omp_get_thread_num() << " finished reading file : " << filename << endl;
	//cout << "reducerQueue[" << omp_get_thread_num() << "] = " << reducerQueue[omp_get_thread_num()].size() << endl;
	#endif
	
	/*for(int i = 0; i < hashMap[testHashVal].size(); i++)
	{
		cout << "Thread " << omp_get_thread_num() << ", file : " << filename <<  ", word = " << hashMap[testHashVal].at(i).word << ", count = " << 
		hashMap[testHashVal].at(i).count << endl;
		
	}*/
	//return;
}

long GetFileSize(std::string filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

int getNumFiles(string directory)
{
	struct dirent* dir;
	DIR* curDir;
	curDir = opendir(directory.c_str());
	if(curDir == NULL)
	{
		cout << "Opening dir failed" << endl;
		return -1;
	}
	else
	{
		while((dir=readdir(curDir)) != NULL)
		{
			if((string(dir->d_name).compare(".") == 0) || (string(dir->d_name).compare("..") == 0))
			{
				continue;
			}
			//cout << dir->d_name << endl;
			filenames.push_back(string(dir->d_name));
			cout << "filesize of " << dir->d_name << " is " << GetFileSize("files/"+string(dir->d_name)) << endl;
		}
	}
	closedir(curDir);
	return filenames.size();
}

int reducer(int tid)
{
	std::map<string, int> wordCount;
	//check for null in vector
	#pragma omp critical
	cout << "Reducer Thread : " << omp_get_thread_num() << endl;
	
	while(reducerQueue[omp_get_thread_num()].empty() == 1);
	
	while((reducerQueue[omp_get_thread_num()].size() != 0) || (mapperDone[omp_get_thread_num()] == 0))
	{
		while(reducerQueue[omp_get_thread_num()].empty() == 1);
		struct hashMapEntry hme;
		#pragma omp critical
		{
			hme = reducerQueue[omp_get_thread_num()].front();
			reducerQueue[omp_get_thread_num()].pop();
			wordCount[hme.word] += hme.count;
		}
		//cout << "redQ[0] size = " << reducerQueue[omp_get_thread_num()].size() << endl;
	} 
	
	string filename = "reducer_" + to_string(omp_get_thread_num()) + ".txt" ;
	ofstream out;
	out.open(filename);
	
	for(auto i : wordCount)
	{
		out << "word = " << i.first << ", count = " << i.second << endl;
	}
	out.close();
	//cout << "wordcount[world] = " << wordCount["world"] << endl;
	return 0;
}



void reducerThread(int reducerID)
{
	cout << "Started the reducer thread " << to_string(reducerID) << endl; 
	ofstream out;
	out.open("reducer_mpi_" + to_string(reducerID) + ".txt");
	MPI_Datatype mpi_datatype = makeType();
	MPI_Status stat;
	struct hme_mpi temp;;
	while(sendCompleted != 1)
	{
		//cout << "Waitign for receiev" << endl;
		int rc = MPI_Recv(&temp, sizeof(temp), mpi_datatype,  MPI_ANY_SOURCE, 1 , MPI_COMM_WORLD, &stat);
		out << "Received data = " << ", name = " << temp.word << ", count = " << temp.count << endl;
		if(temp.count == -100)
		{
			break;
		}
	}
	out.close();
	cout << "Reducer Thread " << reducerID << " is returning from reduceFunction" << endl;
	//return;
} 


int main(int argc, char* argv[])
{
	if(argc != 2)
	{
		cout << "usage : ./mapreduce <numThreads>" << endl;
		exit(0);
	}
	int sendCompleted = 0;
	int numP;
	int pid;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numP);
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);
	clock_t start_c, end_c;
	int numThreads = atoi(argv[1]);
	maxThreads = numThreads;
	omp_set_nested(1);
	omp_set_num_threads(numThreads);
	cout << "Hello" << endl;
	int numMappers = numP / 2;
	int numReducers = numMappers;
	
	cout << "Pid : " << pid << ", numMappers = " << numMappers << ", numReducers = " << numReducers << endl;
	int numFiles = getNumFiles("files/");
	int filesPerMapper = numFiles / numMappers;
	int scaledMapperPid = pid - numMappers;
	
	if( pid == 0)
	{
		cout << "Master Thread" << endl;
	}
	else
	{
		cout << "Pid = " << pid << endl;
	}
	
	double start, end;
	if(pid == 0)
	{
		start = MPI_Wtime();
		start_c = clock();
	}
	
	if(pid >= numMappers)
	{
		cout << "Pid : " << pid << "This is a mapper Thread" << endl;
		#pragma omp parallel 
		{
			#pragma omp master
			{
				#pragma omp task
				{
					cout << "starting parallely" << endl;
				}
					
					
				#pragma omp task
				{
					//cout << "starting parallely" << endl;
						//sleep(5);
					
					cout << "Pid = " << pid << ", FilePerMapper = " << filesPerMapper << ", scaledMapperPid = " << scaledMapperPid << endl;
					int startFileNum = filesPerMapper * scaledMapperPid;
					int endFileNum;
					if(scaledMapperPid == numMappers - 1)
						endFileNum = numFiles;
					else
						endFileNum = (filesPerMapper * scaledMapperPid) + filesPerMapper; 
					cout << "Pid = " << pid << ", startFileNum = " << startFileNum << ", endFileNum = " << endFileNum << endl;
					#pragma omp parallel for num_threads(numThreads)
					//#pragma omp master
					for(int i = startFileNum; i < endFileNum; i++)
					{
						//#pragma omp critical
						cout << "pid = " << pid << ", tid = " << omp_get_thread_num() << " , file = " << filenames.at(i) << endl;
						readFile("files/"+ filenames.at(i));
					}
						/*MPI_Datatype  data = makeType();
						struct hme_mpi temp;
						temp.count = -100;
						//MPI_Send(&temp, sizeof(temp), tempData, 0, 1, MPI_COMM_WORLD);
						
						
						cout << "pid : " << pid << "Sending stop to reducer " << pid - numMappers << endl;
						int rc = MPI_Send(&temp, sizeof(temp), data, pid - numMappers, 1, MPI_COMM_WORLD);
						*/
						
						//readFile("files/15.txt");
				}
				
			}
		}
		
	}
	else
	{
		cout << "Pid : " << pid << "This is a reducer Thread" << endl;
		reducerThread(pid);
		MPI_Comm_rank(MPI_COMM_WORLD, &pid);
		cout << "Pid : " << pid << ", Reducer thread returned" << endl;
		goto end;
		/*cout << "Started the reducer thread " << to_string(pid) << endl; 
		ofstream out;
		out.open("reducer_mpi_" + to_string(pid) + ".txt");
		MPI_Datatype mpi_datatype = makeType();
		MPI_Status stat;
		struct hme_mpi temp;;
		while(1)
		{
			//cout << "Waitign for receiev" << endl;
			int rc = MPI_Recv(&temp, sizeof(temp), mpi_datatype,  MPI_ANY_SOURCE, 1 , MPI_COMM_WORLD, &stat);
			if(temp.count == -100)
			{
				break;
			}
			//out << "Received data = " << ", name = " << temp.word << ", count = " << temp.count << endl;
		}
		out.close();
		cout << "Pid : " << pid << ", Reducer thread returned" << endl;*/
	}
	
	
	if(pid >= numMappers)
	{
		MPI_Datatype  data = makeType();
		struct hme_mpi temp;
		temp.count = -100;
		cout << "pid = " << pid << ", stoping reducer thread " << pid - numMappers << endl;
		int rc = MPI_Send(&temp, sizeof(temp), data, pid - numMappers, 1, MPI_COMM_WORLD);
		
	}
	end : 
	cout << "PId = " << pid << ", at barrier" << endl;
	//MPI_Barrier(MPI_COMM_WORLD);
	//end_c = clock();
	//cout << "Pid = " << pid << ",Time elapsed = " << (end_c - start_c)/CLOCKS_PER_SEC << endl;;
	if(pid == 0)
	{
		end = MPI_Wtime();
		end_c = clock();
		cout << "Pid = " << pid << ",Time elapsed = " << (end_c - start_c)/CLOCKS_PER_SEC << endl;;
	}
	//MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	return 0;
}
