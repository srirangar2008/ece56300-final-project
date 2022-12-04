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
#include <sys/time.h>

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
	cout << "Thread = " << omp_get_thread_num() << " is reading file " << filename << endl;
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
	end = clock();
	cout << "Thread num : " << omp_get_thread_num() << " - Time taken to read filename " << filename << " = " << ((double)(end - start)/CLOCKS_PER_SEC) << endl;
	//cout << "Total words in file " << filename << " = " << count << endl;
	//cout << omp_get_thread_num() << ": wordcoutn size = " << wordCount.size() << endl;
//	map<std::string, int>::iterator itr;
//	itr = wordCount.begin();
	
	//for(auto itr : wordCount)
	//for(itr = wordCount.begin(); itr != wordCount.end(); itr++)
	//#pragma omp parallel for num_threads(2)
	clock_t start_m, end_m;
	#pragma omp critical
	start_m = clock();
	for(int i = 0; i < wordCount.size(); i++)
	{
		
		map<std::string, int>::iterator itr = wordCount.begin();
		std::advance(itr, i);
		//cout << itr.first , ", " << itr.second << endl;
		
		int hashVal = hashFunction(itr->first);
		struct hashMapEntry hme;
		hme.word.assign(itr->first);
		hme.count = itr->second;
		#pragma omp critical
		{
			hashMap[hashVal].push_back(hme);
			//Putting to the corresponding reducer
			int reducer_id = hashVal % maxThreads;
			//cout << "reducer_id = " << reducer_id << ", hashVal = " << to_string(hashVal) << endl;
			reducerQueue[reducer_id].push(hme);
			//startReducer[omp_get_thread_num()] = 1;
		}
		//cout << "Word = " << itr->first << ", Count = " << itr->second << endl;
	}
	#pragma omp critical
	end_m = clock();
	cout << "Thread Num : " << omp_get_thread_num() << " - Time taken for mapping words of file : " << filename << " = " << ((double)(end_m - start_m)/CLOCKS_PER_SEC) << endl;
	//cout << omp_get_thread_num() << ": Hashmap size = " << hashMap.size() << endl;
	mapperDone[omp_get_thread_num()] = 1;
	//int testHashVal = hashFunction("you");
	//cout << "testHashVal = " << testHashVal << endl;
	//for(int i = 0; i < hashMap[testHashVal].size(); i++)
	//{
//		cout << "Word = " << hashMap[testHashVal].at(i).word << ", count = " << hashMap[testHashVal].at(i).count << endl;
//	}
	//cout << "Thread " << omp_get_thread_num() << " finished reading file : " << filename << endl;
	//cout << "reducerQueue[" << omp_get_thread_num() << "] = " << reducerQueue[omp_get_thread_num()].size() << endl;
	/*for(int i = 0; i < hashMap[testHashVal].size(); i++)
	{
		cout << "Thread " << omp_get_thread_num() << ", file : " << filename <<  ", word = " << hashMap[testHashVal].at(i).word << ", count = " << 
		hashMap[testHashVal].at(i).count << endl;
		
	}*/
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
			cout << dir->d_name << endl;
			filenames.push_back(string(dir->d_name));
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

int main(int argc, char* argv[])
{
	if(argc != 2)
	{
		cout << "usage : ./mapreduce <numThreads>" << endl;
		exit(0);
	}
	
	int numThreads = atoi(argv[1]);
	maxThreads = numThreads;
	omp_set_nested(1);
	omp_set_num_threads(numThreads);
	cout << "Hello" << endl;
	
	double start, end;
	start = -omp_get_wtime();
	
	
	/*#pragma omp master
	{
		cout << "In the master thread. STarting the tasks now" << endl;
		#pragma omp parallel for
		for(int i = 0; i < filenames.size(); i++)
		{
			sleep(omp_get_thread_num());
			int tid = omp_get_thread_num();
			cout << "Thread ID = " << tid << ", filename = " << filenames.at(i) << endl;
			readFile("files/" + filenames.at(i));
		}
	}*/
	//Start reading and the mappers
	//STart the reducer in parallel
	int numFiles = getNumFiles("files/");
	#pragma omp parallel 
	{
		#pragma omp master
		{
			#pragma omp task
			{
					//cout << "starting paralelely" << endl;
					//sleep(5);
				#pragma omp parallel for num_threads(numThreads)
				//#pragma omp master
				for(int i = 0; i < numFiles; i++)
				{
					#pragma omp critical
					cout << "tid = " << omp_get_thread_num() << " , file = " << filenames.at(i) << endl;
					readFile("files/"+ filenames.at(i));
				}
					//readFile("files/15.txt");
			}
			
			
		}
		#pragma omp task
			{
				//cout << "starting task2 parallely" << endl;
				//#pragma omp parallel for num_threads(numThreads)
				//for(int i = 0; i < numThreads; i++)
				reducer(omp_get_thread_num());
		
			}
	}
		
	
	
	end = omp_get_wtime();
	cout << "Time elapsed = " << end + start << endl;;
	//readFile("files/1.txt");
	return 0;
}
