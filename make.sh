echo "Compiling now"
#module load gcc
#g++ -fopenmp -o mapreduce mapreduce.cpp

module load intel 
mpicxx mapreduce-mpi.cpp -fopenmp -std=c++11 -o mapreduce_mpi

