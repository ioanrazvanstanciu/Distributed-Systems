#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

// printez mesajul
void printMessage(int from, int to)
{
	printf("M(%d,%d)\n", from, to);
}

// functie apelata de workeri ca sa imi dea topologia
void printTopology(int size, int *v, int source)
{
	printf("%d -> ", source);
	int first = 0;

	for (int i = 0; i < 3; i++)
	{
		first = 0;
		printf("%d:", i);
		for (int j = 3; j < size; j++)
		{
			if (v[j] == i)
			{
				if (first == 0)
				{
					printf("%d", j);
					first = 1;
				}
				else
				{
					printf(",%d", j);
				}
			}
		}
		printf(" ");
	}

	printf("\n");
}

// functie care imi populeaza vectorul de copii din fisiere pentru fiecare vector
void populateVect(int *children, int *noChildren, int rank, char *fileName)
{
	// citesc fisierul
	FILE *fp;
	char buff[255];
	int nr;
	int node;
	fp = fopen(fileName, "r");
	fscanf(fp, "%d\n", &nr);
	// citesc copiii si pun in vectorul de parinti
	noChildren[rank] = nr;
	for (int i = 0; i < nr; i++)
	{
		fscanf(fp, "%d\n", &node);
		children[node] = rank;
	}
}

// functie care primeste un vector de la un alt cluster si il pune la clusterul meu actual
void receiveTopo(int *v, int rS, int rR, int size)
{
	int *recV;
	recV = malloc(size * sizeof(int));
	MPI_Recv(recV, size, MPI_INT, rS, 0, MPI_COMM_WORLD, NULL);
	for (int i = 3; i < size; i++)
	{
		if (recV[i] == rS)
			v[i] = rS;
	}
}

// functie care imi copiaza o topologie
void ansamblTopo(int *v, int rS, int rR, int size)
{
	int *recV;
	recV = malloc(size * sizeof(int));
	MPI_Recv(recV, size, MPI_INT, rS, 0, MPI_COMM_WORLD, NULL);
	for (int i = 3; i < size; i++)
	{
		if (v[i] != rR)
			v[i] = recV[i];
	}
}

// functie care copiaza o topologie cu totul
void copyTopo(int *v, int rS, int size)
{
	int *recV;
	recV = malloc(size * sizeof(int));
	MPI_Recv(recV, size, MPI_INT, rS, 0, MPI_COMM_WORLD, NULL);
	for (int i = 3; i < size; i++)
	{
		v[i] = recV[i];
	}
}

// functia cu care workerii imi copiaza topologia
void copyTopoWorker(int *v, int size)
{
	int *recV;
	recV = malloc(size * sizeof(int));
	MPI_Recv(recV, size, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, NULL);
	for (int i = 3; i < size; i++)
	{
		v[i] = recV[i];
	}
}

// functie in care un worker imi primeste pozitiile de calcul,
// imi calculeaza vectorul si imi trimite inapoi la parinte
void receiveModify(int *v, int rank, int dimV, int *parent)
{
	int start_index, end_index;
	MPI_Recv(&start_index, 1, MPI_INT, parent[rank], 0, MPI_COMM_WORLD, NULL);
	MPI_Recv(&end_index, 1, MPI_INT, parent[rank], 0, MPI_COMM_WORLD, NULL);
	MPI_Recv(v, dimV, MPI_INT, parent[rank], 0, MPI_COMM_WORLD, NULL);

	for (int i = start_index; i < end_index; i++)
	{
		v[i] = v[i] * 2;
	}
	MPI_Send(v, dimV, MPI_INT, parent[rank], 0, MPI_COMM_WORLD);
	printMessage(rank, parent[rank]);
}

// printarea vectorului final
void printVect(int size, int *v)
{
	for (int i = 0; i < size; i++)
	{
		printf("%d ", v[i]);
	}
	printf("\n");
}

int main(int argc, char *argv[])
{
	int numtasks, rank;
	int parent[40];
	int noC[3] = {0, 0, 0};
	int *v;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	MPI_Status status;

	// initializez cu -1
	for (int i = 0; i < numtasks; i++)
	{
		parent[i] = -1;
	}

	// rankurile 0, 1 si 2 isi formeaza vectorii partiali de workeri
	if (rank == 0)
	{
		populateVect(parent, noC, rank, "cluster0.txt");
	}
	else if (rank == 1)
	{
		populateVect(parent, noC, rank, "cluster1.txt");
	}
	if (rank == 2)
	{
		populateVect(parent, noC, rank, "cluster2.txt");
	}

	// rankul 0 trimite vectorul de parinti la clusterul 1
	if (rank == 0)
	{
		MPI_Send(parent, numtasks, MPI_INT, 1, 0, MPI_COMM_WORLD);
		printMessage(0, 1);
	}

	// clusterul 1 primeste topologia clusterului 0 si o adauga la acestuia
	// si trimite vectorul partial mai departe la clusterul 2
	if (rank == 1)
	{
		receiveTopo(parent, 0, 1, numtasks);
		MPI_Send(parent, numtasks, MPI_INT, 2, 0, MPI_COMM_WORLD);
		printMessage(1, 2);
	}

	// clusterul 2 primeste topologia clusterelor 0 si 1 si o adauga la a lui
	// pe care o trimite finala la rankul 0
	if (rank == 2)
	{
		receiveTopo(parent, 1, 2, numtasks);
		MPI_Send(parent, numtasks, MPI_INT, 0, 0, MPI_COMM_WORLD);
		printMessage(2, 0);
	}

	// rankul 0 face topologia finala si o trimite la rankul 1
	if (rank == 0)
	{
		ansamblTopo(parent, 2, 0, numtasks);
		printTopology(numtasks, parent, rank);
		MPI_Send(parent, numtasks, MPI_INT, 1, 0, MPI_COMM_WORLD);
		printMessage(0, 1);
	}

	// rankul 1 copiaza topologia finala si o trimite la rankul 2
	if (rank == 1)
	{
		copyTopo(parent, 0, numtasks);
		printTopology(numtasks, parent, 1);
		MPI_Send(parent, numtasks, MPI_INT, 2, 0, MPI_COMM_WORLD);
		printMessage(1, 2);
	}

	// rankul 2 copiaaza topologia si o afiseaza
	if (rank == 2)
	{
		copyTopo(parent, 1, numtasks);
		printTopology(numtasks, parent, 2);
	}

	// fiecare parinte trimite topologia la copii sai
	for (int j = 3; j < numtasks; j++)
	{
		if (rank == parent[j])
		{
			MPI_Send(parent, numtasks, MPI_INT, j, 0, MPI_COMM_WORLD);
			printMessage(rank, j);
		}
	}

	// fiecare copil primeste topologia de la parinte si o afiseaza
	for (int i = 3; i < numtasks; i++)
	{
		if (rank == i)
		{
			copyTopoWorker(parent, numtasks);
			printTopology(numtasks, parent, rank);
		}
	}

	int dimV = atoi(argv[1]);
	v = calloc(dimV, sizeof(int));

	// daca rankul este 0 atunci
	// creez vectorul si trimit vectorul la
	// rankurile 1 si 2
	if (rank == 0)
	{
		for (int i = 0; i < dimV; i++)
		{
			v[i] = i;
		}
		MPI_Send(v, dimV, MPI_INT, 1, 0, MPI_COMM_WORLD);
		printMessage(0, 1);
		MPI_Send(v, dimV, MPI_INT, 2, 0, MPI_COMM_WORLD);
		printMessage(0, 2);
	}

	// rankurile 1 si 2 primesc vectorul trimis de rankul 0
	if (rank == 1 || rank == 2)
	{
		MPI_Recv(v, dimV, MPI_INT, 0, 0, MPI_COMM_WORLD, NULL);
	}

	int start_index, end_index;

	// daca rankurile sunt de parinti
	if (rank == 0 || rank == 1 || rank == 2)
	{
		for (int j = 3; j < numtasks; j++)
		{
			// trimit partile de calculat la lucrator
			if (rank == parent[j])
			{
				start_index = (j - 3) * (double)dimV / (numtasks - 3);
				end_index = (j - 3 + 1) * (double)dimV / (numtasks - 3);
				MPI_Send(&start_index, 1, MPI_INT, j, 0, MPI_COMM_WORLD);
				printMessage(rank, j);
				MPI_Send(&end_index, 1, MPI_INT, j, 0, MPI_COMM_WORLD);
				printMessage(rank, j);
				MPI_Send(v, dimV, MPI_INT, j, 0, MPI_COMM_WORLD);
				printMessage(rank, j);
				MPI_Recv(v, dimV, MPI_INT, j, 0, MPI_COMM_WORLD, NULL);
			}
		}
	}

	// lucratorii primesc vectorul si isi modifica partea aferenta
	if (rank != 0 && rank != 1 && rank != 2)
	{
		receiveModify(v, rank, dimV, parent);
	}

	// daca sunt parintii 1 si 2 atunci trimit la rankul 0
	if (rank == 1 || rank == 2)
	{
		MPI_Send(v, dimV, MPI_INT, 0, 0, MPI_COMM_WORLD);
		printMessage(rank, 0);
	}

	// rankul 0 asambleaza vectorul final si afiseaza vectorul
	if (rank == 0)
	{
		int *copyV = calloc(dimV, sizeof(int));
		int *copyV2 = calloc(dimV, sizeof(int));

		MPI_Recv(copyV, dimV, MPI_INT, 1, 0, MPI_COMM_WORLD, NULL);
		MPI_Recv(copyV2, dimV, MPI_INT, 2, 0, MPI_COMM_WORLD, NULL);
		for (int i = 0; i < dimV; i++)
		{
			if (copyV[i] == copyV2[i])
				continue;
			if (v[i] != copyV[i])
			{
				v[i] = copyV[i];
			}
			else if (v[i] != copyV2[i])
			{
				v[i] = copyV2[i];
			}
		}
		printf("Rezultat: ");
		printVect(dimV, v);
	}

	MPI_Finalize();
}
