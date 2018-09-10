#include "mpi.h"
#include "unistd.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
 
int main (int argc, char *argv[])
{
#define MAXCMD 100000
   int i, lencmd;
   char cmd[MAXCMD];
   char rank[10];
   int mpi_rank;

   MPI_Init(&argc, &argv);

   lencmd = 0;
   for (i = 1; i < argc; i++) {
     lencmd += (strlen(argv[i]) + 1);
   }
   if (lencmd > (MAXCMD - 1)) {
     printf("command too long\n");
     return(0);
   }

   strcpy(cmd, "");
   for (i = 1; i < argc; i++) {
     strcat(cmd, argv[i]);
     strcat(cmd, " ");
   }

   MPI_Comm_rank(MPI_COMM_WORLD,&mpi_rank);
   sprintf(rank,"%d",mpi_rank);
   strcat(cmd, rank);
  
   //printf("cmd=%s\n", cmd);

   system(cmd);
 
   MPI_Finalize();
 
   return(0);
}
