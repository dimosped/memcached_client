// Misc helper functions
#include "util.h"
#include "mt.h"
#include "worker.h"
#include "mt19937p.h"

// Let's us replace the  random function
int randomFunction(){
  //int rand_int =  genrand_int32();
//  printf("rand int %d\n", rand_int);
  int rand_int = rand();
  return rand_int;
}

// Let's us replace the  random function
int parRandomFunction(struct worker* worker){
  //return rand();
//  return genrand_int32();
//  printf("generating rand...");
  int rand_int = genrand(&(worker->myMT19937p));
  //printf("rand int %d\n", rand_int);
  //int rand_int = rand();
//  printf("Done\n");
  return rand_int;
}//End randomFunction()

double round(double d) {
  return floor(d + 0.5);
}

void timingTests() {

  timestampTest();
  lockTest();

}

void lockTest() {

  int nIterations = 100000;
  pthread_mutex_t lock;

  struct timeval timestamp1;
  int i;
  gettimeofday(&timestamp1, NULL);
  for(i = 0; i < nIterations; i++){
    pthread_mutex_lock(&lock);  
    pthread_mutex_lock(&lock);  
  }
  struct timeval timestamp2;
  gettimeofday(&timestamp2, NULL);
  double diff = (timestamp2.tv_sec - timestamp1.tv_sec) + 1e-6*(timestamp2.tv_usec - timestamp1.tv_usec);
  double time = diff/(float)nIterations;
 
  printf("Lock/unlock time %.9f \n", time);

}


void timestampTest() {


  printf("--- Timestamp test ---\n");

  int i;
  struct stat res;
  int nIterations = 100000;
  for( i = 0; i < nIterations; i++) {
    struct timeval timestamp1;
    gettimeofday(&timestamp1, NULL);
    struct timeval timestamp2;
    gettimeofday(&timestamp2, NULL);
    double diff = (timestamp2.tv_sec - timestamp1.tv_sec) + 1e-6*(timestamp2.tv_usec - timestamp1.tv_usec);
    addSample(&res, diff);
  }
  printf("Timestamp resolution %.9f s\n", getAvg(&res));


}

//Translate a hostname to an IP address
char* nslookup(char* hostname){

  struct hostent* host_info = 0 ;
  int attempt;
  for( attempt=0 ; (host_info==0) && (attempt<3) ; ++attempt ) {
    host_info = gethostbyname(hostname) ;
  }

  char* ip_address;
  if(host_info){

    struct in_addr * address = (struct in_addr * )host_info->h_addr;
    ip_address = inet_ntoa(* address);

    printf("host: %s\n", host_info->h_name);
    printf("address: %s\n",ip_address);

  } else {
    printf("DNS error\n");
    exit(-1);
  }

  //pretty sure i should free something here... 
  return ip_address;

}//End nslookup

void writeBlock(int fd, void* buffer, int writeSize) {

#if DEBUG_READ_WRITE
  printf("Going to write %d bytes\n", writeSize);
#endif

  int nWriteTotal = 0;
  int nWrite = 0;

#if DEBUG_READ_WRITE
  printf("Writing:\n");
  int i;
  for(i = 0; i < writeSize; i++){
    printf("%8x ", *((unsigned char*)buffer+i));
    if(i % 4 == 3 && i != 0){
     printf("\n");
    }
  }
  printf("\n");
#endif

  while(nWriteTotal != writeSize){
    nWrite = write(fd, buffer + nWriteTotal, writeSize - nWriteTotal);
    if(nWrite < 0) {
      printf("Write error: %d\n", nWrite);
      perror("Write error");
      exit(-1);
    }
    nWriteTotal += nWrite;
  }

  if(nWriteTotal != writeSize){
    printf("Write block failed\n");
    exit(-1);
  }

#if DEBUG_READ_WRITE
  printf("Done with write block\n");
#endif

}//End writeBlock()

void readBlock(int fd, void* buffer, int readSize) {

#if DEBUG_READ_WRITE
  printf("Going to read %d bytes\n", readSize);
#endif

  int nReadTotal = 0;
  int nRead = 0;
  while(nReadTotal != readSize){
    nRead = read(fd, buffer + nReadTotal, readSize - nReadTotal);
    if(nRead < 0) {
      perror("Read error");
      exit(-1);
    }
    nReadTotal += nRead;
  }

#if DEBUG_READ_WRITE
  printf("Reading:\n");
  int i;
  for(i = 0; i < readSize; i++){
    printf("%8x ", *((unsigned char*)buffer+i));
    if(i % 4 == 3 && i != 0){
     printf("\n");
    }
  }
  printf("----\n");
#endif

  if( nReadTotal != readSize){
    printf("Read block failed\n");
    exit(-1);
  }

#if DEBUG_READ_WRITE
  printf("Done with read block\n");
#endif

}//End readBlock()


#if defined(__i386__)

__inline__ unsigned long long rdtsc(unsigned long long *pTstampVal)
{
     __asm__ volatile (".byte 0x0f, 0x31" : "=A" (*pTstampVal));
     return x;
}
#elif defined(__x86_64__)


__inline__ void myRdtsc(unsigned long long *pTstampVal)
{
  //unsigned hi, lo;
  //__asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi));
  //*pTstampVal =  ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );

  unsigned cycles_low, cycles_high;
  asm volatile("RDTSCP\n\t"
          "mov %%edx, %0\n\t"
          "mov %%eax, %1\n\t"
          "CPUID\n\t": "=r" (cycles_high), "=r"(cycles_low):: "%rax", "%rbx", "%rcx", "%rdx");

  *pTstampVal = ( (unsigned long long)cycles_low)|( ((unsigned long long)cycles_high)<<32 );
}

__inline__ void calibrateCpuClockSpeed(unsigned long *cyclesPerSec){
  unsigned  int sleepTimeMsec = 250; // in milliseconds
  unsigned long long avgCycles = 0;
  unsigned long long mytstampA, mytstampB;

  unsigned  int iterations;
  for (iterations = 0; iterations < 10; ++iterations) {
    myRdtsc(&mytstampA);
    nanosleep((struct timespec[]){{sleepTimeMsec/1000, (sleepTimeMsec%1000)*1000000L}}, NULL);
    myRdtsc(&mytstampB);
    avgCycles += mytstampB - mytstampA;
  }

  avgCycles = avgCycles / iterations;

  *cyclesPerSec = ((1000 * avgCycles) / sleepTimeMsec);
}

__inline__ void getTimeFromCpuCycles(unsigned long long *pCycles, unsigned long *cpuHz, struct timeval *pRetTime){
  pRetTime->tv_sec = (__time_t) ((*pCycles)/(*cpuHz));
  pRetTime->tv_usec = (__suseconds_t) ((((*pCycles)%(*cpuHz))/((double)(*cpuHz)))*1000000L);
}

#endif