// client - a memcached load tester
// David Meisner 2010 (meisner@umich.edu)

//Protocol based on http://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol

#include "loader.h"

int verbose;

void printUsage() {

        printf( "usage: loader [-option]\n"
                "        [-a arg  input distribution file]\n"
                "        [-b arg  the size of batch for sent requests (only for the latency test)]\n"
                "        [-c arg  total number of connections]\n"
                "        [-d arg  value size distribution file]\n"
                "        [-D arg  size of main memory available to each memcached server in MB]\n"
                "        [-e use  an exponential arrival distribution (default: constant)]\n"
                "        [-f arg  fixed object size]\n"
                "        [-k arg  num keys (default: 1000)]\n"
                "        [-g arg  fraction of requests that are gets (The rest are sets)]\n"
                "        [-h prints this message]\n"
                "        [-j preload data]\n"
                "        [-l arg use a fixed number of gets per multiget]\n"
                "        [-m arg fraction of requests that are multiget]\n"
                "        [-i arg fraction of requests that are increment]\n"
                "        [-n enable naggle's algorithm]\n"
                "        [-N arg provide a key population distribution file]\n"
                "        [-u use UDP protocl (default: TCP)]\n"
                "        [-o arg  ouput distribution file, if input needs to be scaled]\n"
                "        [-q arg  request always hit the same object (maximize the CPU cache hits)]\n"
                "        [-r ATTEMPTED requests per second (default: max out rps)]\n"
                "        [-s server configuration file]\n"
                "        [-S dataset scaling factor]\n"
                "        [-t arg  runtime of loadtesting in seconds (default: run forever)]\n"
                "        [-T arg  interval between stats printing (default: 1)]\n"
                "        [-w number of worker threads (specify 0 for latency test)]\n"
                "        [-x run timing tests instead of loadtesting]\n");
}


//Parses the command line arguments
struct config* parseArgs(int argc, char** argv) {
  
  struct config* config = malloc(sizeof(struct config));

  int ncpus = (int) sysconf(_SC_NPROCESSORS_ONLN);
  config->n_cpus = ncpus;
  config->protocol_mode = TCP_MODE;
  config->n_workers = 1;
  config->n_servers = 1;
  config->scaling_factor = 0;
  config->multiget_frac = 0.0;
  config->run_time  = -1;
  config->stats_time  = 1;
  config->n_connections_total = 1;
  config->naggles = 0;
  config->fixed_size = -1;
  config->get_frac = 0.9f;
  config->n_keys = 1000;
  config->multiget_size = -1;
  config->multiget_dist = NULL;
  config->incr_frac = 0.0;
  config->arrival_distribution_type = ARRIVAL_CONSTANT;
  config->rps = -1;
  config->zynga = 0;
  config->random_seed = 1;
  config->pre_load = 0;
  config->bad_multiget = 0;
  config->value_size_dist = NULL;
  config->key_pop_dist = NULL;
  config->dep_dist = NULL;
  config->interarrival_dist = NULL;
  config->input_file=NULL;
  config->output_file=NULL;
  config->server_memory=1024;
  config->server_file=NULL;
  int i;
  for(i=0; i<MAX_SERVERS; i++){
    config->server_port[i]=MEMCACHED_PORT;
    config->server_ip_address[i]=NULL;
  }
  config->do_latency = 0;
  config->tx_batch_size = 1;
  config->hit_one_object = 0;

  int c;
  while ((c = getopt (argc, argv, "a:b:c:d:D:ef:g:hi:jk:l:L:m:MnN:o:p:qur:s:S:t:T:w:W:xz")) != -1) {
    switch (c) {

      case 'a':
	config->input_file=calloc(strlen(optarg)+1, sizeof(char));
  	strcpy(config->input_file, optarg);	
        break;

      case 'c':
        config->n_connections_total = atoi(optarg);
        break;

      case 'd':
        printf("Using size distribution file %s\n", optarg);
        config->value_size_dist = loadDistributionFile(optarg);
        break;

      case 'D':
        config->server_memory = atoi(optarg);
        break;

      case 'e':
        config->arrival_distribution_type = ARRIVAL_EXPONENTIAL;
        break;

      //Use a fixed object size
      case 'f':
        config->fixed_size = atoi(optarg);
        break;

      case 'i':
        config->incr_frac = (float) atof(optarg);
        if(config->incr_frac < 0 || config->incr_frac > 1.0){
          printf("Increment fraction must be between 0 and 1.0\n");
          exit(-1);
        }
        break;

      case 'j':
        config->pre_load = 1;
        break;

      case 'l':
        config->multiget_size = atoi(optarg);
        break;

      case 'L':
        printf("Using multiget file %s\n", optarg);
        config->multiget_dist = loadDistributionFile(optarg);
        config->multiget_size = -1;
        config->multiget_frac = 1.0;
        break;

      case 'h':
        printUsage();
        exit(0);     
        break;

      case 'k':
        config->n_keys = atoi(optarg);
        break;

      case 'm':
        config->multiget_frac = (float) atof(optarg);
        break;

      case '1':
        config->bad_multiget = 1;
        break;

      case 'n':
        config->naggles = 1;     
        break;

      case 'N':
        printf("Using popularity file %s\n", optarg);
        config->key_pop_dist = loadDistributionFile(optarg);     
        break;

      case 'u':
        config->protocol_mode = UDP_MODE;     
        break;
 
      //Fraction of requests that are gets
      case 'g':
        config->get_frac = (float) atof(optarg);
        if(config->get_frac < 0 || config->get_frac > 1.0){
          printf("Get fraction must be between 0 and 1.0\n");   
          exit(-1);
        }
        break;

      case 'o':
        config->output_file=calloc(strlen(optarg)+1, sizeof(char));
        strcpy(config->output_file, optarg);
        break;

      case 'r':
        config->rps = atoi(optarg);
        if(config->rps == 0)
          config->do_latency = 1;
          if (config->do_latency && config->n_workers > 1){
            printf("Server latency tests require only one worker thread (user has specified %d).\n", config->n_workers);
            exit(-1);
          }
        break;

      case 'b':
        if(!config->do_latency){
          printf("You can only use the batch flag if you have also specified that it is a latency test (flag: \"-r 0\").");
          exit(1);
        }
        config->tx_batch_size = atoi(optarg);
        if(config->tx_batch_size < 1 || config->tx_batch_size > 1000){
          printf("Please choose a request TX batch size between 1 and 1000 (you selected %d).", config->tx_batch_size);
          exit(1);
        }
        break;

      case 'q':
        config->hit_one_object = 1;
        printf("\nHitting only one object.\n");
        break;

      case 's':
        config->server_file=calloc(strlen(optarg)+1, sizeof(char));
        strcpy(config->server_file, optarg);
        break;
      
      case 'S':
        config->scaling_factor=atoi(optarg);
        break;
      
      case 't':
        config->run_time = atoi(optarg);     
        break;

      case 'T':
        config->stats_time = atoi(optarg); 
        printf("stats_time = %d\n", config->stats_time);    
        break;

      case 'w':
        config->n_workers = atoi(optarg);     
        break;
     
      case 'x':
        timingTests();
        exit(0);
        //break;

      case 'z':
        config->zynga = 1;     
        break;

      default:
        printUsage();
        exit(1);
    }
  }

  return config;

}//End parseArgs()

void loadServerFile(struct config* config){
  FILE* file = fopen(config->server_file, "r");
  if(file==NULL){
    printf("File %s does not exist!\n", config->server_file);
    exit(1);	
  }
  char lineBuffer[1024]; 
  int i=0;
  while (fgets(lineBuffer, sizeof(lineBuffer), file)) {
    char* ipaddress = nslookup(strtok(lineBuffer, " ,\n"));
    config->server_port[i]=atoi(strtok(NULL, " ,\n"));
    config->server_ip_address[i]=calloc(strlen(ipaddress)+1, sizeof(char));
    strcpy(config->server_ip_address[i], ipaddress);   
    i++;
  }
  fclose(file);
  config->n_servers=i;
}

//Prints the configuration
void printConfiguration(struct config* config) {

  printf("Configuration:\n");
  printf("\n");
  printf("nProcessors on system: %d\n", config->n_cpus);
  printf("nWorkers: %d\n", config->n_workers);
  printf("runtime: %d\n", config->run_time);

//  printf("Connecterions per worker: %d\n",  config->n_connections_per_worker);
  if(config->fixed_size > 0){
    printf("Fixed value size: %d\n", config->fixed_size);
  }
  printf("Get fraction: %f\n", config->get_frac);
  if(config->naggles){
    printf("Naggle's algorithm: True\n");
  } else {
    printf("Naggle's algorithm: False\n");
  }
  printf("\n\n");

}//End printConfiguration()

void setupLoad(struct config* config) {

  if(config->input_file==NULL){
   printf("Option '-a' is mandatory and requires an argument\n");
   exit(-1);	
  }

  if(config->server_file==NULL){
    printf("Option '-s' is mandatory and requires a server configuration file as an argument\n");
    exit(-1);
  }

  loadServerFile(config);

  if(config->n_workers % config->n_servers != 0){
   printf("Number of client (worker) threads must be divisible by the number of servers\n");
   exit(-1);
  }

  if((config->output_file == NULL) && (config->scaling_factor>=1)){
    printf("Scaling the dataset requires an output file\n");
    exit(-1);
  }

  //printf("----------%d---------", config->pre_load);

  //if(config->pre_load || (config->scaling_factor==1)) {
  if(config->scaling_factor==0) {
    //printf("INSIDE");
    config->dep_dist = loadDepFile(config);
  }
  else {
    //printf("OUTSIDE");
    //config->dep_dist = loadAndScaleDepFile(config);
    config->dep_dist = loadAndScaleDepFile_dimos(config);
  }

  if(config->value_size_dist == NULL){
    config->value_size_dist = createUniformDistribution(1, 1024);
  }

  if(config->key_pop_dist == NULL){
    config->key_pop_dist = createUniformDistribution(0, config->n_keys -1);
    //printf("created uniform distribution %d\n", config->n_keys);
  } else {
    config->n_keys = CDF_VALUES;
  }

  config->key_list = generateKeys(config);
  
  if(config->multiget_dist == NULL) {
    config->multiget_dist = createUniformDistribution(2, 10);
  }

  printf("rps %d cpus %d\n", config->rps, config->n_workers);
  if (config->rps == -1 || config->rps == 0)
      return;

  int meanInterarrival = 1.0/(((float)config->rps)/(float)config->n_workers)*1e6;
  printf("meanInterarrival %d\n", meanInterarrival);
  if(config->arrival_distribution_type == ARRIVAL_CONSTANT) {
    config->interarrival_dist = createConstantDistribution(meanInterarrival);
  } else {
    config->interarrival_dist = createExponentialDistribution(meanInterarrival);
  }

}//End setupLoad()


//Cleanup mallocs and make valgrind happy
void cleanUp(struct config* config) {
 
  int i;
  for( i = 0; i < config->key_list->n_keys; i++ ){ 
    free(config->key_list->keys[i]);
  }//End for i
  free(config->key_list);
  free(config->value_size_dist);
  free(config->key_pop_dist);
  free(config);
}


int main(int argc, char** argv){

  /*struct request* request = malloc(sizeof(struct request));
  printf("\n%p\n", (void *) request);


  printf("\n%p\n", &request->send_time_tsc);

  struct request* request2 = request;
  printf("\n%p\n", &request2->send_time_tsc);


  myRdtsc(&request2->send_time_tsc);
  printf("\n%llu\n", request2->send_time_tsc);

  myRdtsc(&request2->send_time_tsc);
  printf("\n%llu\n", request2->send_time_tsc);


  //printf("\n%p\n", (void *) request2);
  //printf("\n%p\n", &request2);


  exit(1);
   */

  
  struct config* config = parseArgs(argc, argv);
  printConfiguration(config);

  setupLoad(config);

  //exit(0);

  createWorkers(config);
  statsLoop(config);
  return 0;

}//End main()
