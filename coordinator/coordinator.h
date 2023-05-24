/**
 * The MapReduce coordinator.
 */

#ifndef H1_H__
#define H1_H__
#include "../rpc/rpc.h"
#include "../lib/lib.h"
#include "../app/app.h"
#include "job.h"
#include <glib.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <rpc/pmap_clnt.h>
#include <string.h>
#include <memory.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>

typedef struct {
  GList* task_queue;
  GList* active_queue;
  GHashTable* ht;
  int next_id;
} coordinator;

typedef struct {
  int id;
  int c_maps;    
  int c_reduces; 
  int complete;
  int failed;
  submit_job_request* request;
} job_t;

typedef struct {
  job_t* job_owner;
  path file;
  bool_t reduce; //true if reduce
  int task_num;
  time_t start_time;
} task_t;




void coordinator_init(coordinator** coord_ptr);
#endif