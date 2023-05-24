/**
 * The MapReduce coordinator.
 */

#include "coordinator.h"

#ifndef SIG_PF
#define SIG_PF void (*)(int)
#endif

/* Global coordinator state. */
coordinator* state;

extern void coordinator_1(struct svc_req*, struct SVCXPRT*);

/* Set up and run RPC server. */
int main(int argc, char** argv) {
  register SVCXPRT* transp;

  pmap_unset(COORDINATOR, COORDINATOR_V1);

  transp = svcudp_create(RPC_ANYSOCK);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create udp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_UDP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, udp).");
    exit(1);
  }

  transp = svctcp_create(RPC_ANYSOCK, 0, 0);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create tcp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_TCP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, tcp).");
    exit(1);
  }

  coordinator_init(&state);

  svc_run();
  fprintf(stderr, "%s", "svc_run returned");
  exit(1);
  /* NOTREACHED */
}

/* EXAMPLE RPC implementation. */
int* example_1_svc(int* argp, struct svc_req* rqstp) {
  static int result;

  result = *argp + 1;

  return &result;
}

/* SUBMIT_JOB RPC implementation. */
int* submit_job_1_svc(submit_job_request* argp, struct svc_req* rqstp) {
  static int result;

  printf("Received submit job request\n");

  //check if valid app
  app application = get_app(argp->app);
  if (application.name == NULL) {
    result = -1;
    return &result;
  } 

  //build up default job info 
  job_t* new_job = (job_t*) malloc(sizeof(job_t));
  new_job->id = state->next_id;
  new_job->c_maps = 0;
  new_job->c_reduces = 0;
  new_job->complete = false;
  new_job->failed = false;
  
  
  //build up the request
  submit_job_request* request = malloc(sizeof(submit_job_request));

  //copy files
  request->files.files_val = (path*) malloc(sizeof(path) * argp->files.files_len);
  memcpy(request->files.files_val, argp->files.files_val, argp->files.files_len); 
  for(int i=0; i < argp->files.files_len; i++) {
    request->files.files_val[i] = strdup(argp->files.files_val[i]);
  }
  request->files.files_len = argp->files.files_len;

  //copy over args
  request->args.args_val = (char*) malloc(sizeof(char) * argp->args.args_len);
  memcpy(request->args.args_val, argp->args.args_val, argp->args.args_len);
  request->args.args_len = argp->args.args_len;

  // Copy over rest 
  request->output_dir = strdup(argp->output_dir);
  request->app = strdup(argp->app);
  request->n_reduce = argp->n_reduce;

  new_job->request = request;

  //add to ht for fast look up
  g_hash_table_insert(state->ht, GINT_TO_POINTER(new_job->id), new_job);
  
  printf("length of task queue is %d\n", g_list_length(state->task_queue));

  // create task and add to the queue
  for(int i =0; i < new_job->request->files.files_len; i++ ){
    task_t* task = (task_t*) malloc(sizeof(task_t));
    task->task_num = i;
    task->reduce = false;
    task->file = strdup(new_job->request->files.files_val[i]);
    task->job_owner= new_job;
    

    
    state->task_queue = g_list_append(state->task_queue, task);
  }
  printf("length of task queue is %d\n", g_list_length(state->task_queue));
  result = new_job->id;
  state->next_id += 1; //incr global job id

  
  /* Do not modify the following code. */
  struct stat st;
  if (stat(argp->output_dir, &st) == -1) {
    mkdirp(argp->output_dir);
  }

  return &result;
  /* END */
}

/* POLL_JOB RPC implementation. */
poll_job_reply* poll_job_1_svc(int* argp, struct svc_req* rqstp) {
  static poll_job_reply result;

  printf("Received poll job request\n");
  job_t* p_job = g_hash_table_lookup(state->ht, GINT_TO_POINTER(*argp));
  result.invalid_job_id = false; 
  if(p_job == NULL){
    result.invalid_job_id = true;
    return &result;
  };

  if (p_job->id < 0) {
    result.invalid_job_id = true;
  }

  result.done = p_job->complete;
  result.failed = p_job->failed;
  return &result;
}

/* GET_TASK RPC implementation. */
get_task_reply* get_task_1_svc(void* argp, struct svc_req* rqstp) {
  static get_task_reply result;

  printf("Received get task request\n");
  result.file = "";
  result.output_dir = "";
  result.app = "";
  result.wait = true;
  result.args.args_len = 0;
  
  /* TODO */
  // check if task has timed out.
  GList* elem1 = NULL;
  if (state->active_queue != NULL) {
    elem1 = g_list_first(state->active_queue);
  }
  if (elem1 == NULL) {
    printf("logging list empty\n");
  } else if (((task_t*)elem1->data)->job_owner->failed == 1) {
    printf("from failed task\n");
    state->active_queue = g_list_delete_link(state->active_queue, elem1);
  } else {
    task_t* info = elem1->data;
    if(time(NULL) - info->start_time > TASK_TIMEOUT_SECS )  {
      if(!info->reduce){
        printf("im the mapp\n");
        result.file = strdup(info->file);
      } 
      
      result.output_dir = info->job_owner->request->output_dir;
      result.app = info->job_owner->request->app;
      result.n_map = info->job_owner->request->files.files_len;
      result.wait = false; //maybe set with writing both reduce and map at start
      result.args.args_len = info->job_owner->request->args.args_len;
      result.task = info->task_num;
      result.n_reduce = info->job_owner->request->n_reduce;
      result.args.args_val = info->job_owner->request->args.args_val;
      result.reduce = info->reduce;
      result.job_id = info->job_owner->id;
      info->start_time = time(NULL);

      printf("THIS IS THE TIME %ld", info->start_time);
      state->active_queue = g_list_delete_link(state->active_queue, elem1);
      state->active_queue = g_list_append(state->active_queue, info);
      return &result; 
      }   
  }

  GList* elem = NULL;
  if (state->task_queue != NULL) {
    elem = g_list_first(state->task_queue);
  }
  if (elem == NULL) {
    printf("logging list empty\n");
  } else if (((task_t*)elem->data)->job_owner->failed == 1) {
    printf("from failed task\n");
    state->task_queue = g_list_delete_link(state->task_queue, elem);
    // free(elem->data);
  } else {
    task_t* info = elem->data;

    if(!info->reduce){
      printf("im the mapp\n");
      result.file = strdup(info->file);
    } 
    result.output_dir = info->job_owner->request->output_dir;
    result.app = info->job_owner->request->app;
    result.n_map = info->job_owner->request->files.files_len;
    result.wait = false; //maybe set with writing both reduce and map at start
    result.args.args_len = info->job_owner->request->args.args_len;
    result.task = info->task_num;
    result.n_reduce = info->job_owner->request->n_reduce;
    result.args.args_val = info->job_owner->request->args.args_val;
    result.reduce = info->reduce;
    result.job_id = info->job_owner->id;
    info->start_time = time(NULL);
    
    //remove from list and free it
    state->task_queue = g_list_remove(state->task_queue, elem->data);
    state->active_queue = g_list_append(state->active_queue, info);

  }
  printf("contents : task num %d, job num %d, file %s",result.task, result.job_id, result.file );
  return &result;
}

/* FINISH_TASK RPC implementation. */
void* finish_task_1_svc(finish_task_request* argp, struct svc_req* rqstp) {
  static char* result;

  printf("Received finish task request\n");
  
  /* TODO */
  GList* elem = (GList*) state->active_queue;
    while(elem != NULL){
      task_t* temp = elem->data;
      elem = elem->next;
      if(argp->job_id == temp->job_owner->id && argp->task == temp->task_num){
        state->active_queue = g_list_remove(state->active_queue, temp);
        //possible free
      }
    }

  job_t* target = g_hash_table_lookup(state->ht, GINT_TO_POINTER(argp->job_id));
  if(argp->success){
    if(!argp->reduce){
      target->c_maps++;
    } else {
      target->c_reduces++;
    }
  } else { //fail case
    printf("failed task\n");
    target->complete = 1;
    target->failed = 1;
    GList* elem = (GList*) state->task_queue;
    while(elem != NULL){
      task_t* temp = elem->data;
      elem = elem->next;
      if(argp->job_id == temp->job_owner->id){
        state->task_queue = g_list_remove(state->task_queue, temp);
        //possible free
      }
    }
    return (void*)&result;
  }


  if(target->c_maps == target->request->files.files_len && !argp->reduce) {
    for(int i =0; i < target->request->n_reduce; i++){
      task_t* task = (task_t*) malloc(sizeof(task_t));
      task->job_owner= target;
      task->reduce = true;
      task->task_num = i;
      state->task_queue = g_list_prepend(state->task_queue, task);
    }
  }

  if(target->c_reduces == target->request->n_reduce){
    target->complete=1;
    target->failed=0;
  }
  return (void*)&result;
}

/* Initialize coordinator state. */
void coordinator_init(coordinator** coord_ptr) {
  *coord_ptr = malloc(sizeof(coordinator));

  coordinator* coord = *coord_ptr;
   coord->next_id = 0;
  coord->ht = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
  coord->task_queue = NULL;
  coord->active_queue = NULL;
}