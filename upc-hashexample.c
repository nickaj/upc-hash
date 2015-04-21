/* Copyright (c) 2015 The University of Edinburgh. */

/* Licensed under the Apache License, Version 2.0 (the "License"); */
/* you may not use this file except in compliance with the License. */
/* You may obtain a copy of the License at */

/*     http://www.apache.org/licenses/LICENSE-2.0 */

/* Unless required by applicable law or agreed to in writing, software */
/* distributed under the License is distributed on an "AS IS" BASIS, */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. */
/* See the License for the specific language governing permissions and */
/* limitations under the License. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <upc.h>
#include <upc_collective.h>

typedef unsigned long int numb;   
                         /* Should be 64 bit wide, to hold the square of: */
                         /* If you change this, also change "atol" in main */
#define modulus 1073741827
#define multipl 33554467

typedef numb * obj;

numb N;     /* Number of objects to hash */
size_t m;   /* Size of objects in bytes, rounded up to be a multiple of
               sizeof(numb) */
numb k;     /* Number of times each object is expected */
/* Some fast way to create funny data: */

numb val = 1234567;



numb next(void)
{
    val = (val * multipl) % modulus;
    return val;
}
void resetvalue(void)
{
  int i,j,nval;
  numb dummy;

  val = 1234567;
  nval=N/THREADS;
    for(j=0;j<MYTHREAD*nval;j++){
      for (i = m/sizeof(numb); i > 0;i--){
	dummy=next();
      }
    }
    
}

obj newobj(void)
{
    obj o,o2;
    int i;
    o = malloc(m);
    o2 = o;
    for (i = m/sizeof(numb); i > 0;i--) *o2++ = next();
    return o;
}

/* Code for hashing: */

/*shared  obj  *hashtab;*/
shared numb *hashtab;
shared numb *hashcount;
/*shared hashtemp *obj;*/
upc_lock_t *shared lock[THREADS]; /* same number of locks as threads */
numb hashlen;
numb nobj;
numb nextra;
numb collisions;

void inithash(numb N)
{
  int i;

  hashlen = 2*N+1;
  nobj=2*N/THREADS+1;
  hashtab = (shared  numb *)upc_all_alloc(THREADS,nobj*sizeof(numb));
  hashcount = (shared numb *)upc_all_alloc(THREADS,nobj*sizeof(numb));
  /*  hashtemp = (shared obj *)upc_all_alloc(THR\EADS,sizeof(obj));*/
  if (hashtab == NULL || hashcount == NULL) exit(1);
  collisions = 0;
  /*  lock[MYTHREAD]=upc_all_lock_alloc(); */
  /*  lock[MYTHREAD]=upc_global_lock_alloc();*/


  for ( i=0; i<THREADS; ++i ) {
    upc_lock_t* temp = upc_all_lock_alloc();
    if ( upc_threadof( &lock[i] ) == MYTHREAD ) {
      lock[i] = temp;
    }
  }

  upc_barrier;
}

void finalisehash()
{
  /* free the memory */
  upc_free(hashtab);
  upc_free(hashcount);
  /*  upc_lock_free(lock);*/

}

numb f(obj o)
/* Our hash function, this should do: */
{

    numb x = 0;
    int i;
    for (i = m/sizeof(numb); i > 0; i--){
      x += *o++;
    }
    return x % hashlen;
}

numb hashlookup(obj o)
/* Never fill up the hash table! Returns the number of times a value
 * was seen so far. If an obj is seen for the first time, it is stored
 * without copying it, do not free the object in this case, ownership
 * goes over to the hash table. */
{
  int iter;
  int vthread;
  numb v;
    v = f(o);
    /*    printf("check the hash[%d] *o %d v %d\n",MYTHREAD,*o,v);*/
    iter=0;
    while (1) {    
      iter++;
      /* which thread, and lock, owns the hashtab entry. */
      vthread=upc_threadof(&hashtab[v]);
      upc_lock(lock[vthread]);
      if (hashtab[v]) {  
	/*	printf("already entry [%d] %d %d %d %d %d\n",MYTHREAD,v,hashtab[v],o,hashtab[v],*o);*/
	
	/*	if (memcmp(o,&hashtab[v],m)) {*/
	if(!(*o==hashtab[v])){
	  /*	  printf("collision[%d] %d %d %d %d %d\n",MYTHREAD,v,hashtab[v],o,hashtab[v],*o);*/
	  /* find the thread which holds the object. */
	  /*	  printf("collision[%d] vthread=%d *hashtab[v]=%d\n",MYTHREAD,vthread,hashtab[v]);*/
	  v++;
	  if (v >= THREADS*nobj) v = 0;
	  collisions++;
	  upc_unlock(lock[vthread]);
	}
	else {   /* Found! */
	  hashcount[v]++;
	  upc_unlock(lock[vthread]);
	  /*	  printf("repetition %d %d %d %d\n",hashtab[v],o,hashtab[v],*o);*/

	  return hashcount[v];
	}
      }
      else {   /* Definitely not found */
	hashtab[v] = *o;
	hashcount[v] = 1;
	upc_unlock(lock[vthread]);
	/*	printf("new [%d] %d %d %d %d %d\n",MYTHREAD,v,hashtab[v],o,hashtab[v],*o);*/
	return hashcount[v];

      }

    }

}

int main(int argc, char *argv[])
{
    numb i,j,c;
    obj o;

    int repeatObj=0;

    static shared int allCollisions[THREADS];
    static shared int sumCollisions;
    static shared int allRO[THREADS];
    static shared int sumRO;

    if (argc != 4) {
        puts("Usage: hashexample N m k");
        puts("       where N is the number of objects to hash");
        puts("         and m is the length of the objects in bytes");
        puts("         and k is the number of times objects are expected");
        return 0;
    }
    N = atol(argv[1]);
    m = (size_t) atol(argv[2]);
    m = (m + sizeof(numb) - 1) / sizeof(numb);
    m *= sizeof(numb);
    k = atol(argv[3]);
    
    if(MYTHREAD==0){
      printf("%s:N=%d N/THREADS=%d\n",argv[0],N,N/THREADS);
    }

    inithash(N);

    for (i = 1; i <= k; i++) {
        resetvalue();
	upc_forall(j=0;j<N;j++;j){
	  o = newobj();
	  c = hashlookup(o);
	  /*	  if (c > 1) {*/
	  /* now always free */
	  free(o);

	  if (c != i) {
	    repeatObj++;
	  }
        }
    }
    
    /* before calling finalise everyone should wait here */
    /*    upc_barrier;*/
    /* Need to aggregate the number of collisions, on a collective? */
    /*    allCollisions[MYTHREAD]=collisions;*/
    /* aggregate using the collective */
    /*    upc_all_reduceI(&sumCollisions,&allCollisions[MYTHREAD],UPC_ADD, 1,THREADS,NULL,UPC_IN_ALLSYNC|UPC_OUT_ALLSYNC);
    upc_barrier;
    allRO[MYTHREAD]=repeatObj;
    upc_all_reduceI(&sumRO,&allRO[MYTHREAD],UPC_ADD, 1,THREADS,NULL,UPC_IN_ALLSYNC|UPC_OUT_ALLSYNC);
    upc_barrier;*/
    /* write the results */
    /*    upc_forall(j=0;j<THREADS*nobj;j++;j){
       if(hashtab[j]){
	 printf("ID[%d] j %d #j %d *j %d count %d collisions %d\n",MYTHREAD,j,hashtab[j],hashtab[j],hashcount[j],sumCollisions);
       }
    }

    if(MYTHREAD==0){
      printf("finn:sumCollisions %d sum repititions %d\n",sumCollisions,sumRO);
      }*/
    return 0;
}
