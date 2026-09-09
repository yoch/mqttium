#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <stdatomic.h>
/* Only counters, no application-byte copies and no allocation per call. */
enum {EPWAIT,EPCTL,RECV,RECVFROM,SEND,SENDTO,READ,WRITE,MMAP,MUNMAP,MADVISE,NOPS};
static const char *names[NOPS]={"epoll_wait","epoll_ctl","recv","recvfrom","send","sendto","read","write","mmap","munmap","madvise"};
typedef struct { _Atomic unsigned long calls,err,again,bytes,ns,hist[25],zero,positive,ready2; } stat_t;
static stat_t stats[NOPS];
static _Atomic unsigned long first_recv,last_recv;
static _Atomic int enabled;
static uint64_t now(void){struct timespec ts;clock_gettime(CLOCK_MONOTONIC,&ts);return (uint64_t)ts.tv_sec*1000000000ull+ts.tv_nsec;}
static void note(int op,long result,uint64_t start,int arg,int saved){
 if(!atomic_load_explicit(&enabled,memory_order_relaxed))return;
 stat_t *s=&stats[op];uint64_t dt=now()-start;unsigned b=0;uint64_t v=dt;
 while(v>1&&b<24){v>>=1;b++;}
 s->calls++;s->ns+=dt;s->hist[b]++;
 if(result<0){s->err++;if(saved==EAGAIN||saved==EWOULDBLOCK)s->again++;}
 else s->bytes+=result;
 if(op==EPWAIT){if(arg==0)s->zero++;else s->positive++;if(result>1)s->ready2++;}
}
void diag_reset(void){atomic_store(&enabled,0);memset(stats,0,sizeof(stats));first_recv=last_recv=0;atomic_store(&enabled,1);}
void diag_dump(const char *path){
 int was=atomic_exchange(&enabled,0);FILE *f=fopen(path,"w");if(f){
 fprintf(f,"{\"pid\":%d,\"recv_address_first\":%lu,\"recv_address_last\":%lu,\"syscalls\":{",getpid(),(unsigned long)first_recv,(unsigned long)last_recv);
 for(int i=0;i<NOPS;i++){stat_t *s=&stats[i];fprintf(f,"%s\"%s\":{\"calls\":%lu,\"errors\":%lu,\"eagain\":%lu,\"result_sum\":%lu,\"elapsed_ns\":%lu,\"timeout_zero\":%lu,\"timeout_nonzero\":%lu,\"multi_ready\":%lu,\"duration_log2_ns\":[",i?",":"",names[i],(unsigned long)s->calls,(unsigned long)s->err,(unsigned long)s->again,(unsigned long)s->bytes,(unsigned long)s->ns,(unsigned long)s->zero,(unsigned long)s->positive,(unsigned long)s->ready2);
 for(int j=0;j<25;j++)fprintf(f,"%s%lu",j?",":"",(unsigned long)s->hist[j]);fprintf(f,"]}");}fprintf(f,"}}\n");fclose(f);}atomic_store(&enabled,was);
}
#define LOAD(n) if(!real)real=dlsym(RTLD_NEXT,n)
#define FINISH(op,res,t,arg) int e=errno;note(op,res,t,arg,e);errno=e;return res
int epoll_wait(int fd,struct epoll_event *events,int n,int timeout){static int(*real)(int,struct epoll_event*,int,int);LOAD("epoll_wait");uint64_t t=now();int r=real(fd,events,n,timeout);FINISH(EPWAIT,r,t,timeout);}
int epoll_pwait(int fd,struct epoll_event *events,int n,int timeout,const sigset_t *mask){static int(*real)(int,struct epoll_event*,int,int,const sigset_t*);LOAD("epoll_pwait");uint64_t t=now();int r=real(fd,events,n,timeout,mask);FINISH(EPWAIT,r,t,timeout);}
int epoll_ctl(int fd,int op,int target,struct epoll_event *ev){static int(*real)(int,int,int,struct epoll_event*);LOAD("epoll_ctl");uint64_t t=now();int r=real(fd,op,target,ev);FINISH(EPCTL,r,t,op);}
ssize_t recv(int fd,void *buf,size_t n,int flags){static ssize_t(*real)(int,void*,size_t,int);LOAD("recv");uint64_t t=now();ssize_t r=real(fd,buf,n,flags);if(r>0&&enabled){if(!first_recv)first_recv=(uintptr_t)buf;last_recv=(uintptr_t)buf;}FINISH(RECV,r,t,fd);}
ssize_t recvfrom(int fd,void *buf,size_t n,int flags,struct sockaddr *a,socklen_t *l){static ssize_t(*real)(int,void*,size_t,int,struct sockaddr*,socklen_t*);LOAD("recvfrom");uint64_t t=now();ssize_t r=real(fd,buf,n,flags,a,l);FINISH(RECVFROM,r,t,fd);}
ssize_t send(int fd,const void *buf,size_t n,int flags){static ssize_t(*real)(int,const void*,size_t,int);LOAD("send");uint64_t t=now();ssize_t r=real(fd,buf,n,flags);FINISH(SEND,r,t,fd);}
ssize_t sendto(int fd,const void *buf,size_t n,int flags,const struct sockaddr *a,socklen_t l){static ssize_t(*real)(int,const void*,size_t,int,const struct sockaddr*,socklen_t);LOAD("sendto");uint64_t t=now();ssize_t r=real(fd,buf,n,flags,a,l);FINISH(SENDTO,r,t,fd);}
ssize_t read(int fd,void *buf,size_t n){static ssize_t(*real)(int,void*,size_t);LOAD("read");uint64_t t=now();ssize_t r=real(fd,buf,n);FINISH(READ,r,t,fd);}
ssize_t write(int fd,const void *buf,size_t n){static ssize_t(*real)(int,const void*,size_t);LOAD("write");uint64_t t=now();ssize_t r=real(fd,buf,n);FINISH(WRITE,r,t,fd);}
void *mmap(void *a,size_t n,int prot,int flags,int fd,off_t off){static void*(*real)(void*,size_t,int,int,int,off_t);LOAD("mmap");uint64_t t=now();void *r=real(a,n,prot,flags,fd,off);int e=errno;note(MMAP,r==MAP_FAILED?-1:(long)n,t,fd,e);errno=e;return r;}
int munmap(void *a,size_t n){static int(*real)(void*,size_t);LOAD("munmap");uint64_t t=now();int r=real(a,n);FINISH(MUNMAP,r,t,0);}
int madvise(void *a,size_t n,int advice){static int(*real)(void*,size_t,int);LOAD("madvise");uint64_t t=now();int r=real(a,n,advice);FINISH(MADVISE,r,t,advice);}
