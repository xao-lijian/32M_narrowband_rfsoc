// gcc -O2 -D_GNU_SOURCE -o udp2dada_vdif_blk  udp2dada_vdif_blk.c \
/*   */ -I/home/uwb/linux_64/include -L/home/uwb/linux_64/lib \
/*   */ -Wl,-rpath=/home/uwb/linux_64/lib -lpsrdada -lm
//lijian@xao.ac.cn  2025.10.2
// 运行示例：
//   dada_db -k a000 -n 32 -b 268435456
//   LD_PRELOAD=/usr/lib64/libvma.so \
//   ./udp2dada_vdif_blk -k a000 --header header_all.cfg \
//       --ip 10.17.16.11 --port 17200 \
//       --tsamp 3.125e-8 --freq 1420 --bw 32 \
//       --vdif-endian le --payload-endian le \
//       --dfmax 31249 --batch 64 --stat 1.0


#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <sched.h>

#include <dada_hdu.h>
#include <ipcio.h>
#include <ipcbuf.h>
#include <ascii_header.h>
#include <multilog.h>

#define FRAME_SIZE        8256
#define HEADER_SKIP       64                  // 32B VDIF + 32B padding
#define PAYLOAD_SIZE      (FRAME_SIZE - HEADER_SKIP)     // 8192
#define SHORTS_PER_FRAME  (PAYLOAD_SIZE/2)               // 4096
#define GROUPS_PER_FRAME  (SHORTS_PER_FRAME/4)           // 1024
#define BLOCK_LEN         1024                // a3/a2/a1/a0 各 1024

static volatile int stop_flag = 0;
static void on_sigint(int s){ (void)s; stop_flag = 1; }

static inline uint32_t rd32_le(const uint8_t* p){
  return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
static inline uint32_t rd32_be(const uint8_t* p){
  return ((uint32_t)p[0]<<24) | ((uint32_t)p[1]<<16) | ((uint32_t)p[2]<<8) | (uint32_t)p[3];
}
static inline uint16_t bswap16(uint16_t x){ return (uint16_t)((x>>8) | (x<<8)); }

/* 解析 VDIF 头：
   seconds   = w0[29:0]
   ref_epoch = w1[29:24]
   data_frame= w1[23:0]
   le: 1=小端, 0=大端。 参见你提供脚本。  :contentReference[oaicite:1]{index=1} */
static inline void parse_vdif_hdr(const uint8_t* vdif32, int le,
                                  int* ref_epoch, uint32_t* seconds, uint32_t* df)
{
  uint32_t w0 = le ? rd32_le(vdif32+0) : rd32_be(vdif32+0);
  uint32_t w1 = le ? rd32_le(vdif32+4) : rd32_be(vdif32+4);
  *seconds    = (w0 & 0x3FFFFFFFu);
  *ref_epoch  = (int)((w1 >> 24) & 0x3Fu);
  *df         = (w1 & 0xFFFFFFu);
}

/* VDIF epoch：epoch0=2000-01-01，步进 6 个月 */
static time_t vdif_epoch_to_time_t(int ref_epoch, uint32_t seconds){
  int year  = 2000 + (ref_epoch/2);
  int month = 1 + (ref_epoch & 1) * 6;
  struct tm tm0; memset(&tm0,0,sizeof(tm0));
  tm0.tm_year = year - 1900; tm0.tm_mon = month - 1; tm0.tm_mday = 1;
#ifdef _GNU_SOURCE
  time_t t0 = timegm(&tm0);
#else
  char* oldtz = getenv("TZ"); setenv("TZ","UTC",1); tzset();
  time_t t0 = mktime(&tm0);
  if (oldtz) setenv("TZ", oldtz, 1); else unsetenv("TZ"); tzset();
#endif
  return t0 + (time_t)seconds;
}

static void utc_str(time_t t,char*s,size_t n)
{ struct tm g; gmtime_r(&t,&g); strftime(s,n,"%Y-%m-%d-%H:%M:%S",&g); }

static inline double now_sec(void)
{ struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec + ts.tv_nsec*1e-9; }

static int load_header_template(const char* path, char* buf4096)
{
  memset(buf4096,0,4096);
  FILE* f=fopen(path,"rb"); if(!f) return -1;
  size_t n=fread(buf4096,1,4096,f); 
  fclose(f);
  if(n==0) return -1;
  ascii_header_set(buf4096,"HDR_SIZE","%d",4096);
  char tmp[8]; if (ascii_header_get(buf4096,"END","%s",tmp)!=1) ascii_header_set(buf4096,"END","%s","");
  return 0;
}
static key_t parse_key(const char* s)
{ char*e=0; unsigned long v=strtoul(s,&e,16); if(e&&*e) v=strtoul(s,0,10); return (key_t)v; }

int main(int argc,char**argv)
{
  // 缺省参数
  const char* key_str="a000";
  const char* ip="10.17.16.11"; int port=17200;
  const char* header_path="header_all.cfg";
  double tsamp_sec=3.125e-8;      // 输入单位：秒
  double freq_mhz=1420.0, bw_mhz=32.0;
  int vdif_le=1;                  // VDIF 头默认小端
  int payload_be=0;               // 载荷16位默认小端（若 viewer 需 -e big 就 --payload-endian be）
  uint32_t DF_MAX=31249;          // 秒内 data_frame 最大值（0..31249）
  int BATCH=64;                   // recvmmsg 批量
  double stat_interval = 1.0;     // 统计输出间隔（秒）

  for(int i=1;i<argc;i++)
  {
    if(!strcmp(argv[i],"-k")&&i+1<argc) key_str=argv[++i];
    else if(!strcmp(argv[i],"--ip")&&i+1<argc) ip=argv[++i];
    else if(!strcmp(argv[i],"--port")&&i+1<argc) port=atoi(argv[++i]);
    else if(!strcmp(argv[i],"--header")&&i+1<argc) header_path=argv[++i];
    else if(!strcmp(argv[i],"--tsamp")&&i+1<argc) tsamp_sec=atof(argv[++i]);  // 仍以“秒”输入
    else if(!strcmp(argv[i],"--freq")&&i+1<argc)  freq_mhz=atof(argv[++i]);
    else if(!strcmp(argv[i],"--bw")&&i+1<argc)    bw_mhz=atof(argv[++i]);
    else if(!strcmp(argv[i],"--vdif-endian")&&i+1<argc){ const char*e=argv[++i];
      if(!strcasecmp(e,"le")||!strcasecmp(e,"little")) vdif_le=1;
      else if(!strcasecmp(e,"be")||!strcasecmp(e,"big")) vdif_le=0;
    } else if(!strcmp(argv[i],"--payload-endian")&&i+1<argc){ const char*e=argv[++i];
      if(!strcasecmp(e,"be")||!strcasecmp(e,"big")) payload_be=1;
      else if(!strcasecmp(e,"le")||!strcasecmp(e,"little")) payload_be=0;
    } else if(!strcmp(argv[i],"--dfmax")&&i+1<argc){ long v=atol(argv[++i]); if(v>0) DF_MAX=(uint32_t)v;
    } else if(!strcmp(argv[i],"--batch")&&i+1<argc){ int v=atoi(argv[++i]); if(v>0) BATCH=v;
    } else if(!strcmp(argv[i],"--stat")&&i+1<argc){ stat_interval = atof(argv[++i]); if(stat_interval<=0) stat_interval=1.0;
    } else if(!strcmp(argv[i],"--help")){
      fprintf(stderr,"Usage: %s -k a000 --header header_all.cfg [--ip IP --port 17200]\n"
                     "       [--tsamp 3.125e-8 --freq MHz --bw MHz]\n"
                     "       [--vdif-endian le|be --payload-endian le|be]\n"
                     "       [--dfmax 31249 --batch 64 --stat 1.0]\n", argv[0]);
      return 0;
    }
  }

  // 降抖动（失败忽略）
  mlockall(MCL_CURRENT|MCL_FUTURE);
  struct sched_param sp={.sched_priority=60}; sched_setscheduler(0,SCHED_FIFO,&sp);

  // 连接 DADA ring
  multilog_t* log=multilog_open("udp2dada_vdif_blk",0); multilog_add(log,stderr);
  dada_hdu_t* hdu=dada_hdu_create(log); dada_hdu_set_key(hdu,parse_key(key_str));
  if(dada_hdu_connect(hdu)<0 || dada_hdu_lock_write(hdu)<0)
  { multilog(log,LOG_ERR,"dada attach/lock failed\n"); return 1; }

  // UDP socket
  int s=socket(AF_INET,SOCK_DGRAM,0); if(s<0){ perror("socket"); return 1; }
  int on=1; setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&on,sizeof(on));
  int rcvbuf=64*1024*1024; setsockopt(s,SOL_SOCKET,SO_RCVBUF,&rcvbuf,sizeof(rcvbuf));
  
#ifdef SO_BUSY_POLL
  int busy=50; setsockopt(s,SOL_SOCKET,SO_BUSY_POLL,&busy,sizeof(busy));
#ifdef SO_PREFER_BUSY_POLL
  int prefer=1; setsockopt(s,SOL_SOCKET,SO_PREFER_BUSY_POLL,&prefer,sizeof(prefer));
#endif
#endif

  struct sockaddr_in a; memset(&a,0,sizeof(a)); a.sin_family=AF_INET; a.sin_port=htons((uint16_t)port);
  if(inet_pton(AF_INET,ip,&a.sin_addr)!=1){ perror("inet_pton"); return 1; }
  if(bind(s,(struct sockaddr*)&a,sizeof(a))<0){ perror("bind"); return 1; }

  signal(SIGINT,on_sigint);
  fprintf(stderr,"Listen %s:%d  (VDIF %s, payload %s, DF_MAX=%u, batch=%d, stat=%.3fs)\n",
          ip,port, vdif_le?"LE":"BE", payload_be?"BE":"LE", DF_MAX, BATCH, stat_interval);

  // 先收一帧确定 UTC_START
  uint8_t first[FRAME_SIZE];
  ssize_t n=recvfrom(s,first,sizeof(first),0,NULL,NULL);
  if(n!=FRAME_SIZE){ fprintf(stderr,"first recv %zd\n",n); return 1; }

  int ref_epoch=0; uint32_t sec=0, df=0;
  parse_vdif_hdr(first,vdif_le,&ref_epoch,&sec,&df);
  time_t abs_sec=vdif_epoch_to_time_t(ref_epoch,sec);
  char utc[64]; utc_str(abs_sec,utc,sizeof(utc));
  
  fprintf(stderr,"First VDIF: ref=%d sec=%u df=%u -> UTC_START=%s\n",ref_epoch,sec,df,utc);

  // 写 DADA 头（TSAMP 用微秒）
  double tsamp_us = tsamp_sec * 1e6;   // 秒 -> 微秒
  char hdr[4096];
  if(load_header_template(header_path,hdr)!=0)
  { multilog(log,LOG_ERR,"load header %s fail\n",header_path); return 1; }

  ascii_header_set(hdr,"HDR_SIZE","%d",4096);
  ascii_header_set(hdr,"TSAMP","%.8f",tsamp_us);  // 单位：us
  ascii_header_set(hdr,"NCHAN","%d",1);
  ascii_header_set(hdr,"NPOL","%d",2);
  ascii_header_set(hdr,"NDIM","%d",2);
  ascii_header_set(hdr,"NBIT","%d",16);
  ascii_header_set(hdr,"BW","%.6f",bw_mhz);
  ascii_header_set(hdr,"FREQ","%.6f",freq_mhz);
  ascii_header_set(hdr,"UTC_START","%s",utc);
  ascii_header_set(hdr,"INSTRUMENT","%s","Medusa");
  ascii_header_set(hdr,"BASEBAND_OUTNBIT","%d",16);

  uint64_t hsz=ipcbuf_get_bufsz(hdu->header_block);
  char* hwr=ipcbuf_get_next_write(hdu->header_block);
  memcpy(hwr,hdr,4096);
  ipcbuf_mark_filled(hdu->header_block,4096);

  // 丢帧统计（累计）
  uint64_t total_frames=0,total_lost=0;
  int have_prev=0; uint32_t prev_df=0; time_t prev_abs=0;

  // 速率统计（区间）
  double t_start = now_sec();
  double t_last  = t_start;
  uint64_t frames_since = 0;
  uint64_t bytes_since  = 0;
  uint64_t lost_since   = 0;

  // 批量接收结构
  int B = BATCH;
  struct mmsghdr* msgs = calloc(B,sizeof(*msgs));
  struct iovec*   iovs = calloc(B,sizeof(*iovs));
  struct sockaddr_in* src = calloc(B,sizeof(*src));
  uint8_t *bufs = NULL; posix_memalign((void**)&bufs,64,(size_t)B*FRAME_SIZE);
  int16_t *outs = NULL;  posix_memalign((void**)&outs,64,(size_t)B*SHORTS_PER_FRAME*sizeof(int16_t));
  
  for(int i=0;i<B;i++)
  {
    iovs[i].iov_base = bufs + i*FRAME_SIZE; iovs[i].iov_len = FRAME_SIZE;
    memset(&msgs[i],0,sizeof(msgs[i]));
    msgs[i].msg_hdr.msg_iov = &iovs[i]; msgs[i].msg_hdr.msg_iovlen = 1;
    msgs[i].msg_hdr.msg_name = &src[i]; msgs[i].msg_hdr.msg_namelen = sizeof(src[i]);
  }

  // 先把第一帧写入（分块），并计入统计
  {
    uint16_t* u=(uint16_t*)(first+HEADER_SKIP);
	
    if(payload_be)
	{ for(int k=0;k<SHORTS_PER_FRAME;k++) u[k]=bswap16(u[k]); }

    int16_t* p=(int16_t*)u; int16_t* out=outs;
    for(int g=0; g<GROUPS_PER_FRAME; g++)
	{
      int16_t a0=p[4*g+0], a1=p[4*g+1], a2=p[4*g+2], a3=p[4*g+3];
      out[0*BLOCK_LEN + g] = a3; // X.re
      out[1*BLOCK_LEN + g] = a2; // X.im
      out[2*BLOCK_LEN + g] = a1; // Y.re
      out[3*BLOCK_LEN + g] = a0; // Y.im
    }
    ipcio_write(hdu->data_block,(char*)out,PAYLOAD_SIZE);

    have_prev=1; prev_df=df; prev_abs=abs_sec;
    total_frames++; frames_since++; bytes_since += FRAME_SIZE;
    // 注意：丢帧在下一包到来时才能判定，这里不增加 lost_since
  }

  fprintf(stderr,"Streaming (recvmmsg, block-layout)...\n");

  while(!stop_flag)
  {
    int recvd = recvmmsg(s,msgs,B,0,NULL);
	
    if(recvd<0)
	{ if(errno==EINTR) continue; perror("recvmmsg"); break; }

    for(int i=0;i<recvd;i++)
	{
      uint8_t* fr = (uint8_t*)iovs[i].iov_base;

      // 解析头并做丢帧检测
      parse_vdif_hdr(fr, vdif_le, &ref_epoch, &sec, &df);
      time_t abs = vdif_epoch_to_time_t(ref_epoch, sec);

      if(have_prev)
	  {
        int64_t dsec = (int64_t)abs - (int64_t)prev_abs;
        uint64_t lost = 0;
        if(dsec == 0)
		{
          int32_t gap = (int32_t)df - (int32_t)prev_df - 1;
          if(gap > 0) lost = (uint64_t)gap;
          else if (df <= prev_df)
		  {
            fprintf(stderr,"[WARN] same-sec DF non-monotonic: prev=%u cur=%u\n", prev_df, df);
          }
        }
		else if(dsec > 0)
		{
          if(prev_df <= DF_MAX) lost += (uint64_t)(DF_MAX - prev_df);
          if(dsec > 1)         lost += (uint64_t)(dsec - 1) * (uint64_t)(DF_MAX + 1);
          if(df > 0)           lost += (uint64_t)df;
        }
		else
		{
          char pbuf[64], cbuf[64]; utc_str(prev_abs,pbuf,64); utc_str(abs,cbuf,64);
          fprintf(stderr,"[WARN] VDIF time backwards: %s -> %s\n", pbuf, cbuf);
        }
		
        if(lost > 0)
		{
          total_lost += lost;
          lost_since += lost;
          char pbuf[64], cbuf[64]; utc_str(prev_abs,pbuf,64); utc_str(abs,cbuf,64);
          fprintf(stderr,"[DROP] lost=%llu  %s(DF=%u) -> %s(DF=%u)  total_lost=%llu\n",
                  (unsigned long long)lost, pbuf, prev_df, cbuf, df, (unsigned long long)total_lost);
        }
      }

      // 载荷端序 + 分块重排
      uint16_t* u = (uint16_t*)(fr + HEADER_SKIP);
	  
      if(payload_be)
	  { for(int k=0;k<SHORTS_PER_FRAME;k++) u[k]=bswap16(u[k]); }
  
      int16_t* p = (int16_t*)u;
	  
      int16_t* out = outs + i*SHORTS_PER_FRAME;

      for(int g=0; g<GROUPS_PER_FRAME; g++)
	  {
        int16_t a0=p[4*g+0], a1=p[4*g+1], a2=p[4*g+2], a3=p[4*g+3];

		 //int16_t sign = (g & 1) ? -1 : 1;
	     //out[0*BLOCK_LEN + g] = sign * a3; // X.re
         //out[1*BLOCK_LEN + g] = sign * a2; // X.im
         //out[2*BLOCK_LEN + g] = sign * a1; // Y.re
         //out[3*BLOCK_LEN + g] = sign * a0; // Y.im
        out[0*BLOCK_LEN + g] = a3; // X.re
        out[1*BLOCK_LEN + g] = a2; // X.im
        out[2*BLOCK_LEN + g] = a1; // Y.re
        out[3*BLOCK_LEN + g] = a0; // Y.im
      }

      ipcio_write(hdu->data_block,(char*)out,PAYLOAD_SIZE);

      // 计数
      prev_abs=abs; prev_df=df; have_prev=1;
      total_frames++; frames_since++; bytes_since += FRAME_SIZE;
    }

    // ——— 速率统计输出 ———
    double t_now = now_sec();
    double dt = t_now - t_last;
    if (dt >= stat_interval)
	{
      double fps   = frames_since / dt;
      double gbps  = (bytes_since * 8.0) / dt / 1e9;  // UDP 载荷 Gb/s
      double loss_rate = (total_frames>0) ? ((double)total_lost/(double)total_frames) : 0.0;

      fprintf(stderr,
        "[XAO_STAT] dt=%.3fs  recv=%.0f fps  rate=%.3f Gb/s  "
        "frames(total)=%llu  lost(total)=%llu  lost(dt)=%llu  loss=%.3e\n",
        dt, fps, gbps,
        (unsigned long long)total_frames,
        (unsigned long long)total_lost,
        (unsigned long long)lost_since,
        loss_rate
      );

      // 清零区间计数
      frames_since = 0;
      bytes_since  = 0;
      lost_since   = 0;
      t_last = t_now;
    }
  }

  ipcio_close(hdu->data_block);
  dada_hdu_unlock_write(hdu);
  dada_hdu_disconnect(hdu);
  dada_hdu_destroy(hdu);
  close(s);

  fprintf(stderr,"udp2dada_vdif_blk stopped. frames=%llu lost=%llu  loss=%.3e\n",
          (unsigned long long)total_frames,(unsigned long long)total_lost,
          total_frames?((double)total_lost/(double)total_frames):0.0);
  return 0;
}

