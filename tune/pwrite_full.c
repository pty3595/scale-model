// 시험용 LD_PRELOAD: pwrite 가 짧게 쓰면 나머지를 이어 쓴다.
// NIXL HF3FS plugin(dram_zc)은 L1 등록 때 pwrite 를 한 번만 부르고 짧은 쓰기를 실패로 본다.
// Linux 는 한 번에 0x7ffff000 바이트까지만 쓰므로 2 GiB 가 넘는 L1 은 등록되지 않는다. 고친 plugin 을 흉내 낸다.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>

static ssize_t (*real_pwrite64)(int, const void *, size_t, off_t);

static ssize_t full(int fd, const void *buf, size_t n, off_t off) {
  if (!real_pwrite64) real_pwrite64 = dlsym(RTLD_NEXT, "pwrite64");
  size_t done = 0;
  while (done < n) {
    ssize_t w = real_pwrite64(fd, (const char *)buf + done, n - done, off + (off_t)done);
    if (w < 0) {
      if (errno == EINTR) continue;
      return done ? (ssize_t)done : -1;
    }
    if (w == 0) break;
    done += (size_t)w;
  }
  return (ssize_t)done;
}

ssize_t pwrite64(int fd, const void *buf, size_t n, off_t off) { return full(fd, buf, n, off); }
ssize_t pwrite(int fd, const void *buf, size_t n, off_t off) { return full(fd, buf, n, off); }
