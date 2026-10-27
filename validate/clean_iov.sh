#!/usr/bin/env bash
# 죽은 프로세스가 남긴 hf3fs USRBIO iov(/dev/shm 공유 메모리)를 지운다 (10-21: MP 서버를 kill 하면 iov 76 개 · 14.8 GiB 가 남아
# 다음 vLLM 이 메모리가 모자라 뜨지 못했다). hf3fs_iovdestroy 처럼 3FS 의 iov 링크를 먼저 지워 FUSE 가 놓게 하고 /dev/shm 파일을 지운다.
# 이 계정 소유만, 5분보다 오래된 것만(막 만들어지는 중인 것 제외), 이 계정의 살아 있는 프로세스가 매핑한 것은 건드리지 않는다.
#   SM_MOUNT=<3FS 마운트> clean_iov.sh [-n]      -n: 지울 것만 보인다
set -u
M=${SM_MOUNT:?3FS 마운트를 SM_MOUNT 로 준다}
DRY=${1:-}
inuse=$(for p in $(pgrep -u "$(id -u)"); do grep -ho '/dev/shm/hf3fs-iov-[0-9a-f-]*' /proc/$p/maps 2>/dev/null; done | sort -u)
n=0 bytes=0
for f in $(find /dev/shm -maxdepth 1 -name 'hf3fs-iov-*' -user "$(id -u)" -mmin +5); do
  grep -qxF "$f" <<< "$inuse" && continue
  links=$(find $M/3fs-virt/iovs -maxdepth 1 -lname "$f" 2>/dev/null)
  n=$((n + 1)); bytes=$((bytes + $(stat -c %s "$f")))
  if [ "$DRY" = -n ]; then echo "$f ${links:-(링크 없음)}"; continue; fi
  [ -n "$links" ] && rm -f $links
  rm -f "$f"
done
echo "$(hostname) iov $n 개 $((bytes >> 20)) MiB $([ "$DRY" = -n ] && echo '지울 것' || echo '지움')"
