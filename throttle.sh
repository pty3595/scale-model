#!/usr/bin/env bash
# 성능 상한으로 디스크·망 비율을 바꾼다. 모두 런타임 설정이라 재부팅하면 사라지고, reset 으로 되돌린다.
#   throttle.sh stor FREAD [FWRITE]    storage 디스크 읽기·쓰기를 NVMe 한 개 천장의 FREAD·FWRITE 배로
#                                      (storage_main cgroup io.max, 노드의 NVMe 마다. FWRITE 를 빼면 FREAD 와 같게)
#   throttle.sh net GBIT [STOR_GBIT]   RoCE 포트 송신 상한(모든 TC 의 maxrate). STOR_GBIT 를 주면 storage 포트만 그 값
#   throttle.sh reset | show
# 망 상한은 보내는 쪽만 막는다. 여러 storage 가 한 클라이언트로 몰릴 때(incast)의 손실이 상한 아래에서는 사라지는데,
# 이 차이는 무시하고 쓰기로 했다(2026-10-01). 링크 속도 자체를 낮추는 것은 스위치가 받지 않는다(results/linkspeed-20261001).
HERE=$(cd "$(dirname "$0")" && pwd)
eval "$(python3 "$HERE/cluster.py" env)" || exit 1    # STOR · STOR_IF · CLI3 · CLI_IF · NVME · SSH · SUDO (clusters/*.toml)
# storage VM 에는 mlnx_qos 가 없어 클라이언트의 것을 /tmp/mlnxqos 에 복사해 쓴다 (iproute2 dcb maxrate 는 값이 안 걸린다)
QOS_STOR="env PYTHONPATH=/tmp/mlnxqos PYTHONDONTWRITEBYTECODE=1 python3 /tmp/mlnxqos/mlnx_qos"
read -r READ_ONE WRITE_ONE < <(python3 -c "
import json; c = json.load(open('$HERE/calibration.json'))['ceilings_gibps']
print(c['nvme_one']['value'], c['nvme_write_one']['value'])")

r() { $SSH $SUSER@$1 "$SUDO $2" 2>&1 | grep -v "^$"; }

ensure_qos() {
  for ip in "${STOR[@]}"; do
    $SSH $SUSER@$ip "test -x /tmp/mlnxqos/mlnx_qos" && continue
    tar -C / -cf - usr/bin/mlnx_qos usr/lib/python3/dist-packages/{dcbnetlink,genetlink,netlink}.py |
      $SSH $SUSER@$ip \
        "mkdir -p /tmp/mlnxqos && tar -xf - -C /tmp/mlnxqos --transform 's,.*/,,' && chmod +x /tmp/mlnxqos/mlnx_qos"
  done
}

stor() {
  local rb wb props="" d
  rb=$(python3 -c "print(int($1 * $READ_ONE * 1024**3))"); wb=$(python3 -c "print(int(${2:-$1} * $WRITE_ONE * 1024**3))")
  for d in "${NVME[@]}"; do props="$props IOReadBandwidthMax=$d\\ $rb IOWriteBandwidthMax=$d\\ $wb"; done
  for ip in "${STOR[@]}"; do r $ip "systemctl set-property --runtime storage_main.service $props"; done
  echo "디스크 상한: NVMe 하나당 읽기 $(python3 -c "print(f'{$rb/1024**3:.3f}')") 쓰기 $(python3 -c "print(f'{$wb/1024**3:.3f}')") GiB/s" \
       "(노드당 ${#NVME[@]}개, $(( ${#STOR[@]} * ${#NVME[@]} ))개 합계 읽기 $(python3 -c "print(f'{${#STOR[@]}*${#NVME[@]}*$rb/1024**3:.2f}')") GiB/s)"
}
stor_reset() { for ip in "${STOR[@]}"; do r $ip "systemctl set-property --runtime storage_main.service IOReadBandwidthMax= IOWriteBandwidthMax="; done; }

rates() { local q; q=$(printf "$1,%.0s" 1 2 3 4 5 6 7); echo "$q$1"; }
net() {
  ensure_qos
  local qc qs
  qc=$(rates "$1"); qs=$(rates "${2:-$1}")
  for i in "${!STOR[@]}"; do r ${STOR[$i]} "$QOS_STOR -i ${STOR_IF[$i]} -r $qs" > /dev/null; done
  for i in "${!CLI3[@]}"; do r ${CLI3[$i]} "mlnx_qos -i ${CLI_IF[$i]} -r $qc" > /dev/null; done
  echo "망 송신 상한: 클라이언트 $1 · storage ${2:-$1} Gbit/s"
}
net_reset() {
  ensure_qos
  for i in "${!STOR[@]}"; do r ${STOR[$i]} "$QOS_STOR -i ${STOR_IF[$i]} -r 0,0,0,0,0,0,0,0" > /dev/null; done
  for i in "${!CLI3[@]}"; do r ${CLI3[$i]} "mlnx_qos -i ${CLI_IF[$i]} -r 0,0,0,0,0,0,0,0" > /dev/null; done
}

show() {
  ensure_qos
  for i in "${!STOR[@]}"; do
    echo "${STOR[$i]} io.max: $(r ${STOR[$i]} 'cat /sys/fs/cgroup/system.slice/storage_main.service/io.max 2>/dev/null' | tr '\n' ' ')" \
         "maxrate: $(r ${STOR[$i]} "$QOS_STOR -i ${STOR_IF[$i]}" | grep -o 'ratelimit: [^,]*' | sort | uniq -c | tr '\n' ' ')"
  done
  for i in "${!CLI3[@]}"; do echo "client ${CLI3[$i]}: $(r ${CLI3[$i]} "mlnx_qos -i ${CLI_IF[$i]}" | grep -o 'ratelimit: [^,]*' | sort | uniq -c | tr '\n' ' ')"; done
}

case $1 in stor | net | reset) [ "$ALLOW_THROTTLE" = 1 ] || { echo "$SM_CLUSTER: [allow] throttle = false"; exit 1; } ;; esac
case $1 in
  stor) stor "$2" "$3" ;;
  net) net "$2" "$3" ;;
  reset) stor_reset; net_reset ;;
  show) show ;;
  *) echo "usage: $0 stor FREAD [FWRITE] | net GBIT [STOR_GBIT] | reset | show"; exit 1 ;;
esac
