# 작업 지시서 — 이 도구를 돌리는 AI 에이전트용

무엇을 왜 재는지, 무엇을 어떤 순서로 재는지, 결과를 어떻게 내는지는 이미 정해져 있다. 에이전트는 이 문서의 순서대로 실행하고 보고만 한다.
여기 없는 일을 해야 할 것 같으면 하지 않고 사용자에게 묻는다. 보고는 한국어로 한다.

## 이 일의 목적

실제 서비스 클러스터에서 LMCache 의 3FS L2 adapter 중 무엇을, 어떤 설정으로 써야 KV cache IO 를 가장 잘 따라가는지 알고 싶다.
이 도구는 추론 엔진 없이, 공개 trace 가 만드는 3FS load · store 를 실제 adapter 코드로 재생(replay)한다. 재생 배속을 올려 가며 **따라가는 최대 배속**을 잰다. 같은 하드웨어에서 최대 배속이 높을수록 그 adapter · 설정이 더 큰 부하를 견딘다.

- **adapter 셋(`hf3fs` · `nixl_store_dynamic` · `nixl_native`)을 같은 조건에서 견주는 것이 이 일의 핵심이다.** 하나만 재면 결과가 아니다. 셋 다 설치하고 셋 다 잰다
- 낼 결과: trace x 모델마다 adapter 셋의 최대 배속, adapter 마다 가장 좋은 knob · 클러스터 설정, 그 위 배속에서 무엇이 막혔나
- 무엇을 어떤 순서로 잴지는 `tune/agent.py` 의 계획이 정한다(아래 "측정"). 계획을 줄이거나 바꾸지 않는다

| 말 | 뜻 |
|---|---|
| adapter | LMCache 가 3FS 에 KV 를 읽고 쓰는 코드. 셋 |
| knob | adapter 의 설정 한 벌(worker 수 · 폴더 수 · 메모리 방식). 이름은 `agent.py coverage` 에 다 나온다 |
| 클러스터 설정 | 3FS FUSE · storage 설정 한 벌(`base` 가 기준). 도구가 걸고 끝나면 되돌린다 |
| trace · 모델 | 재생할 요청 구간과 KV 크기. 아래 표 |
| 배속 · search | 배속 = trace 를 몇 배 빠르게 재생하나. search = adapter · knob · 클러스터 설정 · trace · 모델 하나의 최대 배속을 찾는 실행 묶음(1-2시간) |
| 통과 | 요청 시작 늦음 p99 1초 미만 + store 를 plan 의 95% 이상 끝냄 + 못 찾은 load 1% 미만 + 무효가 아님 |

## 하지 않는 것

사용자가 그 일을 콕 집어 시킨 때만 예외다.

1. 계획을 줄이지 않는다 — adapter · knob · 클러스터 설정 · trace · 모델을 빼지 않는다. 설치가 어렵다고 adapter 를 건너뛰지 않는다. 막히면 그 자리에서 멈추고 보고한다
2. 따로 측정 스크립트 · 모니터를 만들지 않는다. 필요한 숫자(망 버림 · 재전송 포함)는 도구가 실행마다 이미 잰다("도구가 재는 것")
3. 코드(`*.py` · `*.sh`)와 `calibration.json` 을 고치지 않는다. 도구가 에러로 멈추면 고치거나 우회하지 않고 "문제가 생기면"의 표대로 한다
4. 클러스터 설정을 바꾸는 명령을 직접 치지 않는다 — `admin_cli set-config` · `hot-update-config` · `set-layout`, `systemctl restart`, FUSE 다시 띄우기(`tune/fuse_write.py` 도).
   FUSE 쓰기 admission · 쓰기 묶음은 FUSE 를 다시 띄워야 바뀌고 그 노드의 다른 사용자 마운트도 끊겨서 계획에 넣지 않았다(`w32` · `src-w32`)
5. `clusters/<이름>.toml` 의 `[access]` · `[allow]` · `[home]` · `[ref]` 를 고치지 않는다. 비밀번호를 파일이나 대화에 적지 않는다(`[builds]` 는 "설치"대로 채운다)
6. 3FS 에서 아무것도 지우지 않는다(`rm` 을 쓰지 않는다). 재생 폴더는 도구가 지운다
7. 다른 사람이 클러스터를 쓰는 중이면 측정을 띄우지 않는다. 측정을 두 개 함께 돌리지 않는다
8. `tune.py verify` · `throttle.sh` · `validate/` 아래 스크립트를 돌리지 않는다. `raw_hf3fs` 를 돌리지 않고, LMCache PR 5828 의 test 를 돌리지 않는다(그 test 의 `cleanup_data_files()` 는 클러스터의 모든 파일 chunk 를 지운다)
9. 판정 기준(`tune/analyze.py` 의 `LATE_MAX` · `STORE_MIN` · `MISS_MAX`)을 바꾸지 않는다
10. 숫자를 짐작하지 않는다. 결과 파일 · 로그 · `agent.py` 출력에 있는 것만 쓴다
11. `git commit` · `git push` 를 하지 않는다. `results/` · `plans/` · `clusters/<이름>.toml` 을 저장소에 넣지 않는다

## 명령 쓰는 법

권한 확인에 자주 막히지 않게, 명령은 이 문서에 적힌 모양 그대로 쓴다.

- Bash 호출 하나에 명령 하나. `&&` · `|` · `;` 로 잇지 않는다. `cd` 하지 않는다(늘 저장소 폴더에서)
- 측정 · 점검 · 결과는 모두 `python3 tune/agent.py <명령>` 하나로 한다. `kill` · `setsid` · `nohup` · `sleep` · `ssh` · `sudo` · `python3 -c` 를 직접 쓰지 않는다
- 사용자가 한 번 아래를 그 장비의 `.claude/settings.local.json` 에 넣으면 이 명령들은 묻지 않고 돈다. 에이전트는 이 파일을 고치지 않고 사용자에게 넣어 달라고 한다

```json
{
  "permissions": {
    "allow": [
      "Bash(python3 tune/agent.py:*)",
      "Bash(python3 tune/analyze.py:*)",
      "Bash(python3 tune/settings.py state)",
      "Bash(python3 tune/settings.py home)",
      "Bash(python3 cluster.py show)",
      "Bash(python3 cluster.py check)",
      "Bash(python3 ceiling.py)",
      "Bash(git pull)",
      "Bash(git status)"
    ]
  }
}
```

## 요청별로 할 일

| 사용자 요청 | 할 일 |
|---|---|
| 처음 설치, adapter 가 안 깔렸다 | "설치" → "클러스터 파일 · 천장" |
| 측정, sweep, adapter 비교, "다 돌려" | "측정" — 계획을 끝까지 |
| 결과를 알려 달라 | "결과 보고" |
| 지금 망 버림 · 디스크가 어떠냐 | "도구가 재는 것" |
| 돌던 측정을 멈춰 달라 | `python3 tune/agent.py stop` |
| 계획 밖의 것을 콕 집어 시켰다 | `python3 tune/agent.py start ...` (`agent.py` 의 도움말) |
| 에러, 멈춤 | "문제가 생기면" |
| 이 표에 없는 것 | 하지 않고 묻는다 |

## 설치

모든 클라이언트(replay 를 돌리는 노드 — `clusters/<이름>.toml` 의 `[[client]]`)에 adapter 셋이 다 돌게 한다. 끝나면 `python3 tune/agent.py install` 이 "설치 확인 통과" 여야 한다.
`<venv>` 는 `[[client]] python` 의 venv 폴더(예: `$HOME/venv-lmcache`)다. 한 단계가 실패하면 출력을 그대로 보고하고 멈춘다.
설치 명령은 적힌 그대로 친다. 이 절에서만 `cd` 를 쓴다.

1. NIXL — `nixl_store_dynamic` · `nixl_native` 가 쓴다. 버전은 정하지 않는다(LMCache 는 1.3.0 이상이면 된다). HF3FS plugin 은 cu13 wheel 에만 있어 셋을 다 깐다

   ```bash
   <venv>/bin/pip install -U nixl nixl-cu12 nixl-cu13
   <venv>/bin/pip show nixl
   ```

   `Version:` 줄의 값이 아래 `<nixl 버전>` 이다. 보고에 적는다(Spark 측정은 1.4.1 이었다).
   nixl 은 torch 의 CUDA 빌드(12 · 13)에 맞는 backend 를 고른다. torch 가 CUDA 12 빌드면 HF3FS plugin 이 없는 cu12 를 골라 nixl adapter 가 못 돈다 — `agent.py install` 이 이것을 잡는다. 잡히면 보고한다

2. NIXL C++ 헤더 — `nixl_native` 의 확장(`lmcache_nixl`)을 빌드할 때 쓴다. 설치한 NIXL 과 같은 버전의 tag 에서 받는다. 받을 수 없으면 사용자에게 네 파일을 옮겨 달라고 한다

   ```bash
   mkdir -p $HOME/nixl-include
   curl -fsSL -o $HOME/nixl-include/nixl.h https://raw.githubusercontent.com/ai-dynamo/nixl/v<nixl 버전>/src/api/cpp/nixl.h
   curl -fsSL -o $HOME/nixl-include/nixl_descriptors.h https://raw.githubusercontent.com/ai-dynamo/nixl/v<nixl 버전>/src/api/cpp/nixl_descriptors.h
   curl -fsSL -o $HOME/nixl-include/nixl_params.h https://raw.githubusercontent.com/ai-dynamo/nixl/v<nixl 버전>/src/api/cpp/nixl_params.h
   curl -fsSL -o $HOME/nixl-include/nixl_types.h https://raw.githubusercontent.com/ai-dynamo/nixl/v<nixl 버전>/src/api/cpp/nixl_types.h
   ```

3. LMCache 를 README 의 "기준 버전" commit 으로 adapter 셋 모두 넣어 빌드한다 — 시뮬레이터 규칙이 그 commit 에 맞춰져 있다(아래 `<commit>`). 한 노드에서 빌드하고, 다른 클라이언트에는 같은 경로로 복사한다(빌드 지문이 같아야 replay 가 돈다)

   ```bash
   git clone https://github.com/LMCache/LMCache.git $HOME/LMCache-<commit>
   git -C $HOME/LMCache-<commit> checkout <commit>
   ```

   빌드는 `$HOME/LMCache-<commit>` 폴더에서 한 줄로 친다. `<absl>` · `<3fs lib>` 는 hf3fs 를 처음 빌드할 때 쓴 값(Abseil 헤더 폴더 · `libhf3fs_api_shared.so` 가 있는 폴더, 보통 `/opt/3fs/lib`)이다. python 이 3.12 가 아니면 `python3.12` 를 그 버전으로 바꾼다

   ```bash
   NO_GPU_EXT=1 BUILD_WITH_HF3FS=1 BUILD_HF3FS=1 HF3FS_INCLUDE_DIR=<absl> HF3FS_LIB_DIR=<3fs lib> BUILD_WITH_NIXL=1 NIXL_INCLUDE_DIR=$HOME/nixl-include NIXL_LIBRARY_DIR=<venv>/lib/python3.12/site-packages/.nixl_cu13.mesonpy.libs <venv>/bin/python setup.py build_ext --inplace > $HOME/lmcache-<commit>-build.log 2>&1
   ```

   `$HOME/lmcache-<commit>-build.log` 에 `Using explicitly requested storage backend: hf3fs` 와 `...: nixl` 두 줄이 있어야 한다. 그다음 `clusters/<이름>.toml` 에 적는다

   ```toml
   [builds.hf3fs]
   pythonpath = "$HOME/LMCache-<commit>"
   [builds.nixl_store_dynamic]
   pythonpath = "$HOME/LMCache-<commit>"
   [builds.nixl_native]
   pythonpath = "$HOME/LMCache-<commit>"
   ```

4. pwrite shim — `dram_zc` 가 붙은 knob 이 쓴다(L1 이 2 GiB 를 넘으면 NIXL HF3FS plugin 이 등록에 실패하는 것을 피한다). 모든 클라이언트에서

   ```bash
   gcc -O2 -shared -fPIC -o $HOME/pwrite_full.so tune/pwrite_full.c -ldl
   ```

   `clusters/<이름>.toml` 의 `[builds]` 에 `pwrite_shim = "$HOME/pwrite_full.so"` 를 적는다

5. fio-usrbio — `ceiling.py`(3FS 경로 천장)가 쓴다. 모든 클라이언트에 `/opt/3fs/bin/fio-usrbio` 와 `/opt/3fs/lib/fio/hf3fs_usrbio.so` 가 이미 있으면 건너뛴다.
   없으면 소스로 빌드한다 — fio(Spark 와 같은 3.36)와 3FS 소스의 `benchmarks/fio_usrbio` 엔진. 3FS 소스는 설치된 3FS 와 같은 commit 으로 둔다:
   `strings <hf3fs_fuse_main 경로> | grep -m1 "Version: v"` 줄의 마지막 `-` 뒤가 그 commit 이다(아래 `<3FS commit>`)

   ```bash
   git clone --branch fio-3.36 --depth 1 https://github.com/axboe/fio.git $HOME/fio
   cd $HOME/fio
   ./configure
   make -j
   cd -
   git clone https://github.com/deepseek-ai/3FS.git $HOME/3FS-src
   git -C $HOME/3FS-src checkout <3FS commit>
   make -C $HOME/3FS-src/benchmarks/fio_usrbio HF3FS_LIB_DIR=/opt/3fs/lib HF3FS_INCLUDE_DIR=$HOME/3FS-src/src/lib/api FIO_SRC_DIR=$HOME/fio
   ```

   `HF3FS_LIB_DIR` 는 `libhf3fs_api_shared.so` 가 있는 폴더다(보통 `/opt/3fs/lib`). 모든 클라이언트에 같은 경로로 두고(노드마다 같은 명령, 또는 한 곳에서 빌드해 복사),
   `clusters/<이름>.toml` 의 `[builds]` 에 **절대 경로로** 적는다 — `ceiling.py` 가 경로를 따옴표로 감싸 `$HOME` 이 풀리지 않는다

   ```toml
   fio_usrbio = "/home/<계정>/fio/fio"
   usrbio_engine = "/home/<계정>/3FS-src/benchmarks/fio_usrbio/hf3fs_usrbio.so"
   hf3fs_lib = "/opt/3fs/lib"
   ```

6. `python3 tune/agent.py install` — 클라이언트마다 adapter 셋의 import · NIXL backend(cu13)와 버전 · NIXL HF3FS plugin · shim · 빌드 지문을 본다. "설치 확인 통과" 가 나올 때까지 측정하지 않는다

## 클러스터 파일 · 천장

처음 한 번. 한 단계가 기대와 다르면 거기서 멈추고 출력을 그대로 보고한다.

| 순서 | 명령 | 기대 |
|---|---|---|
| 1 | `python3 cluster.py show` | 고른 클러스터 파일 이름. 파일이 없으면 사용자에게 `clusters/TEMPLATE.toml` 을 복사해 `[access]` 를 채워 달라고 한다 |
| 2 | `SM_CLUSTER=<이름> python3 cluster.py discover <이름>` | 초안. 빈칸과 `# 확인` 줄을 목록으로 사용자에게 보여 주고 사람이 채운다. 추측해서 채우지 않는다 |
| 3 | `python3 cluster.py check` | 마지막 줄 "통과" |
| 4 | `python3 tune/settings.py home` | "HOME 과 같다" |
| 5 | `python3 ceiling.py` | `clusters/<이름>-ceiling.json`. 천장 둘을 보고한다: 하드웨어 계산(`하드웨어 read · write` 줄과 먼저 차는 곳)과 fio-usrbio 로 잰 3FS 경로(`read 합` · `write 합`), 그리고 `fio ÷ 하드웨어`. `!! 디스크 사양값이 없어` 가 나오면 그 줄의 NVMe 모델을 사용자에게 보여 주고 사양서의 순차 읽기 · 쓰기(GB/s)를 받아 `[hardware]` 에 적은 뒤 다시 돌린다. 추측해서 적지 않는다. storage 설정이 바뀐 뒤에만 다시. fio-usrbio · 엔진이 없다고 하면 "설치" 5 |

## 측정

### 계획

`python3 tune/agent.py plan` 이 단계마다 끝 · 남음 · 기다림과 남은 시간 어림을 보인다. 단계는 아래 순서이고, 앞 단계 결과로 다음 단계의 knob · 설정을 도구가 고른다.

| 단계 | 무엇 | search 수 |
|---|---|---|
| 1 knob 전부 | 기준 상황(coder x Qwen3-14B, 클러스터 `base`)에서 adapter 셋의 knob 전부 | 21 |
| 2 클러스터 설정 | adapter 마다 1 의 가장 좋은 knob 에서 클러스터 설정 다섯(`[allow]` 가 없는 것은 도구가 뺀다) | 15 이하 |
| 3 경계 확인 | adapter 마다 가장 좋은 조합의 최대 배속과 그 위 한 칸을 두 번씩 더 | 실행 12 |
| 4 다른 trace · 모델 | 아래 다섯 상황에서 adapter 마다 1 의 가장 좋은 knob | 15 |

search 하나가 15-45분(어림)이라 계획 전체는 13-39시간쯤 걸린다. 사람이 끊지 않는 한 계획이 다 끝날 때까지 이어 간다. 계획을 줄이거나 단계를 건너뛰지 않는다 — 사용자가 adapter · trace · 모델을 콕 집어 시키면 그때만 그것을 `agent.py start` 로 돈다(knob 목록은 `agent.py coverage --trace T --model M`).

### 되풀이

이 다섯을 계획이 다 끝날 때까지 되풀이한다.

1. `python3 tune/agent.py next` — 점검(다른 사람의 작업 · storage 디스크 · 돌고 있는 측정 · HOME · 3FS 여유)을 하고, 통과하면 남은 첫 단계를 띄운다. 점검이 실패하면 아무것도 띄우지 않는다 → 실패한 줄을 그대로 사용자에게 보고하고 기다린다
2. 15분마다 `python3 tune/agent.py status` — 끝난 실행 줄이 쌓인다. "→ 돌고 있다"면 계속 기다린다
3. "→ 끝났다"면 사용자에게 한 줄로 알린다: 끝난 단계 · adapter · 그 status 의 마지막 `최대` 줄
4. 단계 번호(1 · 2 · 3 · 4)가 하나 다 끝났으면 `python3 tune/agent.py report` 를 사용자에게 보낸다("결과 보고")
5. 1 로 돌아간다. `plan` 이 "계획이 다 끝났다"면 마지막 `report` 를 보내고 마친다

"→ 끝나지 않고 멈췄다"면 "문제가 생기면"으로 간다.

### trace · 모델

trace 파일은 `traces/`(Alibaba 공개 trace, Git LFS)에 있다. plan(재생할 load · store 목록)은 도구가 처음 쓸 때 시뮬레이터로 계산해 `plans/tune/` 에 둔다. trace · 모델 목록은 `tune/tune.py` 의 `WORKLOADS` · `MODELS` 이고 고치지 않는다.

| 이름 | 무엇 | 계획에서 |
|---|---|---|
| `coder` | Alibaba coder trace 30분부터 3분 | 기준 상황 |
| `coder60` | 같은 trace 60분부터 3분 | 4 |
| `apib` | Alibaba api(B) 30분부터 5분 | 4 |
| `chata` | Alibaba chat(A) 30분부터 9분 | 4 |
| `qwen3-14b` | key 하나 40 MiB | 기준 상황 |
| `ds-v3` | key 하나 17 MiB (MLA) | 4 |
| `llama-70b` | key 하나 80 MiB | 4 |

## 도구가 재는 것

실행(배속 하나, 5-20분)마다 아래를 이미 잰다. "망 버림이 있냐" 같은 질문에는 따로 재지 말고 이 값으로 답한다. 실행이 도는 중이면 그 실행의 값은 끝나야 나온다 — "가장 최근에 끝난 실행 <tag> (<시각>) 기준"이라고 밝히고 그 값을 말한다.

| 무엇 | 값 이름(`analyze.py --json`) | 실행 줄에서 |
|---|---|---|
| request 시작 늦음 | `late_p50` · `late_p99` | `늦음 p99` |
| request 당 load · lookup 시간 | `load_s_p50` · `load_s_p99` · `lookup_s_p99` | `load p50 … p99` |
| 따라간 양 | `load` · `store` · `store_ratio` · `missed_frac` | `load a/b` · `store a/b (비율)` · `못 찾음` |
| storage 디스크 | `disk_read` · `disk_write` · `disk_util_pct` · `disk_queue` | `디스크 r · w` |
| 링크 | `client_rx_max` · `client_tx_max` · `storage_rx_max` · `storage_tx_max` (GiB/s) | |
| 망 손실 | `storage_rx_discards`(storage NIC 수신 버림) · `client_oos` · `storage_oos`(순서 어긋남) · `client_retrans_to` · `storage_retrans_to`(재전송 시간 초과) | `버림` |
| CPU | `client_cpu_pct` · `storage_cpu_pct` | |
| 3FS 천장 대비 | `of_3fs_load` · `of_3fs_store` | `3FS 천장 대비` |
| 무효 | `valid` (storage 재시작 · 멈춤 · 출발 늦음) | `!! 무효` |

- 실행 폴더: `results/tune-speed/<adapter>[-trace][-model]/runs/<knob>+<설정>-x<배속>/` (`summary.json` · `driver-<호스트>.json` · `raw-<호스트>.jsonl` = 노드마다 1초 간격 카운터)
- 한 실행의 한 줄: `python3 tune/analyze.py <실행 폴더>`
- 모든 값: `python3 tune/analyze.py --json results/tune-speed/metrics-<tag>.json <실행 폴더>` 로 쓰고 그 파일을 읽는다

## 결과 보고

`python3 tune/agent.py report` 가 보고 표를 만든다(`results/tune-speed/report-<시각>.md` 에도 쓴다). 그 출력을 고치지 않고 그대로 보내고, 맨 아래 두 줄만 채운다.

- `- 추정:` — 원인을 말하고 싶으면 여기에만, 짐작이라고 밝혀 쓴다. 없으면 "없음"
- `- 무효 · 멈춘 실행:` — `status` · 로그에서 본 `!! 무효` · 멈춤의 tag 와 이유. 없으면 "없음"

표 읽는 법:

| 칸 | 뜻 |
|---|---|
| 가장 좋은 knob+설정 | 그 trace x 모델에서 최대 배속이 가장 높았던 조합 |
| `x3.48 3/3 · 위 x4 0/3 store` | x3.48 에서 세 번 중 세 번 통과, 그 위 칸 x4 에서 세 번 중 0 번, 막힌 쪽은 store |
| `(경계 확인 전)` | 최대나 그 위 칸을 세 번 다 재지 않았다 — "최대"라고 단정하지 않는다 |
| base 에서 잰 knob | 그 trace x 모델에서 search 가 끝난 knob 수 / 전체 |
| 계획 단계 n/27 끝 | 27 이 아니면 "다 했다"고 쓰지 않는다 |

knob 하나하나의 결과는 `python3 tune/agent.py coverage --trace <trace> --model <모델>` 이다.

## 문제가 생기면

어느 경우든 코드를 고치거나, 옵션을 바꿔 우회하거나, 같은 명령을 세 번 넘게 다시 돌리지 않는다. 아래대로 하고 보고한다.

| 출력 · 로그 | 뜻 | 할 일 |
|---|---|---|
| `점검 실패` | 측정을 띄울 때가 아니다 | 실패한 줄을 그대로 보고하고 기다린다 |
| `설치 확인 실패` | adapter 가 덜 깔렸다 | 실패한 줄을 "설치"의 그 단계로 고치고 `agent.py install` 을 다시. 고칠 수 없으면 보고한다 |
| `다른 작업이 보여 멈춘다` | 측정 중에 다른 사람의 작업이 보여 도구가 스스로 멈췄다 | 보고하고 기다린다. 사용자가 다시 하라고 하면 `agent.py next` |
| `클러스터가 되돌릴 곳에 있지 않다` | 시작 때 클러스터 설정이 HOME 과 다르다 | 고치지 않는다. `python3 tune/settings.py state` 출력을 붙여 보고한다 |
| `우리 목록 밖의 FUSE 클라이언트가 있다` | 클러스터 파일에 없는 FUSE 클라이언트가 붙어 있다 | 보고하고 기다린다 |
| `3FS 여유 … < 15%` | 3FS 가 거의 찼다 | 지우지 않는다. 보고하고 기다린다 |
| `pwrite_full.so 가 없다` 같은 `… 가 없다` | 빌드 · shim 이 그 노드에 없다 | "설치"의 그 단계를 그 노드에서 다시 하고 `agent.py install` |
| `driver 가 실패했다`, `Traceback` | 실행이 에러로 끝났다 | `agent.py status` 의 로그 끝 20줄과 그 실행의 로그(`runs/<tag>-x<배속>.log`) 마지막 30줄을 붙여 보고한다 |
| `!! 무효` | 실행이 무효다 | 측정은 그대로 둔다. 보고의 "무효" 줄에 적는다 |
| 30분 넘게 `status` 의 끝난 실행 수가 그대로다 | 멈췄을 수 있다 | `agent.py status` 출력을 붙여 보고한다. 멈추지 않는다 |

측정이 어떻게 끝났든 마지막에 `python3 tune/settings.py home` 을 돌려 결과를 보고에 적는다.
