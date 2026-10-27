# 스토리지 축소 모형

## 왜 만드나

실제 서비스 클러스터는 우리 손에 없고, 있어도 운영 중이라 실험할 수 없다.
PoC 클러스터는 있지만 연산(GPU·통합 메모리)이 먼저 막혀서 스토리지가 한 번도 바빠지지 않는다.
그래서 PoC 에서는 "돌아간다"까지만 알 수 있고, 스토리지 최적화가 실제 클러스터에서 얼마나 효과가 있을지는 모른다.

이 도구는 추론 엔진 없이 LMCache L2 adapter 를 직접 구동해, 공개 trace 가 만드는 3FS IO 를 **replay 배속을 올려 가며** 낸다.
설정마다 따라가는 최대 배속을 찾아 견주고, 그때의 IO 를 3FS 경로 천장(fio-usrbio)과 디스크 천장에 대 본다.
최적화 전·후를 같은 plan 으로 돌려 request 단위로 비교한다.

## 흐름

```
trace + serving config ─→ plan.py (cache_sim.py 로 request 마다 3FS load·store 계산, 배속마다 plan)
                                 │  plans/<이름>/x0.5.json … max.json
                                 ▼
                          replay.py (클라이언트 여러 대가 같은 시각에 driver.py 로 adapter 구동, 카운터 측정)
                                 │  results/<이름>/<배속>/summary.json
                                 ▼
target spec (선택) ─────→ report.py (곡선, 대상 운영점, --baseline 과 request 단위 비교)

ceiling.py (모든 클라이언트 fio-usrbio) ─→ clusters/<이름>-ceiling.json (3FS 경로 천장 — 결과의 "3FS 천장 대비")
```

## 천장 대비 값

sweep 은 replay 배속으로 돈다. 천장은 결과를 3FS · 하드웨어가 낼 수 있는 양에 대 보는 참고 값이고, 없으면 그 값만 빈다.

| 이름 | 뜻 | 분모 |
|---|---|---|
| `of_3fs_load` · `of_3fs_store` | adapter 가 낸 load · store ÷ 3FS 경로 천장 | `ceiling.py` — 모든 클라이언트가 동시에 fio-usrbio 로 읽고 쓴 합 |
| `disk_read` | storage 디스크 읽기 사용률 | `[measured] disk_read_gibps` — storage NVMe 전체 동시 읽기 합 |
| `disk_write` | storage 디스크 쓰기 사용률 (복제 포함) | `[measured] disk_write_gibps` |
| `client_link` | 가장 바쁜 클라이언트 링크 한 방향, payload | `[measured] link_gibps` — 노드 하나의 선로 합 |
| `storage_link` | storage 링크 한 방향, payload (store 는 chain 복제 수만큼) | 같음 |
| `*_link_wire` | 링크 사용률을 RDMA 포트 카운터로 (헤더·재전송 포함) | 같음 |

## 쓰는 법

```bash
python3 ceiling.py                                                     # 3FS 경로 천장 (처음 한 번, storage 설정을 바꾼 뒤)
python3 plan.py alibaba-coder --out plans/coder                        # 기본 배속 0.5,1,2,3,4,max
python3 plan.py alibaba-coder --serving-cfg targets/<서빙 설정>.json --out plans/coder-l1big
python3 replay.py plans/coder --out results/coder                      # 배속을 차례로
python3 report.py results/coder --target targets/<대상>.json           # 곡선 + 대상 운영점
python3 replay.py plans/coder --stripe 1 --out results/coder-s1        # 스토리지 쪽 설정을 바꿔 한 번 더
python3 report.py results/coder-s1 --baseline results/coder --noise results/coder-again
```

trace: `alibaba-chat(A)` `alibaba-api(B)` `alibaba-thinking` `alibaba-coder` `mooncake-conversation` `mooncake-toolagent` (`traces/`)

## 파일

| 파일 | 역할 |
|---|---|
| `cache_sim.py` | 캐시 시뮬레이터 — LMCache MP 모드 (GPU prefix cache · L1 · 3FS). 규칙과 근거는 머리말 |
| `plan.py` | trace 구간 → 배속별 클라이언트 event 목록 (sweep) |
| `replay.py` | 여러 대 동시 replay + 디스크·NIC 카운터로 사용률 측정 |
| `report.py` | 곡선, 대상 운영점, request 단위 A/B |
| `driver.py` | 클라이언트 한 대의 실행기 (`--mode replay`. `run`·`calibrate`·`prepare` 는 한계 측정용) |
| `hw_probe.py` / `hw_sampler.py` | 장비별 CPU·디스크·RDMA·NIC 카운터, 제한 없는 최대 부하 시험 |
| `ceiling.py` | 3FS 경로 천장 — 모든 클라이언트가 같은 시각에 fio-usrbio(1 MiB, iodepth 32)로 덮어쓰고 읽은 합 |
| `traces.py` | trace 목록과 prompt 만들기 (`hash_ids` → token) |
| `calibration.json` | 소프트웨어의 성질(object 모양, adapter)과 PoC 실측 기록. 클러스터마다 다른 값은 `cluster.py` 가 그 클러스터의 것으로 덮어 driver 에 보낸다 |
| `cluster.py` · `clusters/` | 클러스터 고유 값(호스트 · 계정 · 3FS 경로 · storage node · NIC · NVMe · 천장 · 바꿔도 되는 것)은 모두 `clusters/<이름>.toml` 에 있다. `discover` · `check` · `env`. "다른 클러스터로 옮기기" |
| `tune/` | 설정 sweep 도구 — `tune.py`(adapter 설정 → 클러스터 설정 순서로 따라가는 최대 배속. x1 에서 두 배씩 건너 경계를 찾고 15% 칸으로 좁힌다), `screen.py`(후보 거르기 · 다시 돌리기), `settings.py`(FUSE 템플릿 · storage hot-update 걸기 · 확인 · 되돌리기), `analyze.py`(판정), `families.py`(plan 묶음), `guard.py`(남의 작업), `knobs.py` → `KNOBS.md`(손잡이 목록 — 그 클러스터의 결과에서 낸다) |
| `throttle.sh` | 디스크(NVMe 마다 읽기·쓰기 따로)·망(클라이언트·storage 포트 따로) 성능 상한을 걸고 푼다. 망 상한은 송신만 막아 incast 손실이 사라지는데, 이 차이는 무시하고 쓴다 |
| `targets/serving-base.json` | 기준 서빙 설정 (Qwen3-14B, GPU KV 24 GiB, L1 32 GiB, LMCache 기본값) |
| `targets/*.json` | 대상 클러스터 사양 (`report.py --target`) |
| `validate/clean_iov.sh` | 죽은 프로세스가 남긴 hf3fs iov(공유 메모리)를 지운다. replay 가 실행마다 클라이언트에서 부른다 |

replay 는 실행마다 이 장비의 `driver.py` 를 다른 클라이언트의 작업 폴더에 덮어쓰고, calibration 은 `/tmp/scale-model-calibration.json` 으로 보낸다.

## 다른 클러스터로 옮기기

클러스터마다 다른 값은 코드에 없고 `clusters/<이름>.toml` 하나에 있다. 도구는 이 장비의 hostname 이 `[[client]]` 에 든 파일을 고른다.

1. 가져갈 것: 이 저장소, 공개 trace, 클라이언트마다 LMCache 빌드
   - trace: `git clone https://github.com/alibaba-edu/qwen-bailian-usagetraces-anon traces/alibaba` (Git LFS). Mooncake trace 는 Mooncake 저장소의 `FAST25-release/traces` 를 `traces/mooncake/` 에 둔다(sweep 은 Alibaba 만 쓴다)
   - LMCache: 그 클러스터의 3FS 라이브러리(`libhf3fs_api_shared`)로 hf3fs 확장을 빌드해 `$HOME/venv-lmcache-hf3fs` · `$HOME/LMCache-hf3fs-atomic` 처럼 `tune/tune.py` 의 `VERSIONS` 가 가리키는 이름으로 둔다. 클라이언트마다 같은 빌드여야 한다(replay 가 지문으로 견준다)
2. `cp clusters/TEMPLATE.toml clusters/<이름>.toml` 하고 `[access]` 를 채운다. 비밀번호는 파일 경로만 적는다(권한 600, 저장소 밖). 원격에서 돌릴 계정이 ssh 계정과 다르면 `runs_as`
3. `SM_CLUSTER=<이름> python3 cluster.py discover <이름>` — admin_cli(`list-nodes` · `list-clients` · `list-chains` · FUSE 템플릿 · storage 설정)와 각 노드(RDMA 포트가 ACTIVE 인 NIC · NVMe · 마운트 · python · 도구 사본 · FUSE 서비스와 로그)에서 찾은 값으로 초안을 다시 내고, 지금 FUSE 템플릿을 되돌릴 곳으로 받아 둔다. admin_cli · admin_host 가 비어 있으면 3FS 기본 설치 경로와 이 장비로 해 본다
4. 초안의 빈칸과 `# 확인` 을 사람이 채운다. `[allow]` 는 클러스터 주인에게 묻는다. 디스크 · 링크 천장은 선택이다 — 적으면 사용률(%)이 나오고, 사양값이면 그렇게 적어 둔다
5. `python3 cluster.py check` 가 "통과", `python3 tune/settings.py home` 이 "HOME 과 같다"
6. `python3 ceiling.py` 로 3FS 경로 천장을 잰다. fio-usrbio 가 없으면 3FS 소스의 `benchmarks/fio_usrbio` 로 빌드하고 경로를 `[builds]` 에 적는다
7. `python3 tune/tune.py sweep a --trace coder` 처럼 돌린다(첫 배속 x1, `--start` 로 바꾼다). 허락이 없는 쪽을 바꾸는 설정은 건너뛰고, 기준도 그쪽은 지금 값으로 둔다

- **FUSE 템플릿은 mgmtd 에 하나라 그 클러스터의 모든 FUSE 클라이언트에 걸린다.** `list-clients` 에 목록 밖 클라이언트가 보이면 도구가 템플릿을 바꾸지 않는다
- storage hot-update 는 storage 를 다시 띄우면 풀린다. replay 가 실행 앞뒤의 storage PID 를 견줘, 다르면 그 실행을 무효로 하고 설정을 다시 건다
- storage 노드가 클라이언트도 맡으면 같은 host 를 `[[client]]` 와 `[[storage]]` 에 둘 다 둔다. 그 장비의 링크 카운터는 클라이언트 · storage 양쪽에 같은 값으로 잡힌다
- 시작 때 클러스터가 되돌릴 곳에 있지 않으면 `tune.py` · `screen.py` 가 멈춘다 — 되돌리기가 남의 값을 지우지 않게
- 옮기지 않는 것: `tune.py verify`(실제 vLLM 대조, GPU 필요 — 대조 스크립트는 이 저장소에 없다), `throttle.sh`(PoC 에서 비율을 맞추던 것, `[allow] throttle`)
- **그 클러스터의 값과 결과는 저장소에 올리지 않는다.** `.gitignore` 가 `clusters/` 에서 `TEMPLATE.toml` 만 남기고, `results/` · `plans/` · `traces/` · `tune/KNOBS.md` 를 뺀다
