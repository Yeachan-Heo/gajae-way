# gajae-way

**GJC 코딩 에이전트를 Discord/Slack/Telegram에 두기: 채팅에서 답하고, 기억하고, 실제 코딩 작업을 실행합니다.**

<p align="center">
  <img src="assets/logo-horizontal.png" alt="gajae-way" width="640">
</p>

<p align="center"><sub>마스코트 아트워크는 <a href="https://github.com/Yeachan-Heo/gajae-code">gajae-code</a>의 브랜딩에서 파생되었습니다 — 저작권 표시는 <a href="assets/NOTICE.md">assets/NOTICE.md</a>를 참고하세요.</sub></p>

<p align="center">
  <a href="https://github.com/Yeachan-Heo/gajae-way/actions/workflows/build.yml"><img alt="Build" src="https://github.com/Yeachan-Heo/gajae-way/actions/workflows/build.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License: AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-blue.svg"></a>
  <img alt="Bun" src="https://img.shields.io/badge/bun-%3E%3D1.4.0-000000?logo=bun&logoColor=white">
  <img alt="Runtime" src="https://img.shields.io/badge/runtime-TypeScript-3178C6?logo=typescript&logoColor=white">
</p>

<p align="center"><strong>당신의 AI는 브라우저 탭에 있으면 안 됩니다. DM에 있어야 합니다.</strong></p>

gajae-way는 AI 코딩 에이전트를 **상주 페르소나**로 변환하는 런타임입니다 — Discord, Telegram, Slack에 앉아서 Markdown으로 당신을 기억하고, 스케줄에 맞춰 스스로 깨어나며, 모든 대화를 자신의 개인 머리에만 보관합니다. 새로운 장소를 확인할 필요가 없습니다 — 이미 사용 중인 곳에서 대화하면 됩니다. 선택적 로컬 `gajaeway-admin` 콘솔은 운영자용이며, 일상 채팅용이 아닙니다.

```text
Discord / Telegram / Slack ──> gajaeway gateway ──> your persona (gjc)
                              │
                    Markdown memory + scheduled/event monitors
```

채팅 프론트엔드는 쉽습니다. 어려운 것은 실제 봇이 몇 달 동안 실행되어야 할 때 발생하는 모든 일입니다: 회신 중 충돌, 동일 대화의 두 메시지 경쟁, 당신을 잊어버리는 에이전트, 한 방을 다른 방과 혼동하는 에이전트, 보낸 적 없는 메시지에 답변하는 에이전트. gajae-way는 그런 것들을 처리하는 지루하지만 견고한 계층입니다 — 위의 페르소나가 흥미로울 수 있도록.

## 존재 이유

- **하나의 존재, 여러 방 — 절대 혼란이 아님.** 모든 대화는 검증된 출처(`platform/kind/conversationId`)이며 자신의 세션과 에포크를 가집니다. 당신의 DM은 당신의 팀 채널이 아닙니다. `/new`는 *그* 대화만 다시 연결하고 다른 대화는 건드리지 않습니다.
- **`cat`으로 읽을 수 있는 기억.** 턴과 모니터 출력은 `$GAJAEWAY_HOME/memory`에 정규 축에 걸쳐 Markdown으로 저장됩니다. 이것은 당신의 파일시스템, 당신의 git 히스토리, 당신의 grep입니다 — 믿어야 하는 벡터 blob이 아닙니다.
- **요청받지 않아도 작동합니다.** Cron과 이벤트 모니터는 페르소나에게 자신의 턴을 제공하므로 일정에 따라 기억을 정규화하거나 당신이 자는 동안 매일 아침 자신을 감시할 수 있습니다.
- **전달은 기대감이 아닌 기장부입니다.** 회신은 전송되기 전에 견고한 기록을 얻습니다. 충돌 후 결과가 불확실한 전송은 다시 발행되며 조용히 아무것도 일어나지 않은 것처럼 가장하는 대신 *명확하게 중복으로 표시됩니다*.
- **침묵할 때를 알고 있습니다.** `dmPolicy`에서 수락한 DM은 언급 없이 참여합니다. 그룹 트래픽은 명시적으로 채널을 열지 않는 한 언급으로 제한됩니다 — 열린 채널도 페르소나가 소음에 대해 침묵을 선택하도록 합니다.
- **gjc가 항상 보는 안전 하한선.** 모든 페르소나 턴은 복구 불가능한 명령(`/`의 재귀적 제거, 파일시스템 포맷팅, 원본 장치 쓰기, 포크 폭탄) 금지 및 `$HOME` 자체 또는 `$HOME`과 `$GAJAEWAY_HOME` 외부의 절대 경로의 재귀적 삭제를 금지하는 비구성 ActionGuard 시스템 지침을 수행합니다; 게이트웨이는 페르소나 명령을 실행하거나 가로채지 않습니다. 구성된 모니터 스크립트는 실행 전에 동일한 하한선에 대해 별도로 검사됩니다.
- **스택이 아닌 독립형 바이너리.** `bun run build`는 컴파일된 실행 파일을 내보냅니다. 프로덕션 호스트는 launchd/systemd에서 이를 실행합니다; 소스 체크아웃은 당신의 노트북에 남아있습니다.

## 느낌

- 당신의 `dmPolicy`에서 수락하는 DM을 보내고 정상적으로 대화하세요. 당신의 봇은 그 대화의 자신의 진행 중인 컨텍스트에서 응답합니다.
- 그룹에서는 당신이 언급할 때까지 비켜납니다. 일반 대화를 위해 명시적으로 채널을 열 수 있습니다.
- 해당 대화에서 새로 시작하려면 `/new`를 보내세요. 새 세션이 시작되었음을 확인합니다.
- 게이트웨이가 DM 또는 주소 지정된 그룹 턴(실제 언급 또는 기본 회신)을 수락하면 Discord와 Slack 모두 반응-그래디언트 존재를 표시합니다 — 위상 마커, 진행 중인 시계, 및 노력 자리(도구 호출, 출력 토큰으로 폴백)가 답변하는 메시지에 직접 반응됩니다; Discord는 플랫폼의 기본 입력 표시기도 표시하고, Slack은 회신 스레드 아래 Slack의 기본 어시스턴트 상태 라인(`<app> <status>`, `assistant.threads.setStatus` 통해)도 구동합니다. Slack은 추가로 어댑터 구성 `open` 채널의 메시지를 주소 지정된 것으로 승격합니다; Discord는 실제 언급 없이 `open` 채널 메시지를 주소 지정된 것으로 취급하지 않습니다. 반응-그래디언트 마커 자체는 게시되거나 편집된 메시지가 아닙니다. 존재는 턴이 계속 작동하는 동안 중간 작업 메시지를 통해 유지되며 턴이 끝날 때만 제거됩니다. 우연히 청취하는 채널에서는 실제로 회신할 때까지 아무것도 표시되지 않습니다.
- 회신은 견고한 전달 기록으로 보호됩니다. 충돌 후 결과가 불확실한 전송은 다시 발행될 수 있습니다; 그럴 경우 조용히 그런 일이 없었던 척하는 대신 명확하게 중복으로 표시됩니다.
- 대화와 유용한 모니터 출력은 소유권 `$GAJAEWAY_HOME/memory` 디렉토리 아래에 Markdown으로 캡처되며, 소유 스토어에 숨겨진 것이 아닙니다.

## 패키지

`packages/` 아래의 Bun 워크스페이스는 책임에 따라 나뉩니다:

| 패키지 | 책임 |
|---|---|
| [`@gajae-gateway/protocol`](packages/protocol) | 버전 NDJSON 프레임, 협상, 동사/이벤트 카탈로그, 정규 출처. |
| [`@gajae-gateway/sdk`](packages/sdk) | 게이트웨이의 Unix 도메인 소켓 또는 stdio 전송을 위한 클라이언트. |
| [`@gajae-gateway/cli`](packages/cli) | 게이트웨이 소켓 위의 소유자 명령. |
| `@gajae-gateway/gateway` | 데몬: 구성, SQLite 상태, 세션, 전달, 기억, 모니터, 및 `gjc` 경계. |
| `@gajae-gateway/adapter-discord` | Discord 수신 및 아웃바운드 전달, 입력 힌트 및 반응-그래디언트 존재 포함. |
| `@gajae-gateway/adapter-telegram` | Telegram 수신 및 아웃바운드 전달. |
| `@gajae-gateway/adapter-slack` | Socket Mode를 통한 Slack 수신, mrkdwn 전달, 반응, 반응-그래디언트 작업 존재, 선택적 라이브 회신, 및 놓친 메시지 복구. |

`@gajae-gateway/protocol`, `@gajae-gateway/sdk`, 및 `@gajae-gateway/cli`는 `@gajae-gateway` 범위 아래에서 npm으로 게시 가능합니다 — 아래 [패키지 게시](#패키지-게시) 섹션을 참고하세요. 게이트웨이, 어댑터, 관리 콘솔, 및 `@gajae-gateway/subsession`은 개인 애플리케이션 코드로 유지되며 아래 컴파일된 바이너리로만 배송됩니다; `@gajae-gateway/conformance`는 개인, 미배송 CI/테스트 전용 패키지입니다.

메모리, 모니터, 및 게이트웨이가 어떻게 끝에서 끝까지 작동하는지는 [문서](docs/)를 참고하세요.

## 5단계로 시작

1. Bun이 있는 머신에서 독립형 프로그램을 빌드하세요:

   ```sh
   bun install
   bun run build
   ```

   이렇게 하면 `dist/gajaeway-gateway`, `dist/gajaeway-discord`, `dist/gajaeway-telegram`, `dist/gajaeway-slack`, `dist/gajaeway-admin`, 및 `dist/gajaeway`가 생성됩니다.

2. 개인 홈 디렉토리를 선택하고 `$GAJAEWAY_HOME/config.json`과 별도 자격 증명 파일을 생성하세요. 게이트웨이 구성은 인라인에 저장된 비밀 값이 아닌 자격 증명 **파일**을 참고합니다. 전체 레이아웃 및 예제는 [배포](docs/deployment.md)를 참고하세요.

3. Discord용 `$GAJAEWAY_HOME/adapter-discord.json`을 자신의 `tokenFile` 참조와 함께 생성하세요. Telegram을 사용할 때 유사한 Telegram 어댑터 구성을 생성하거나, Slack을 사용할 때 `botTokenFile`과 `appTokenFile` 참조를 포함하는 `adapter-slack.json`을 생성하세요.

4. 서비스 관리자 아래에서 게이트웨이를 장기 실행 데몬으로 실행하세요:

   ```sh
   dist/gajaeway-gateway daemon
   ```

   게이트웨이와 브로커는 `gajaeway update`가 `$GAJAEWAY_HOME/gjc`에 설치하는 고정 버전의 `gjc`를 사용합니다; 모든 턴에 대한 AI 런타임을 제공합니다.

5. Discord 어댑터를 시작한 다음 당신의 봇에 DM을 보내세요:

   ```sh
   dist/gajaeway-discord
   ```

   Telegram이나 Slack을 사용할 때는 `dist/gajaeway-telegram` 또는 `dist/gajaeway-slack`을 별도로 시작하세요.

## Discord 봇 토큰 얻기

`gajaeway setup`을 실행하고 Discord를 선택하면 봇 토큰을 얻기 위한 단계별 가이드가 표시됩니다:

1. [Discord 개발자 포털](https://discord.com/developers/applications)로 이동
2. **New Application** 클릭, 이름 입력 (예: "My gajae-way agent")
3. **Bot** → **Add Bot**로 이동
4. **TOKEN** 아래에서 **Reset Token** → **Yes, do it!** 클릭
5. **Copy**를 클릭하여 봇 토큰 복사
6. **중요**: **GATEWAY INTENTS**까지 스크롤하고 **MESSAGE CONTENT INTENT** 활성화 (봇이 메시지를 읽을 수 있도록 함)
7. **Save Changes** 클릭
8. 프롬프트에서 토큰 붙여넣기

검증 후에는 준비가 된 **초대 링크**를 받게 되며, 이를 다른 사람과 공유하거나 직접 클릭하여 Discord 서버에 봇을 추가할 수 있습니다. 링크에는 필요한 권한이 자동으로 포함됩니다.

## 당신의 것으로 만들기

`SOUL.md`, `AGENTS.md`, 및 `USER.md`를 `$GAJAEWAY_HOME/workspace`에 둡니다. `SOUL.md`와 `USER.md`는 각 턴에 대해 읽습니다. gjc는 세션이 시작될 때 `AGENTS.md`를 검색합니다; 나중에 편집이나 삭제는 다음 턴에서 그 프로젝트 컨텍스트 사본을 대체합니다. 워크스페이스는 또한 당신의 페르소나의 작업 디렉토리입니다. 홈 디렉토리를 개인용으로 유지합니다: 이는 구성, 게이트웨이 데이터베이스, 당신의 워크스페이스, 및 기억을 포함합니다.

- [배포 가이드](docs/deployment.md) — 구성, 자격 증명, 및 서비스 관리자 설정
- [기억 가이드](docs/memory.md) — 읽을 수 있는 기억 및 검색
- [모니터 가이드](docs/monitors.md) — 예정된 및 이벤트 기반 작업
- [아키텍처](docs/architecture.md) — 프로토콜 및 신뢰성 세부 사항
- [운영자 런북](docs/runbooks/gajaeway-v1.md) — 복구 및 문제 해결

## 개발

이 저장소는 Bun/TypeScript 워크스페이스입니다.

```sh
bun install
bun run build     # gateway, adapters, admin, 및 CLI를 dist/ 아래의 독립형 바이너리로 컴파일합니다
bun test packages # 전체 패키지 테스트 스위트를 실행합니다
```

프로덕션 호스트는 컴파일된 바이너리를 실행하며, 이 소스 체크아웃이 아닙니다.

### 패키지 게시

`@gajae-gateway/protocol`, `@gajae-gateway/sdk`, 및 `@gajae-gateway/cli`는 `@gajae-gateway` 범위 아래에서 npm으로 게시 가능합니다 — 릴리스 워크플로우는 아래 [패키지 게시](#패키지-게시) 섹션을 참고하세요. 게이트웨이, 어댑터, 관리 콘솔, 및 `@gajae-gateway/subsession`은 개인 애플리케이션 코드로 유지되며 아래 컴파일된 바이너리로만 배송됩니다; `@gajae-gateway/conformance`는 개인, 미배송 CI/테스트 전용 패키지입니다.

각 패키지는 `src/`를 Bun으로 단일 Node-ESM 해석 가능한 `dist/*.js`에 번들하고, `tsc`(`emitDeclarationOnly`)로 `.d.ts` 선언을 내보내며, 게시가 항상 현재 컴파일된 출력을 배송하도록 하는 `prepublishOnly` 스크립트를 가집니다. 게시된 타볼은 `dist/` (비-Bun 소비자에 대해 `main`/`types`/기본 내보내기 조건으로 해석됨) 및 `src/` (설치된 Bun 소비자가 컴파일된 빌드 대신 소스를 직접 가져오도록 `bun` 내보내기 조건으로 해석됨)를 포함합니다. `@gajae-gateway/protocol`은 영 런타임 의존성을 가집니다. 패킹은 순수 npm이 아닌 Bun을 통해야 합니다: `npm pack --dry-run`은 패키지의 내용만 나열하며 의존성을 다시 쓰지 않습니다 — 패키지 디렉토리의 순수 `npm pack`/`npm publish`는 `workspace:*`를 패키징된 매니페스트에 남깁니다, 실제 npm 소비자는 해석할 수 없습니다. `bun pm pack`은 게시되는 각 패키지의 `@gajae-gateway/protocol`/`@gajae-gateway/sdk`에 대한 `workspace:*` 의존성을 패킹되는 정확한 버전으로 다시 씁니다, 레지스트리 유효 매니페스트를 생성합니다 — `bun pm pack`으로 패킹, 스크래치 프로젝트에 타볼 `npm install`, 및 설치된 패키지에 대해 실제 `node --input-type=module -e "import('@gajae-gateway/sdk')"` 실행으로 확인됩니다 (기록: [`artifacts/ultragoal-npm-clean-install-verification.txt`](artifacts/ultragoal-npm-clean-install-verification.txt)).

`scripts/release-packages.ts`는 필요한 의존성 순서(`protocol` → `sdk` → `cli`, `sdk`와 `cli`는 먼저 게시된 버전에 의존하므로)로 전체 릴리스를 실행합니다:

```sh
bun run release:dry-run   # 각 패키지를 빌드한 다음 dist-packed/로 `bun pm pack`합니다 — 레지스트리를 건드리지 않습니다
bun run release:publish   # 각 패키지를 빌드하고 패킹한 다음 해당 타볼을 `npm publish`합니다 (비-`latest` dist-tag에 대해 --tag <name> 추가)
```

드라이 런에서 `dist-packed/*.tgz`를 검사한 다음 `release:publish`를 실행하세요. Bun은 워크스페이스 항목에서 다시 쓰인 버전을 `bun.lock`에서 가져오며, `package.json` 버전 범프 후 `bun install`은 새로 고치지 않습니다; 스크립트는 피어 패키지를 릴리스되는 버전 이외의 버전에 고정하는 타볼을 거부하므로 매니페스트와 함께 `bun.lock` 항목을 범프하세요. 업로드 자체는 npm CLI를 사용합니다, 신뢰할 수 있는 게시를 지원하는 것은 npm뿐이기 때문입니다. `@gajae-gateway/sdk`와 `@gajae-gateway/cli`는 `@gajae-gateway/protocol`/`@gajae-gateway/sdk`이 먼저 게시되어야 하므로 스크립트가 그 순서로 릴리스합니다. 각 패키지의 `publishConfig`는 범위 지정된 이름에 대한 공개 npm 접근을 설정합니다.

### CI/CD

[`build.yml`](.github/workflows/build.yml)은 모든 푸시 및 풀 요청에서 실행됩니다: Biome, `tsc --noEmit`, `bun test packages/`, 및 냉동 코퍼스 벤치마크 게이트, 각각 macOS arm64, Linux x64, 및 Linux arm64에 걸쳐.

[`release.yml`](.github/workflows/release.yml)은 `v*.*.*` 태그 푸시 (또는 선택적 `npm_tag` 입력으로 수동 `workflow_dispatch` 통해, 비-`latest` dist-tag의 경우) 때 실행됩니다. 이는 먼저 전체 `verify` 매트릭스를 다시 실행하고, 녹색일 때만: 의존성 순서로 npm에 `@gajae-gateway/protocol`, `@gajae-gateway/sdk`, 및 `@gajae-gateway/cli`를 게시 (`bun run release:publish` 통해), 각 플랫폼에 대해 여섯 독립형 바이너리를 빌드하고 보관하고, 첨부된 보관 및 자동 생성 노트와 함께 GitHub 릴리스를 생성합니다. 게시는 토큰이 아닌 npm 신뢰 게시 (OIDC)를 사용합니다: 세 패키지 각각은 저장소 `Yeachan-Heo/gajae-way`와 워크플로우 `release.yml`에 대해 npmjs.com의 GitHub 액션 신뢰할 수 있는 게시자를 가지며, 작업은 `id-token: write`를 유지합니다. 게시는 GitHub 호스팅 러너에서만, `release.yml`에서만 작동합니다; 워크플로우 파일 이름 변경은 세 패키지 모두에서 신뢰할 수 있는 게시자를 업데이트해야 합니다.

## 라이선스

저작권 (C) 2026 Yeachan Heo. [GNU Affero General Public License v3.0](LICENSE) 아래에 라이선스됩니다.
