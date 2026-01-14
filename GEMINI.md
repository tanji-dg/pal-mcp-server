# PAL MCP Server - Development & Handover Guide

このドキュメントは、PAL MCP Serverの設計思想、カスタマイズ内容、および現在進行中の「リアルタイムJSON（ストリーミング）対応」について、別のLLMが開発を引き継ぐための詳細なガイドです。

---

## 1. アーキテクチャ概要 (PAL as a Bridge)

PALは、MCP (Model Context Protocol) サーバーとして動作し、主に **"clink" ツール** を通じて外部のAI CLIエージェント（Gemini CLI, Codex CLI等）をラップします。

- **Bridge Mode**: PAL自体にAPIキーが設定されていない場合でも、外部CLI（すでに認証済み）を呼び出すことでAI機能を利用可能にします。
- **DISABLED_TOOLS**: APIキー不足によるエラーを避けるため、PAL自体のAIツール（`chat`, `codereview`等）は `DISABLED_TOOLS` 環境変数によって無効化されています。これにより、外部CLIがPALのツールを逆呼び出し（CallTool）した際の認証エラーを防止しています。

---

## 2. 重要な修正と不具合対策 (既知のノウハウ)

### タイムゾーンと環境変数
- **TZ=Asia/Tokyo**: ログのタイムスタンプを日本時間にするため、`server.py` の冒頭（環境変数読み込み前）で `time.tzset()` を呼び出しています。
- **Environment Fallback**: `utils/env.py` は、`.env` ファイルに値がない場合にシステム環境変数を参照するように修正されています。

### Gemini CLI のループ対策 (`--exit-on-error`)
- **問題**: Gemini CLI内部の `LoopDetectionService` が失敗すると、無限に "Aborted()" ログが出力され、PALのログが肥大化する。
- **対策**: Gemini CLI側に `--exit-on-error` フラグを実装。致命的なエラー時に `process.exit(1)` するようソースコードを修正済み。

---

## 3. Gemini CLI の統合構造

### サブモジュール管理
- **ディレクトリ**: `gemini-cli/`
- **ブランチ**: `feat/enhance-cli-integration` (独自修正を含むローカルブランチ)
- **ビルド**: 修正を反映させるには、ディレクトリ内で `npm run build` が必須。

### ラッパースクリプト (`gemini-cli/gemini-built.sh`)
- **役割**: PALから呼び出される際のエントリポイント。
- **引数の優先順位**: `yargs` の仕様により、位置引数（プロンプト）の前にフラグ（`--model` 等）を置く必要がある。このスクリプトは引数を解析し、特定のフラグを先頭に移動させてから `node` を実行する。

---

## 4. リアルタイム JSON 対応 (進行中のタスク)

### 目標
Gemini CLIの `-o stream-json` モードをサポートし、生成中の思考プロセスやツール呼び出しをリアルタイムでユーザーに通知する。

### 実装の急所
1.  **`tools/clink.py` の `_notification_callback`**:
    - `stdout` から流れてくる JSON チャンクをパースする。
    - `{"type":"message", "content":"...", "delta":true}` から思考プロセスを抽出。
    - `{"type":"tool_call", ...}` から実行中のツール名を抽出。
2.  **`clink/parsers/gemini.py` (GeminiJSONParser)**:
    - 実行完了後、`stdout` には複数の JSON オブジェクトが蓄積されている。
    - `line.split("\n")` で分割し、最後に出現する `type: "result"` のオブジェクトを最終回答として採用するロジックが必要。
3.  **設定更新**:
    - `conf/cli_clients/gemini.json` の `additional_args` に `-o stream-json`を追加する。

---

## 5. 開発環境とテスト

### サーバーの起動
```bash
./run-server.sh
```

### ログの監視 (リアルタイム出力の確認)
```bash
tail -f logs/mcp_server_*.log | grep "CLI RAW"
```

### Gemini CLI の直接テスト
```bash
echo "Hello" | ./gemini-cli/gemini-built.sh -o stream-json --yolo
```

---

## 6. 次のステップ (ToDo)
- [ ] `feat/clink-stream-json` ブランチでの実装を完了させる。
- [ ] 複数の JSON オブジェクトが混在する `stdout` のパースが、既存のパーサーを壊さないか確認する。
- [ ] `Codex` など他の CLI における JSON イベント形式との整合性を保つ。
