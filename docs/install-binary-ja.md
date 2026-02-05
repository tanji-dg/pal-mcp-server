# バイナリパッケージ（Wheel）からのインストール手順

このガイドでは、ビルド済みのバイナリパッケージ（`.whl` ファイル）を使用して PAL MCP Server をインストールおよび設定する方法を説明します。この方法は、オフライン環境でのインストールや、特定のバージョンを管理された環境に配布する場合に便利です。

## 前提条件

- **Python 3.9以上**（3.12推奨）
- **pip**（Pythonパッケージインストーラー）

## ステップ 1: パッケージのインストール

`dist/` ディレクトリにある `.whl` ファイルを特定し、pip を使用してインストールします。仮想環境（venv）の使用を強く推奨します。

```bash
# 任意: 仮想環境の作成と有効化
python -m venv .pal_venv
source .pal_venv/bin/activate  # Windowsの場合: .pal_venv\Scripts\activate

# Wheelファイルのインストール
pip install dist/pal_mcp_server-9.8.2-py3-none-any.whl
```

## ステップ 2: 環境変数の設定

サーバーの動作にはAPIキーが必要です。これらは環境変数として設定するか、作業ディレクトリに `.env` ファイルを作成して記述します。

### 必須のAPIキー
以下のうち、少なくとも1つを設定してください：
- `GEMINI_API_KEY`
- `OPENAI_API_KEY`
- `OPENROUTER_API_KEY`

### 任意の設定
- `DEFAULT_MODEL`: デフォルトモデルの設定（例: `flash`, `pro`, `auto`）
- `LOG_LEVEL`: ログの出力レベル（例: `INFO`, `DEBUG`）

## ステップ 3: MCPクライアントとの統合

インストールが完了すると、`pal-mcp-server` コマンドが利用可能になります。MCPクライアントの設定では、この実行ファイルの絶対パスを指定します。

### 実行ファイルのパスを確認する
```bash
which pal-mcp-server
```

### Claude Desktop の設定例
`claude_desktop_config.json` に以下を追加します：

```json
{
  "mcpServers": {
    "pal": {
      "command": "/path/to/your/venv/bin/pal-mcp-server",
      "env": {
        "GEMINI_API_KEY": "あなたのAPIキー"
      }
    }
  }
}
```

### Gemini CLI の設定例
`~/.gemini/settings.json` に以下を追加します：

```json
{
  "mcpServers": {
    "pal": {
      "command": "/path/to/your/venv/bin/pal-mcp-server",
      "env": {
        "GEMINI_API_KEY": "あなたのAPIキー"
      }
    }
  }
}
```

## ステップ 4: 動作確認

ターミナルで以下のコマンドを実行して、インストールが正しく行われたか確認します：

```bash
pal-mcp-server --help
```

MCP対応のチャットクライアントで以下のように入力してみてください：
「pal を使って利用可能なモデルをリストして」
