# Installing PAL MCP Server from Binary (Wheel)

This guide explains how to install and configure the PAL MCP Server using the pre-built binary package (`.whl`). This is useful for offline installations, redistributing the server, or installing a specific version in a controlled environment.

## Prerequisites

- **Python 3.9+** (3.12 recommended)
- **pip** (Python package installer)

## Step 1: Install the Package

Locate the `.whl` file in the `dist/` directory and install it using pip. It's recommended to use a virtual environment.

```bash
# Optional: Create and activate a virtual environment
python -m venv .pal_venv
source .pal_venv/bin/activate  # On Windows: .pal_venv\Scripts\activate

# Install the wheel file
pip install dist/pal_mcp_server-9.8.2-py3-none-any.whl
```

## Step 2: Configure Environment Variables

The server requires API keys to function. You can set these as environment variables or create a `.env` file in your working directory.

### Essential API Keys
Set at least one of the following:
- `GEMINI_API_KEY`
- `OPENAI_API_KEY`
- `OPENROUTER_API_KEY`

### Optional Configuration
- `DEFAULT_MODEL`: Set a default model (e.g., `flash`, `pro`, `auto`)
- `LOG_LEVEL`: Set logging verbosity (e.g., `INFO`, `DEBUG`)

## Step 3: Integration with MCP Clients

Once installed, the `pal-mcp-server` command becomes available. Use the absolute path to the executable in your MCP client configuration.

### Finding the Executable Path
```bash
which pal-mcp-server
```

### For Claude Desktop
Add this to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "pal": {
      "command": "/path/to/your/venv/bin/pal-mcp-server",
      "env": {
        "GEMINI_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

### For Gemini CLI
Add this to `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "pal": {
      "command": "/path/to/your/venv/bin/pal-mcp-server",
      "env": {
        "GEMINI_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

## Step 4: Verification

Test the installation by running the following command in your terminal:

```bash
pal-mcp-server --help
```

In your MCP-enabled chat client, try:
`"Use pal to list available models"`

## Option B: Run via Docker (Offline Image)

For environments where you prefer Docker or cannot use Python directly, you can import the pre-built minimal image.

### 1. Import the Image
```bash
# Load the image (approx. 180MB)
docker load < pal-mcp-server-minimal.tar.gz
```

### 2. Run with Docker Compose
Use the provided `docker-compose.prod.yml` to start the server. This configuration:
- Persists data to your home directory (`~/.pal/`).
- Installs necessary AI tools (Claude Code, Gemini CLI, etc.) on first run.
- Starts the monitoring dashboard on port 9876.

```bash
# Create configuration file
cp .env.example .env
# Edit .env with your API keys (GEMINI_API_KEY, etc.)

# Start the server
docker-compose -f docker-compose.prod.yml up -d
```

### 3. Verification
Access the monitoring dashboard at: `http://localhost:9876/dashboard`
