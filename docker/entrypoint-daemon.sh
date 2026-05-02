#!/bin/bash
# Setup Hermes config from Docker environment variables

HERMES_HOME="${HERMES_HOME:-/root/.hermes}"
CONFIG_FILE="$HERMES_HOME/config.yaml"
ENV_FILE="$HERMES_HOME/.env"

mkdir -p "$HERMES_HOME"

# Create config.yaml with custom provider if OPENAI_BASE_URL is set
if [ -n "$OPENAI_BASE_URL" ]; then
    cat > "$CONFIG_FILE" << EOF
model:
  provider: custom
  base_url: "${OPENAI_BASE_URL}"
  default: ${HERMES_MODEL:-deepseek-chat}
EOF
    echo "Created Hermes config.yaml with custom provider: $OPENAI_BASE_URL"
fi

# Create .env file with API key
if [ -n "$OPENAI_API_KEY" ]; then
    cat > "$ENV_FILE" << EOF
OPENAI_API_KEY=${OPENAI_API_KEY}
OPENAI_BASE_URL=${OPENAI_BASE_URL}
EOF
    echo "Created Hermes .env with API key"
fi

# Execute the main command
exec "$@"