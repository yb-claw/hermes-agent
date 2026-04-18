---
sidebar_position: 16
title: "Yuanbao"
description: "Connect Hermes Agent to the Yuanbao enterprise messaging platform via WebSocket gateway"
---

# Yuanbao

Connect Hermes to [Yuanbao](https://yuanbao.tencent.com/), Tencent's enterprise messaging platform. The adapter uses a WebSocket gateway for real-time message delivery and supports both direct (C2C) and group conversations.

:::info
Yuanbao is an enterprise messaging platform primarily used within Tencent and enterprise environments. It uses WebSocket for real-time communication, HMAC-based authentication, and supports rich media including images, files, and voice messages.
:::

## Prerequisites

- A Yuanbao account with bot creation permissions
- Yuanbao APP_ID and APP_SECRET (from platform admin)
- Python packages: `websockets` and `httpx`
- For media support: `aiofiles`

Install the required dependencies:

```bash
pip install websockets httpx aiofiles
```

## Setup

### 1. Create a Bot in Yuanbao

1. Log in to the Yuanbao platform admin panel
2. Navigate to Bot Management or Developer Settings
3. Create a new bot application
4. Note the **APP_ID** and **APP_SECRET** provided
5. Record the **WebSocket URL** (e.g., `wss://api.yuanbao.example.com/ws`)
6. Record the **API Domain** (e.g., `https://api.yuanbao.example.com`)

### 2. Run the Setup Wizard

The easiest way to configure Yuanbao is through the interactive setup:

```bash
hermes gateway setup
```

Select **Yuanbao** when prompted. The wizard will:

1. Ask for your APP_ID
2. Ask for your APP_SECRET
3. Ask for the WebSocket URL
4. Ask for the API Domain
5. Verify the connection
6. Save the configuration automatically

### 3. Configure Environment Variables

After initial setup, verify these variables in `~/.hermes/.env`:

```bash
# Required
YUANBAO_APP_ID=your-app-id
YUANBAO_APP_SECRET=your-app-secret
YUANBAO_WS_URL=wss://api.yuanbao.example.com/ws
YUANBAO_API_DOMAIN=https://api.yuanbao.example.com

# Optional: bot account ID (normally obtained from sign-token)
# YUANBAO_BOT_ID=your-bot-id

# Optional: home channel for cron/notifications
YUANBAO_HOME_CHANNEL=@bot_account_id
YUANBAO_HOME_CHANNEL_NAME="Bot Notifications"

# Optional: restrict access
YUANBAO_ALLOWED_USERS=user_account_1,user_account_2
```

### 4. Start the Gateway

```bash
hermes gateway
```

The adapter will connect to the Yuanbao WebSocket gateway, authenticate using HMAC signatures, and begin processing messages.

## Features

- **WebSocket gateway** — real-time bidirectional communication
- **HMAC authentication** — secure request signing with APP_ID/APP_SECRET
- **C2C messaging** — direct user-to-bot conversations
- **Group messaging** — conversations in group chats
- **Media support** — images, files, and voice messages via COS (Cloud Object Storage)
- **Markdown formatting** — messages are automatically chunked for Yuanbao's size limits
- **Message deduplication** — prevents duplicate processing of the same message
- **Heartbeat/keep-alive** — maintains WebSocket connection stability
- **Typing indicators** — shows "typing…" status while the agent processes
- **Automatic reconnection** — handles WebSocket disconnections with exponential backoff
- **Group information queries** — retrieve group details and member lists
- **Reply heartbeat** — tracks response states during long operations

## Configuration Options

### Chat ID Formats

Yuanbao uses different account identifiers depending on conversation type:

| Chat Type | Format | Example |
|-----------|--------|---------|
| Direct message (C2C) | User account | `@user_account` or numeric ID |
| Group message | Group code | `@group_code` or group ID |

### Media Uploads

The Yuanbao adapter automatically handles media uploads via COS (Tencent Cloud Object Storage):

- **Images**: Supports JPEG, PNG, GIF, WebP
- **Files**: Supports all common document types
- **Voice**: Supports WAV, MP3, OGG

Media URLs are automatically validated and downloaded before upload to prevent SSRF attacks.

## Home Channel

Use the `/sethome` command in any Yuanbao chat (DM or group) to designate it as the **home channel**. Scheduled tasks (cron jobs) deliver their results to this channel.

You can also set it manually in `~/.hermes/.env`:

```bash
YUANBAO_HOME_CHANNEL=@user_account_or_group_code
YUANBAO_HOME_CHANNEL_NAME="My Bot Updates"
```

### Example: Set Home Channel

1. Start a conversation with the bot in Yuanbao
2. Send the command: `/sethome`
3. The bot responds: "Home channel set to [chat_name] with ID [chat_id]. Cron jobs will deliver to this location."
4. Future cron jobs and notifications will be sent to this channel

### Example: Cron Job Delivery

Create a cron job:

```bash
/cron "0 9 * * *" Check server status
```

The scheduled output will be delivered to your Yuanbao home channel every day at 9 AM.

## Usage Tips

### Starting a Conversation

Send any message to the bot in Yuanbao:

```
hello
```

The bot responds in the same conversation thread.

### Available Commands

All standard Hermes commands work on Yuanbao:

| Command | Description |
|---------|-------------|
| `/new` | Start a fresh conversation |
| `/model [provider:model]` | Show or change the model |
| `/sethome` | Set this chat as the home channel |
| `/status` | Show session info |
| `/help` | Show available commands |

### Sending Files

To send a file to the bot, mention it:

```
Analyze this document @file.pdf
```

The bot downloads and processes the file.

### Receiving Files

When you ask the bot to create or export a file, it sends the file directly to your Yuanbao chat.

## Troubleshooting

### Bot is online but not responding to messages

**Cause**: Authentication failed during WebSocket handshake.

**Fix**:
1. Verify APP_ID and APP_SECRET are correct
2. Check that the WebSocket URL is accessible
3. Ensure the bot account has proper permissions
4. Review gateway logs: `tail -f ~/.hermes/logs/gateway.log`

### "Connection refused" error

**Cause**: WebSocket URL is unreachable or incorrect.

**Fix**:
1. Verify the WebSocket URL format (should start with `wss://`)
2. Check network connectivity to the Yuanbao API domain
3. Confirm firewall allows WebSocket connections
4. Test URL with: `curl -I https://[YUANBAO_API_DOMAIN]`

### Media uploads fail

**Cause**: COS credentials are invalid or media server is unreachable.

**Fix**:
1. Verify API_DOMAIN is correct
2. Check that media upload permissions are enabled for your bot
3. Ensure the media file is accessible and not corrupted
4. Check COS bucket configuration with platform admin

### Messages not delivered to home channel

**Cause**: Home channel ID format is incorrect or cron job hasn't triggered.

**Fix**:
1. Verify YUANBAO_HOME_CHANNEL is in correct format
2. Test with `/sethome` command to auto-detect correct format
3. Check cron job schedule with `/status`
4. Verify bot has send permissions in the target chat

### Frequent disconnections

**Cause**: WebSocket connection is unstable or network is unreliable.

**Fix**:
1. Check gateway logs for error patterns
2. Increase heartbeat timeout in connection settings
3. Ensure stable network connection to Yuanbao API
4. Consider enabling verbose logging: `HERMES_LOG_LEVEL=debug`

## Advanced Configuration

### Custom Message Chunking

Yuanbao has a maximum message size. Hermes automatically chunks large responses, but you can customize behavior:

```bash
# In ~/.hermes/config.yaml under platforms.yuanbao:
message_chunk_size: 4096  # Adjust text chunk size (bytes)
```

### Connection Timeout Settings

```bash
# In ~/.hermes/.env:
YUANBAO_CONNECT_TIMEOUT=30      # WebSocket connect timeout (seconds)
YUANBAO_HEARTBEAT_INTERVAL=60   # Heartbeat frequency (seconds)
YUANBAO_RECONNECT_MAX_WAIT=300  # Max backoff time (seconds)
```

### Verbose Logging

Enable debug logging to troubleshoot connection issues:

```bash
HERMES_LOG_LEVEL=debug hermes gateway
```

## Integration with Other Features

### Cron Jobs

Schedule tasks that run on Yuanbao:

```
/cron "0 */4 * * *" Report system health
```

Results are delivered to your home channel.

### Background Tasks

Run long operations without blocking the conversation:

```
/background Analyze all files in the archive
```

### Cross-Platform Messages

Send a message from CLI to Yuanbao:

```bash
hermes send-message yuanbao "@group_code" "Hello from CLI"
```

## Related Documentation

- [Messaging Gateway Overview](./index.md)
- [Slash Commands Reference](/docs/reference/slash-commands.md)
- [Cron Jobs](/docs/user-guide/features/cron-jobs.md)
- [Background Tasks](/docs/guides/tips.md#background-tasks)
