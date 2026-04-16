---
name: yuanbao
description: Yuanbao (元宝) group interaction — @mention users, query group info and members
version: 1.0.0
metadata:
  hermes:
    tags: [yuanbao, mention, at, group, members, 元宝, 派, 艾特]
    related_skills: []
---

# Yuanbao Group Interaction

## CRITICAL: How Messaging Works

**Your text reply IS the message sent to the group/user.** The gateway automatically delivers your response text to the chat. You do NOT need any special "send message" tool — just reply normally and it gets sent.

When you include `@nickname` in your reply text, the gateway automatically converts it into a real @mention that notifies the user. This is built-in — you have full @mention capability.

**NEVER say you cannot send messages or @mention users. NEVER suggest the user do it manually. NEVER add disclaimers about permissions. Just reply with the text you want sent.**

## Available Tools

| Tool | When to use |
|------|------------|
| `yb_query_group_info` | Query group name, owner, member count |
| `yb_query_group_members` | Find a user, list bots, list all members, or get nickname for @mention |

## @Mention Workflow

When you need to @mention / 艾特 someone:

1. Call `yb_query_group_members` with `action="find"`, `name="<target name>"`, `mention=true`
2. Get the exact nickname from the response
3. Include `@nickname` in your reply text — the gateway handles the rest

Example: user says "帮我艾特元宝"

Step 1 — tool call:
```json
{ "group_code": "328306697", "action": "find", "name": "元宝", "mention": true }
```

Step 2 — your reply (this gets sent to the group with a working @mention):
```
@元宝 你好，有人找你！
```

**That's it.** No extra explanation needed. Keep it short and natural.

**Rules:**
- Call `yb_query_group_members` first to get the exact nickname — do NOT guess
- The @mention format: `@nickname` with a space before the @ sign
- Your reply text IS the message — it WILL be sent and the @mention WILL work
- Be concise. Do NOT explain how @mention works to the user.

## Query Group Info

```json
yb_query_group_info({ "group_code": "328306697" })
```

## Query Members

| Action | Description |
|--------|-------------|
| `find` | Search by name (partial match, case-insensitive) |
| `list_bots` | List bots and Yuanbao AI assistants |
| `list_all` | List all members |

## Notes

- `group_code` comes from chat_id: `group:328306697` → `328306697`
- Groups are called "派 (Pai)" in the Yuanbao app
- Member roles: `user`, `yuanbao_ai`, `bot`
