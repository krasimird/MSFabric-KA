/**
 * Azure Function — POST /api/chat
 *
 * Proxies chat requests to the Anthropic Claude API,
 * keeping the API key server-side.
 *
 * API key resolution order:
 * 1. ANTHROPIC_API_KEY env var (for local dev via local.settings.json)
 * 2. Azure Key Vault: kv-ai-site-builder / anthropicapikey
 */

const { DefaultAzureCredential } = require("@azure/identity");
const { SecretClient } = require("@azure/keyvault-secrets");

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
const MODEL = "claude-sonnet-4-20250514";
const DEFAULT_MAX_TOKENS = 4096;
const MAX_ALLOWED_TOKENS = 16384;

const KV_URL = "https://kv-ai-site-builder.vault.azure.net";
const KV_SECRET_NAME = "anthropicapikey";

// Cache the key so we only fetch from Key Vault once per function instance
let cachedApiKey = null;

async function getApiKey(context) {
  if (cachedApiKey) return cachedApiKey;

  // 1. Check env var first (local dev)
  if (process.env.ANTHROPIC_API_KEY) {
    cachedApiKey = process.env.ANTHROPIC_API_KEY;
    return cachedApiKey;
  }

  // 2. Fetch from Azure Key Vault
  try {
    const credential = new DefaultAzureCredential();
    const client = new SecretClient(KV_URL, credential);
    const secret = await client.getSecret(KV_SECRET_NAME);
    cachedApiKey = secret.value;
    context.log.info("Anthropic API key loaded from Key Vault.");
    return cachedApiKey;
  } catch (err) {
    context.log.error("Failed to fetch API key from Key Vault:", err.message);
    return null;
  }
}

module.exports = async function (context, req) {
  const apiKey = await getApiKey(context);
  if (!apiKey) {
    context.res = { status: 500, body: { error: "ANTHROPIC_API_KEY is not configured and Key Vault is unreachable." } };
    return;
  }

  const { messages, system, max_tokens } = req.body || {};
  if (!messages || !Array.isArray(messages)) {
    context.res = { status: 400, body: { error: "Request body must include a 'messages' array." } };
    return;
  }

  // Allow client to request more tokens (capped at MAX_ALLOWED_TOKENS)
  const tokensToUse = Math.min(
    Math.max(parseInt(max_tokens, 10) || DEFAULT_MAX_TOKENS, 256),
    MAX_ALLOWED_TOKENS
  );

  try {
    const response = await fetch(ANTHROPIC_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: tokensToUse,
        ...(system ? { system } : {}),
        messages,
      }),
    });

    const data = await response.json();

    context.res = {
      status: response.ok ? 200 : response.status,
      headers: { "Content-Type": "application/json" },
      body: data,
    };
  } catch (err) {
    context.log.error("Claude API call failed:", err);
    context.res = { status: 502, body: { error: "Failed to reach Claude API." } };
  }
};

