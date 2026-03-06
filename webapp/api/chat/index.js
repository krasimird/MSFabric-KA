/**
 * Azure Function — POST /api/chat
 *
 * Proxies chat requests to the Anthropic Claude API,
 * keeping the API key server-side.
 */

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
const MODEL = "claude-sonnet-4-20250514";
const MAX_TOKENS = 4096;

module.exports = async function (context, req) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    context.res = { status: 500, body: { error: "ANTHROPIC_API_KEY is not configured." } };
    return;
  }

  const { messages, system } = req.body || {};
  if (!messages || !Array.isArray(messages)) {
    context.res = { status: 400, body: { error: "Request body must include a 'messages' array." } };
    return;
  }

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
        max_tokens: MAX_TOKENS,
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

