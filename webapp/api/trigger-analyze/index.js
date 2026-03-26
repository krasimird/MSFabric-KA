/**
 * Azure Function — POST /api/trigger-analyze
 *
 * Triggers the KA-Analyze-Embed pipeline in Azure DevOps.
 * Returns immediately with pipeline run info.
 *
 * Reads DevOps PAT from env var or Key Vault secret "devops-pat".
 */

let DefaultAzureCredential, SecretClient;
let moduleLoadError = null;
try {
  ({ DefaultAzureCredential } = require("@azure/identity"));
  ({ SecretClient } = require("@azure/keyvault-secrets"));
} catch (err) {
  moduleLoadError = `Failed to load Azure SDK modules: ${err.message}`;
}
const https = require("https");

// ── Config ──────────────────────────────────────────────────
const KV_URL = "https://kv-ai-site-builder.vault.azure.net";
const DEVOPS_ORG = "INSPIRITBG";
const DEVOPS_PROJECT = "Fabric Pipelines";
const PIPELINE_ID = 54; // KA-Analyze-Embed
const DEVOPS_API_VERSION = "7.1";

// ── Cached PAT ──────────────────────────────────────────────
let cachedPat = null;

async function getDevOpsPat(log) {
  if (cachedPat) return cachedPat;
  if (process.env.DEVOPS_PAT) {
    cachedPat = process.env.DEVOPS_PAT;
    return cachedPat;
  }
  try {
    const cred = new DefaultAzureCredential();
    const kv = new SecretClient(KV_URL, cred);
    cachedPat = (await kv.getSecret("devops-pat")).value;
    log("DevOps PAT loaded from Key Vault.");
    return cachedPat;
  } catch (err) {
    log("KV fetch failed: " + err.message);
    return null;
  }
}

// ── Queue a pipeline run via DevOps REST API ────────────────
function queuePipeline(pat, log) {
  return new Promise((resolve, reject) => {
    const project = encodeURIComponent(DEVOPS_PROJECT);
    const path = `/${DEVOPS_ORG}/${project}/_apis/build/builds?api-version=${DEVOPS_API_VERSION}`;
    const body = JSON.stringify({
      definition: { id: PIPELINE_ID },
      sourceBranch: "refs/heads/main",
    });
    const auth = Buffer.from(`:${pat}`).toString("base64");
    const options = {
      hostname: "dev.azure.com",
      port: 443,
      path,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Basic ${auth}`,
        "Content-Length": Buffer.byteLength(body),
      },
    };
    log(`POST https://dev.azure.com${path}`);
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve({ id: json.id, status: json.status, url: json._links?.web?.href });
          } else {
            reject(new Error(json.message || `HTTP ${res.statusCode}`));
          }
        } catch (e) {
          reject(new Error(`Parse error: ${data.slice(0, 200)}`));
        }
      });
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

// ── Handler ─────────────────────────────────────────────────
module.exports = async function (context, req) {
  const log = (...args) => context.log(...args);

  if (moduleLoadError) {
    context.res = { status: 500, body: { error: moduleLoadError } };
    return;
  }

  try {
    const pat = await getDevOpsPat(log);
    if (!pat) {
      context.res = { status: 500, body: { error: "DevOps PAT not configured" } };
      return;
    }

    const result = await queuePipeline(pat, log);
    log(`Pipeline queued: build #${result.id}, status=${result.status}`);

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: {
        success: true,
        message: "Pipeline triggered successfully",
        buildId: result.id,
        status: result.status,
        url: result.url,
      },
    };
  } catch (err) {
    log("Trigger failed:", err.message);
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: { error: `Failed to trigger pipeline: ${err.message}` },
    };
  }
};

