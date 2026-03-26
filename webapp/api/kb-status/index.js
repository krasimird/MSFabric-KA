/**
 * Azure Function — GET /api/kb-status
 *
 * Returns the last-modified timestamp of gendwh_vectors.jsonl in Blob Storage.
 * Used by the UI to show "Last updated" and to poll after triggering pipeline.
 */

let BlobServiceClient, DefaultAzureCredential;
let moduleLoadError = null;
try {
  ({ BlobServiceClient } = require("@azure/storage-blob"));
  ({ DefaultAzureCredential } = require("@azure/identity"));
} catch (err) {
  moduleLoadError = `Failed to load Azure SDK modules: ${err.message}`;
}

// ── Config ──────────────────────────────────────────────────
const BLOB_ACCOUNT_URL = "https://sainspiritka.blob.core.windows.net";
const CONTAINER = "gendwh-exports";
const VECTORS_BLOB = "latest/gendwh_vectors.jsonl";
const KNOWLEDGE_BLOB = "latest/gendwh_knowledge.jsonl";

// ── Handler ─────────────────────────────────────────────────
module.exports = async function (context, req) {
  if (moduleLoadError) {
    context.res = { status: 500, body: { error: moduleLoadError } };
    return;
  }

  try {
    const cred = new DefaultAzureCredential();
    const blobSvc = new BlobServiceClient(BLOB_ACCOUNT_URL, cred);
    const container = blobSvc.getContainerClient(CONTAINER);

    // Get properties of both blobs in parallel
    const [vectorsProps, knowledgeProps] = await Promise.all([
      container.getBlobClient(VECTORS_BLOB).getProperties(),
      container.getBlobClient(KNOWLEDGE_BLOB).getProperties(),
    ]);

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: {
        vectors: {
          lastModified: vectorsProps.lastModified.toISOString(),
          size: vectorsProps.contentLength,
        },
        knowledge: {
          lastModified: knowledgeProps.lastModified.toISOString(),
          size: knowledgeProps.contentLength,
        },
      },
    };
  } catch (err) {
    context.log("kb-status error:", err.message);
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: { error: `Failed to get KB status: ${err.message}` },
    };
  }
};

