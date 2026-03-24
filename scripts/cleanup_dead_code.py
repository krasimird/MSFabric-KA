"""
One-shot cleanup of dead client-side RAG code from index.html.
Removes: retrieveContext, SYSTEM_PROMPT, followLineageChain, formatChainContext,
         getPinnedChunks, getPinnedChunkPriority, expandFollowUpQuery,
         buildPipelineIndex, PIPELINE_INDEX, sticky context (state + UI + CSS),
         _buildEntityIndex, detectAndPinEntities, renderStickyBar, getStickyContextText.
Rewrites sendMessage to use server-side RAG endpoint.
Updates generateCustomReportExcel to stop calling retrieveContext.
"""
import re, sys, pathlib

p = pathlib.Path('webapp/src/index.html')
text = p.read_text(encoding='utf-8')

# 1. Remove sticky context CSS block (lines 191-214 area)
text = re.sub(
    r'/\* ── Sticky Context Bar ──[^*]*?\*/\n'
    r'.*?\.sticky-context-clear:hover \{[^\n]*\}\n',
    '', text, count=1, flags=re.DOTALL
)

# 2. Remove sticky context bar HTML element
text = text.replace('    <div class="sticky-context-bar" id="stickyContextBar"></div>\n', '')

# 3. Remove stickyBar const
text = re.sub(r"const stickyBar\s*=\s*document\.getElementById\('stickyContextBar'\);\n", '', text)

# 4. Remove PIPELINE_INDEX declaration
text = re.sub(r"let PIPELINE_INDEX = \{\};\s*//[^\n]*\n", '', text)

# 5. Remove Follow-up Query Expansion block (state + function)
text = re.sub(
    r"// ── Follow-up Query Expansion ─+\n"
    r"let _lastUserQuery[^\n]*\n"
    r"\n"
    r"// Stop-words that carry no topical meaning.*?"
    r"function expandFollowUpQuery\(currentQuery, previousQuery\) \{.*?\n\}\n",
    '', text, count=1, flags=re.DOTALL
)

# 6. Remove Sticky Context state
text = re.sub(
    r"// ── Sticky Context ─+\n"
    r"const MAX_STICKY = \d+;\n"
    r"let stickyEntities = \[\];[^\n]*\n",
    '', text, count=1
)

# 7. Remove buildPipelineIndex function + call sites
text = text.replace('        buildPipelineIndex();\n', '')
text = text.replace('    buildPipelineIndex();\n', '')
text = re.sub(
    r"// ── Pipeline index: map pipeline/activity names.*?"
    r"function buildPipelineIndex\(\) \{.*?\n\}\n",
    '', text, count=1, flags=re.DOTALL
)

# 8. Remove followLineageChain function (large)
text = re.sub(
    r"// ── Chain-aware lineage following ─+\n"
    r"function followLineageChain\(fieldName\) \{.*?\n\}\n",
    '', text, count=1, flags=re.DOTALL
)

# 9. Remove formatChainContext function
text = re.sub(
    r"\nfunction formatChainContext\(chain, fieldName\) \{.*?\n\}\n",
    '\n', text, count=1, flags=re.DOTALL
)

# 10. Remove getPinnedChunkPriority function
text = re.sub(
    r"\nfunction getPinnedChunkPriority\(chunk, queryLower\) \{.*?\n\}\n",
    '\n', text, count=1, flags=re.DOTALL
)

# 11. Remove getPinnedChunks function
text = re.sub(
    r"// ── Entity Pinning:.*?\n"
    r"function getPinnedChunks\(query, scoredChunks.*?\{.*?\n\}\n",
    '', text, count=1, flags=re.DOTALL
)

# 12. Remove retrieveContext function + SYSTEM_PROMPT (big block, up to Excel helpers)
text = re.sub(
    r"// ── RAG retrieval \(keyword \+ knowledge base\) ─+\n"
    r"async function retrieveContext\(query\) \{.*?"
    r"- Be concise but thorough`;\n",
    '', text, count=1, flags=re.DOTALL
)

# 13. Remove _buildEntityIndex, detectAndPinEntities, renderStickyBar, getStickyContextText
text = re.sub(
    r"// ── Sticky Context: entity detection & management ─+\n"
    r"function _buildEntityIndex\(\) \{.*?"
    r"function getStickyContextText\(\) \{.*?\n\}\n",
    '', text, count=1, flags=re.DOTALL
)

# 14. Rewrite sendMessage: replace old RAG call with server-side RAG
old_send = (
    "  // RAG context + sticky context (store query for follow-up expansion)\n"
    "  const context = await retrieveContext(text);\n"
    "  _lastUserQuery = text;  // save AFTER retrieval so current Q uses previous Q for expansion\n"
    "  const stickyCtx = getStickyContextText();\n"
    "  let systemWithContext = SYSTEM_PROMPT;\n"
    "  if (window._lastChainQuery) {\n"
    "    systemWithContext += '\\n\\nIMPORTANT: The context contains a multi-layer lineage chain"
)
# Find and replace the full sendMessage body from RAG context to the end of the function
text = re.sub(
    r"  // RAG context \+ sticky context.*?"
    r"    sendBtn\.disabled = false;\n"
    r"    userInput\.focus\(\);\n"
    r"  \}\n\}",
    """  try {
    sendBtn.disabled = true;
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: text, chatHistory })
    });
    const data = await resp.json();
    spinner.remove();

    if (data.error) {
      appendMessage('assistant', '⚠️ Error: ' + (data.error.message || data.error));
    } else {
      const reply = data.content ? data.content.map(c => c.text).join('') : 'No response';
      let usageInfo = null;
      if (data.usage) {
        const inp = data.usage.input_tokens || 0;
        const out = data.usage.output_tokens || 0;
        usageInfo = recordUsage(inp, out);
      }
      appendMessage('assistant', reply, usageInfo);
      chatHistory.push({ role: 'assistant', content: reply });
    }
  } catch (err) {
    spinner.remove();
    appendMessage('assistant', '⚠️ Failed to reach the API. Is the backend running?');
  } finally {
    sendBtn.disabled = false;
    userInput.focus();
  }
}""",
    text, count=1, flags=re.DOTALL
)

# 15. Remove detectAndPinEntities call in sendMessage
text = text.replace(
    "  // ── Sticky context: detect and pin entities mentioned in this message ──\n"
    "  // Use expanded query so follow-ups inherit entity names from previous question\n"
    "  const expandedForPinning = expandFollowUpQuery(text, _lastUserQuery);\n"
    "  detectAndPinEntities(expandedForPinning);\n\n",
    ''
)

# 16. Update generateCustomReportExcel to not call retrieveContext
text = text.replace(
    "  log('Gathering context from knowledge base...');\n"
    "  const context = analysisReady ? retrieveContext(query) : '';",
    "  log('Gathering context from knowledge base (server-side)...');\n"
    "  const context = ''; // Context now provided server-side via RAG"
)

# Clean up excessive blank lines (3+ → 2)
text = re.sub(r'\n{4,}', '\n\n\n', text)

p.write_text(text, encoding='utf-8')
new_lines = text.count('\n')
print(f'Done. New line count: {new_lines + 1}')

