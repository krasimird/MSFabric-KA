# GenDWH Knowledge Assistant — Имплементационен план

**Версия:** 1.0  
**Дата:** 2026-03-06  
**Автор:** InspirIT AI  
**Статус:** Draft

---

## 1. Обхват

Този документ описва стъпките за имплементация на GenDWH Knowledge Assistant — от текущото състояние (работещ PoC notebook) до production-ready система с оркестрация, RAG Web App и daily schedule.

**Текущо състояние:** Единичен notebook с 10 клетки. Phases 0-4 работят. 125/126 таблици анализирани. Excel документи генерирани. Няма оркестрация, няма JSONL knowledge base, няма Web App.

**Целево състояние:** Library notebook + 6 Phase notebooks + Pipeline оркестрация + JSONL knowledge base + RAG Web App на Azure Static Web Apps. Daily scheduled, self-healing, environment-aware.

---

## 2. Спринтове

Имплементацията е разделена на 5 спринта. Всеки спринт завършва с работещ deliverable който може да се демонстрира.

---

### Sprint 1 — Restructure (3 дни)

**Цел:** Преструктуриране на текущия монолитен notebook в Library + Phase notebooks. Всяка фаза чете от Delta и пише в Delta — zero runtime dependency.

**Задачи:**

| # | Задача | Описание | Effort |
|---|--------|----------|--------|
| 1.1 | Създай `GenDWH_Documentation_LIB` | Извади всички дефиниции (CONFIG, helpers, phase functions) в library notebook. Нито една клетка не изпълнява код. Последният ред: `print("✓ LIB loaded")` | 4h |
| 1.2 | Създай Phase_0_Discovery.ipynb | `%run LIB` + `phase_0_discovery()`. Тествай самостоятелно. | 1h |
| 1.3 | Създай Phase_1_Extraction.ipynb | `%run LIB` + `phase_1_metadata_extraction()`. Тествай — трябва да чете от `doc_workspace_inventory` Delta table, не от Python variable. | 2h |
| 1.4 | Рефактор Phase 1 | Добави extraction за: Warehouse, Notebook, SemanticModel, Report, PaginatedReport, VariableLibrary. Всеки тип пише в отделна Delta table. | 8h |
| 1.5 | Създай Phase_2_AI_Analysis.ipynb | `%run LIB` + `phase_2_ai_analysis()`. Тествай — трябва да чете от `doc_metadata_queries`, да ползва cache. | 1h |
| 1.6 | Fix dim_policy | Увеличи `max_tokens` до 16384. Ръчен retry за dim_policy. Потвърди 126/126. | 1h |
| 1.7 | Тествай kernel restart resilience | Рестартирай kernel между всяка Phase. Потвърди че всяка Phase работи самостоятелно — чете от Delta, не зависи от предишен notebook state. | 2h |

**Definition of Done:**
- 7 notebooks в workspace (LIB + 6 Phase)
- Всички attach-нати към GenDWH_Documentation_LH
- Всяка Phase работи след kernel restart
- Phase 1 извлича metadata от всички item types
- 126/126 таблици анализирани в Phase 2

**Deliverable:** Работещи самостоятелни notebooks, ръчно пускани.

---

### Sprint 2 — Knowledge Build + JSONL (2 дни)

**Цел:** Нова Phase 3 — генерира JSONL knowledge base от Delta tables. Всички chunk types. Self-contained, tagged, summarized.

**Задачи:**

| # | Задача | Описание | Effort |
|---|--------|----------|--------|
| 2.1 | Дефинирай chunk schemas | JSON schema за всеки от 15-те chunk types (table_lineage, field_detail, pipeline_overview, и т.н.). Документирай в LIB като constants. | 3h |
| 2.2 | Имплементирай `phase_3_knowledge_build()` | Чете от всички Delta tables, генерира chunks, пише JSONL. Chunk sizing: split големи таблици на batch-ове ≤2000 tokens. | 6h |
| 2.3 | AI summaries (optional) | За всеки `table_lineage` chunk — генерирай `summary` поле чрез Claude API. Template-based fallback ако AI fail. | 4h |
| 2.4 | Създай Phase_3_Knowledge_Build.ipynb | `%run LIB` + `phase_3_knowledge_build()`. Тествай — потвърди JSONL валидност, chunk count, file size. | 1h |
| 2.5 | Валидация | Парсни JSONL обратно, провери: всеки ред е valid JSON, всеки chunk има `type`, `id`, `summary`. Статистика по chunk type. | 1h |

**Definition of Done:**
- `gendwh_knowledge.jsonl` генериран в OneLake Files
- Всички chunk types присъстват
- Всеки chunk е self-contained и tagged
- Файлът може да се парсне ред по ред

**Deliverable:** JSONL knowledge base файл.

---

### Sprint 3 — Pipeline Orchestration (2 дни)

**Цел:** Fabric Data Factory pipeline който оркестрира Phase notebooks с retry, timeout, alerting и schedule.

**Задачи:**

| # | Задача | Описание | Effort |
|---|--------|----------|--------|
| 3.1 | Създай `GenDWH_Documentation_DP` | Pipeline с 6 Notebook Activities: Phase_0 → Phase_1 → Phase_2 → Phase_3 → Phase_4 → Phase_5. Sequential dependencies. | 3h |
| 3.2 | Настрой retry policies | Phase 0,1,3,4,5: retry 2, interval 30-60s. Phase 2: retry 1, interval 120s (кешът пази прогреса). | 1h |
| 3.3 | Настрой timeouts | Phase 0: 5m, Phase 1: 15m, Phase 2: 45m, Phase 3: 5m, Phase 4: 10m, Phase 5: 5m. | 0.5h |
| 3.4 | Error handling | На всяка Phase failure: email notification (O365 Outlook) + Fail activity. Template за email: phase name, error message, run timestamp. | 2h |
| 3.5 | Schedule | Mon-Fri 06:00 FLE Standard Time (след основния ETL). Validate с ръчен trigger. | 0.5h |
| 3.6 | End-to-end test | Пусни pipeline ръчно. Потвърди: всички 6 Phase минават, Delta tables обновени, JSONL генериран, Excel-и генерирани, версия записана. | 2h |
| 3.7 | Failure test | Симулирай failure на Phase 2 (invalid API key). Потвърди: retry се задейства, email notification пристига, pipeline спира. Поправи ключа, пусни отново — cache пази прогреса. | 1h |

**Definition of Done:**
- Pipeline работи end-to-end при ръчен trigger
- Retry работи per phase
- Email notification при failure
- Schedule е настроен
- Успешен failure + recovery тест

**Deliverable:** Scheduled, self-healing documentation pipeline.

---

### Sprint 4 — RAG Web App (3 дни)

**Цел:** Деплойнат Web App на Azure Static Web Apps. Екипът може да пита въпроси и да получава отговори базирани на JSONL knowledge base.

**Задачи:**

| # | Задача | Описание | Effort |
|---|--------|----------|--------|
| 4.1 | Azure Static Web Apps resource | Създай SWA в Azure Portal. Настрой custom domain (optional). | 1h |
| 4.2 | Frontend — JSONL loading | Промени `index.html` да чете `.jsonl` вместо `.json`. Parse line-by-line. Loading indicator. | 2h |
| 4.3 | Frontend — Improved retrieval | Подобри keyword search: entity extraction, scoring по chunk type (table_lineage > field_detail > schema), dedup. | 3h |
| 4.4 | Backend — Azure Function | Deploy `/api/chat` proxy. Тествай CORS, latency, error handling. | 2h |
| 4.5 | Copy JSONL to app | Download `gendwh_knowledge.jsonl` от OneLake, копирай в app root, redeploy. | 0.5h |
| 4.6 | System prompt tuning | Тествай 10-15 въпроса. Tune system prompt за по-добри отговори. Добави примери за impact analysis. | 3h |
| 4.7 | Quick questions | Настрой бутоните в sidebar-а с реални чести въпроси от екипа. | 0.5h |
| 4.8 | User testing | 2-3 души от екипа тестват за 1 ден. Събери feedback. | 4h |
| 4.9 | Fix issues от feedback | Address top 3-5 issues. | 4h |

**Definition of Done:**
- Web App достъпен на URL
- JSONL заредена, sidebar показва статистики
- Въпросите връщат релевантни отговори
- Quick questions работят
- 2-3 колеги са тествали и feedback-ът е адресиран

**Deliverable:** Production-ready Q&A Web App.

---

### Sprint 5 — Polish & Handover (2 дни)

**Цел:** Документация, мониторинг, JSONL update процес, onboarding на екипа.

**Задачи:**

| # | Задача | Описание | Effort |
|---|--------|----------|--------|
| 5.1 | JSONL update process | Документирай процес: pipeline генерира JSONL → download от OneLake → redeploy в SWA. За v2.1: автоматизирай с Azure DevOps / GitHub Actions. | 2h |
| 5.2 | Мониторинг | Pipeline run history в Fabric. Добави alert за 2+ consecutive failures. | 1h |
| 5.3 | Onboarding guide | Кратък guide за екипа: какво е Web App, как да питат, какви въпроси могат, как да репортват проблем. | 2h |
| 5.4 | README per notebook | Markdown клетка в началото на всеки notebook: какво прави, input/output, dependencies. | 2h |
| 5.5 | Cost monitoring | Настрой Anthropic Usage dashboard. Alert ако monthly cost > $20. | 1h |
| 5.6 | API key migration | Мигрирай Claude API key от hardcoded → Azure Key Vault. Обнови LIB Cell 1. Тествай pipeline. | 2h |
| 5.7 | Security review | Провери: JSONL не съдържа PII. Web App достъпен само от corporate network (optional: Azure AD). | 2h |
| 5.8 | Handover session | 1-часова сесия с екипа: demo, Q&A, какво да правят при проблем. | 2h |

**Definition of Done:**
- API key в Key Vault
- Pipeline с мониторинг и alerting
- Екипът знае как да ползва Web App-а
- JSONL update процес документиран
- README в всеки notebook

**Deliverable:** Production system, handed over.

---

## 3. Timeline

```
Week 1:  Sprint 1 (Restructure)           ███████░░░░░  3 дни
         Sprint 2 (Knowledge Build)        ░░░░░░░████░  2 дни

Week 2:  Sprint 3 (Pipeline)              ████░░░░░░░░  2 дни
         Sprint 4 (Web App)               ░░░░██████░░  3 дни

Week 3:  Sprint 5 (Polish & Handover)     ████░░░░░░░░  2 дни
         Buffer                           ░░░░██░░░░░░  1 ден
```

**Общо: 12 работни дни (~2.5 седмици)**

---

## 4. Рискове и митигации

| Риск | Вероятност | Impact | Митигация |
|------|-----------|--------|-----------|
| Fabric REST API не връща definition за някои item types | Средна | Sprint 1 се забавя | Graceful skip — документирай какво не може да се извлече, продължи с останалото |
| SemanticModel extraction изисква допълнителни permissions | Средна | Sprint 1 | Тествай рано (ден 1). Ако не работи — маркирай като v1.1 и продължи |
| Claude API rate limits при AI summaries в Phase 3 | Ниска | Sprint 2 | Template-based fallback за summaries. AI summaries могат да дойдат async |
| Azure Static Web Apps deployment issues | Ниска | Sprint 4 | Fallback: хостване на IIS / като Fabric Notebook output |
| JSONL е твърде голям за browser (>10 MB) | Ниска | Sprint 4 | Lazy loading, split по chunk type, или server-side search |
| Екипът не ползва Web App-а | Средна | Sprint 5 | Onboarding session, Quick Questions за най-честите сценарии, feedback loop |

---

## 5. Dependencies

| Dependency | Нужен за | Кой осигурява | Статус |
|-----------|----------|--------------|--------|
| GenDWH_Documentation_LH | Всички спринтове | Вече създаден | ✅ |
| Anthropic API key | Sprint 1-4 | Наличен | ✅ |
| Fabric workspace access | Sprint 1 | Наличен | ✅ |
| Azure subscription (за SWA) | Sprint 4 | DevOps / Infra екип | ⬜ Потвърди |
| Azure Key Vault (за prod API key) | Sprint 5 | DevOps / Infra екип | ⬜ Потвърди |
| Corporate network access за Web App | Sprint 4 | Security / Network | ⬜ Потвърди |
| 2-3 колеги за user testing | Sprint 4 | Екипът | ⬜ Планирай |

---

## 6. Definition of Done — цял проект

- [ ] Library notebook (`GenDWH_Documentation_LIB`) с всички дефиниции
- [ ] 6 Phase notebooks, всеки self-contained
- [ ] Pipeline `GenDWH_Documentation_DP` с retry, alerting, schedule
- [ ] 126/126 таблици анализирани (включително dim_policy)
- [ ] JSONL knowledge base с всички chunk types
- [ ] Data Lineage Excel с 354 sheets и навигация
- [ ] Data Dictionary Excel с 409+ sheets
- [ ] RAG Web App на Azure Static Web Apps
- [ ] API key в Azure Key Vault
- [ ] Onboarding guide за екипа
- [ ] Handover session проведена
- [ ] Pipeline минава daily за 1+ седмица без intervention

---

## 7. Effort Summary

| Sprint | Дни | Часове (approx) |
|--------|-----|----------------|
| Sprint 1 — Restructure | 3 | 19h |
| Sprint 2 — Knowledge Build | 2 | 15h |
| Sprint 3 — Pipeline | 2 | 10h |
| Sprint 4 — Web App | 3 | 20h |
| Sprint 5 — Polish & Handover | 2 | 14h |
| **Total** | **12** | **~78h** |

---

## 8. Какво имаме vs какво трябва

| Компонент | Текущо състояние | Целево (v1.0) | Gap |
|-----------|-----------------|---------------|-----|
| Notebook structure | 1 монолитен NB, 10 cells | LIB + 6 Phase NBs | Sprint 1 |
| Item extraction | LH, Pipeline, Bronze | + WH, NB, SM, Report, VL | Sprint 1.4 |
| AI Lineage | 125/126 таблици | 126/126 | Sprint 1.6 |
| Knowledge base | Flat JSON export | Structured JSONL | Sprint 2 |
| Orchestration | Ръчно пускане | Pipeline с retry + schedule | Sprint 3 |
| Excel docs | Работещи | Без промяна (четат от Delta) | — |
| Web App | Не съществува | RAG на Azure SWA | Sprint 4 |
| API key | Hardcoded | Azure Key Vault | Sprint 5 |
| Monitoring | Няма | Pipeline alerts + cost monitoring | Sprint 5 |
| Documentation | Project log (md) | README per NB + onboarding guide | Sprint 5 |

---

*Край на имплементационния план. Следва изпълнение.*
