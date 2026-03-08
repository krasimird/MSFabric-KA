# 06 — Pipeline Scheduling (GenDWH_KA_Extraction_DP)

## 1. Importing the Pipeline into Fabric

1. Open **Microsoft Fabric** → navigate to the target workspace (`GenDWH_Administration_UWS_D`).
2. Click **+ New** → **Data Pipeline** → name it `GenDWH_KA_Extraction_DP`.
3. In the pipeline editor, switch to **JSON view** (code icon `</>` in the toolbar).
4. Replace the contents with the JSON from `notebooks/GenDWH_KA_Extraction_DP.json`.
5. Click **Validate** to ensure there are no errors.
6. Click **Save**.

> **Note:** The notebook ID (`8aefd8c4-...`) and workspace ID (`f5ae753e-...`) in the JSON must match
> the actual IDs in your environment. Update them if deploying to Test or Prod.

## 2. Configuring the Schedule

The JSON includes a schedule block (Mon–Fri at 06:00 FLE Standard Time / UTC+2).
To activate it in Fabric:

1. Open the saved pipeline.
2. Click **Schedule** in the top toolbar.
3. Verify the settings:
   - **Recurrence:** Weekly
   - **Days:** Monday, Tuesday, Wednesday, Thursday, Friday
   - **Time:** 06:00
   - **Time zone:** (UTC+02:00) FLE Standard Time (Helsinki, Kyiv, Riga, Sofia, Tallinn, Vilnius)
4. Toggle the schedule **On**.
5. Click **Apply**.

## 3. Verifying the Schedule is Active

- In the workspace item list, the pipeline should show a **clock icon** next to its name.
- Click the pipeline → **Schedule** tab → confirm status is **Enabled**.
- Next run time should display the upcoming weekday at 06:00 EET.

## 4. Monitoring Run History

| Method | Steps |
|--------|-------|
| **Pipeline Runs** | Open the pipeline → **View Run History** → see status, duration, errors per run |
| **Monitoring Hub** | Fabric left nav → **Monitor** → filter by pipeline name or date range |
| **Failure Alerts** | The `Send_Failure_Email` activity sends an email to `krasi.donchev@inspirit.bg` on notebook failure |

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Pipeline stuck in "Queued" | Fabric capacity paused | Resume the capacity in Azure portal |
| Notebook 400 error | Notebook ID changed after redeploy | Update `notebookId` in pipeline JSON |
| Email not sent | Office 365 connector not authorized | Re-authorize the O365 connection in pipeline settings |
| Schedule not firing | Schedule toggled off | Re-enable in Schedule tab |

