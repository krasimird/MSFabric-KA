# 06 — Pipeline Scheduling (GenDWH_KA_Extraction_DP)

## 1. Importing the Pipeline into Fabric

1. Open **Microsoft Fabric** → navigate to the target workspace (`GenDWH_Administration_UWS_D`).
2. Click **+ New** → **Data Pipeline** → name it `GenDWH_KA_Extraction_DP`.
3. In the pipeline editor, switch to **JSON view** (code icon `</>` in the toolbar).
4. Fabric shows a JSON structure with read-only `"name"` and `"objectId"` fields at the top.
   **Important:** Keep the existing `"name"` and `"objectId"` values — only replace the `"properties"` block.
   Copy the `"properties": { ... }` section from `notebooks/GenDWH_KA_Extraction_DP.json` and paste it
   over the existing `"properties": { "activities": [] }` in the editor.
5. Click **Validate** to ensure there are no errors.
6. Click **Save**.

> **Note:** The notebook ID (`8aefd8c4-...`) and workspace ID (`f5ae753e-...`) in the JSON must match
> the actual IDs in your environment. Update them if deploying to Test or Prod.

## 2. Configuring the Schedule

The schedule is configured manually via the Fabric UI (not in the JSON):

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
| **Failure Alerts** | Configure an email notification activity (see §5 below) |

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Pipeline stuck in "Queued" | Fabric capacity paused | Resume the capacity in Azure portal |
| Notebook 400 error | Notebook ID changed after redeploy | Update `notebookId` in pipeline JSON |
| Schedule not firing | Schedule toggled off | Re-enable in Schedule tab |

## 5. Adding Email Notification on Failure (Manual Step)

The `Office365Outlook` / `Office365Email` activity type requires a pre-configured **connection GUID**
that is environment-specific, so it cannot be included in the portable JSON definition.
Add it manually after importing the pipeline:

1. Open the pipeline in **Design view** (canvas).
2. From the **Activities** pane, drag an **Office 365 Outlook — Send an email** activity onto the canvas.
3. Name it `Send_Failure_Email`.
4. Connect it to `Run_Extraction_Notebook` with a **Failure** dependency (red arrow):
   - Hover over the notebook activity → click the **red ✕** connector → drag to the email activity.
5. Configure the email activity **Settings** tab:
   - **Connection:** Select or create an Office 365 Outlook connection (sign in with your org account).
   - **To:** `krasi.donchev@inspirit.bg`
   - **Subject:** `GenDWH KA Extraction FAILED — @{pipeline().Pipeline} — @{utcNow()}`
   - **Body:** (HTML)
     ```
     <h2>⚠ Extraction Pipeline Failed</h2>
     <p><b>Pipeline:</b> @{pipeline().Pipeline}</p>
     <p><b>Run ID:</b> @{pipeline().RunId}</p>
     <p><b>Time:</b> @{utcNow()}</p>
     <p>Check the Fabric Monitor for details.</p>
     ```
   - **Importance:** High
6. Click **Validate** → **Save**.

