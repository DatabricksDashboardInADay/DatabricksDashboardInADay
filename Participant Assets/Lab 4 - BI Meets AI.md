# 🧪 Lab 4 – Exploring Databricks Genie with the Sunny Bay Metric View

## 🎯 Learning Objectives

By the end of this lab, you will be able to:

- Use a Unity Catalog metric view (Sunny Bay Coffee Sales) as the main dataset for a Genie space.  
- Ask natural-language questions in Genie and interpret the generated SQL and visualizations.  
- Evaluate and benchmark the quality of Genie’s answers using sanity checks and reference queries.  

## Introduction

**What Is Databricks Genie?**

Databricks Genie is an AI-powered, natural-language interface that lets business users ask questions about their data in plain English and get back answers as tables, charts, and summaries without writing SQL or building reports first. It sits on top of governed datasets like Unity Catalog metric views, so all insights respect existing security, governance, and business definitions.

With Genie, teams can:
- Go beyond static dashboards and get on‑the‑fly answers to new questions that weren’t pre-modeled in reports.
- Self‑serve insights using conversational prompts, reducing dependency on data engineers and analysts for every new question.
- Rely on consistent metrics and semantics defined in Unity Catalog and the Genie knowledge store, improving accuracy and trust in results. 

## Instructions

Before you start, please verify:
- The **Sunny Bay Coffee Sales metric view** from Lab 2 is created and published in Unity Catalog.  
- A **SQL warehouse** (Pro or serverless) is available and selectable for Genie queries.  

### Create the “Sunny Bay Sales Genie” Space

1. In the Databricks workspace, open **Genie** from the left navigation.  
2. Click **New space**.  

![](./Artifacts/Genie_CreateGenieSpace.png)

3. Under **Data sources** (or equivalent section), **add the Sunny Bay Coffee metric view** that you defined in Lab 2 named `sm_fact_coffee_sales_genie`

![](./Artifacts/Genie_SetDataSource.png)

4. Once the Genie space is created, fill in the basic information under "Configure" -> "Settings" :
   - **Title:** `Sunny Bay Sales Genie`  
   - **Description:**   “Ask questions about Sunny Bay Roastery coffee sales, customers, products, and stores using governed metrics from the Sunny Bay metric view.”  
   
![](./Artifacts/Genie_BasicSettings.png)


5. Select a **Pro or serverless SQL warehouse** to run queries.  
6. Click **Save** to save the changes to the Genie space.  
7. In the Genie space settings:
- Configure knowledge / context text (“Instructions” box):
    - Describe the business context: Sunny Bay Roastery, coffee sales, currencies, time grain, etc. 
    - Clarify metric meanings
    - Specify standard aggregations and filters (e.g., “Prefer last 30 days when no date is specified”).
    - To save you from typing, you can past [this text](../Participant%20Assets/Artifacts/Genie_Instructions.txt).
    
![](./Artifacts/Genie_Instructions.png)

8. You can now ask a first question to Genie and it should give acceptable results. For example, you can type **Show me the profit by month for the year 2023 as a bar chart** in the chat window, which should provide the following output:

![](./Artifacts/Genie_TestQuestion.png)

9. (Optional) By clicking on `Show SQL`, you can review the SQL query that Genie created to select data from your Metric View.


### Optimize the Genie Space for High Quality (Knowledge & Semantics)

1. Let's assume that your Fiscal year doesn't match the Calendar year. It instead stretches from June 1st to May 31st. Add this item to the instructions from the previous section: `* whenever users are asking about dates, use the fiscal year instead of the calendar year. It starts on June 1st and ends on May 31st.`

2. Open a new chat to reflect the updated instructions and ask the same question. Genie should now adjust the selected months to the defined fiscal year.

![](./Artifacts/Genie_FiscalYear.png)

3. Review the changed time selector and optionally the SQL query that was updated equally.



