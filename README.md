# 📉 Financial-Risk-Model (Financial Risk Analysis Model)

## 📌 Overview

This project is a Machine Learning-based financial risk analysis model developed to assess the creditworthiness and financial health of US publicly traded companies. The core objective is to leverage key financial statement data extracted from SEC 10-K/10-Q filings to predict the likelihood of financial distress (bankruptcy) and inform fundamental investment valuation.The initial development phase focuses on NASDAQ 100 (NDX) companies to stabilize the accounting classification logic, with plans to expand the scope to S&P 500 (SP500) companies to analyze risk across diverse industrial sectors.

## 🚀 Project Goals

1. Develop a Financial Distress Prediction Model: Build a classification model that predicts the short-term probability of a company entering financial distress using derived financial ratios as features.
2. Utilize the Z''-Score as Foundation: Implement and utilize the Altman Z''-Score model's components as the initial screening tool and as key features within the machine learning pipeline to establish a strong basis for bankruptcy risk assessment.
3. Data-Driven Credit Assessment: Provide a preemptive, objective, and data-driven risk indicator that complements traditional credit rating methodologies.

## 🛠️ Core Component: The Altman Z''-Score (Z'')

The Z''-Score is selected as the foundational metric for this model due to its broader applicability across all industrial sectors (non-manufacturing and services included), making it ideal for the diverse universe of US publicly traded companies.

$$ Z'' = 6.56X_1 + 3.26X_2 + 6.72X_3 + 1.05X_4$$

VariableDescription$X_1$Working Capital / Total Assets (Liquidity)$X_2$Retained Earnings / Total Assets (Age of Firm's Earnings)$X_3$Earnings Before Interest & Taxes (EBIT) / Total Assets (Profitability)$X_4$Book Value of Equity / Total Liabilities (Leverage/Solvency)

## 💻 Technology Stack

AreaTools & LibrariesPurposeLanguagePythonPrimary development languageData Processingpandas, numpyData acquisition and financial ratio calculationDatabasePostgreSQLPersistent storage and management of calculated resultsDB Connectorpsycopg2Secure, type-safe data insertion into PostgreSQLMachine Learningscikit-learn, LightGBM / XGBoostClassification model training and evaluation for financial distressData AcquisitionSEC EDGAR API (or related libraries)Extracting financial statement data from 10-K/10-Q filingsVisualizationmatplotlib, seabornResult analysis and visualization

## 🗺️ Development Roadmap

1. Data Acquisition & Preprocessing (Phase 1 - NDX Focus):

   - Extract and standardize financial data from SEC 10-K/10-Q filings for NDX companies.
   - Stabilize Accounting Classification Logic: Establish robust mapping logic to accurately extract $X_1$ to $X_4$ variables.
   - Implement Type Safety: Ensure type safety logic for PostgreSQL insertion, particularly for handling large integers and precise float values.

2. Dataset Expansion (Phase 2 - SP500 Expansion):

   - Expand the stabilized logic from NDX to S&P 500 companies to increase dataset size and industry diversity.

3. Feature Engineering:

   - Calculate the Z''-Score and its constituent components.
   - Derive supplementary financial health indicators (liquidity, profitability, growth metrics).

4. Model Training & Evaluation:

   - Train classification models using financial distress (e.g., bankruptcy, default) as the target variable.
   - Evaluate performance using metrics like AUC, Accuracy, and F1-Score.

5. Interpretation & Deployment:

   - Analyze model predictions (e.g., Feature Importance).

   - Integrate the model into a practical tool for investment decision-making.

## 🤝 Contributing

Bug reports and feature suggestions are always welcome.