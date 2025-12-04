# 📉 Financial-Risk-Model (Financial Risk Analysis Model)

## 📌 Overview

This project is a Machine Learning-based financial risk analysis model developed to assess the creditworthiness and financial health of US publicly traded companies. The core objective is to leverage key financial statement data extracted from SEC 10-K/10-Q filings to predict the likelihood of financial distress (bankruptcy) and inform fundamental investment valuation.

## 🚀 Project Goals

1. Develop a Financial Distress Prediction Model: Build a classification model that predicts the short-term probability of a company entering financial distress using derived financial ratios as features.

2. Utilize the Z''-Score as Foundation: Implement and extensively utilize the Altman Z''-Score model's components as the initial screening tool and as key features within the machine learning pipeline to establish a strong basis for bankruptcy risk assessment across diverse sectors.

3. Data-Driven Credit Assessment: Provide a preemptive, objective, and data-driven risk indicator that complements traditional credit rating methodologies.

## 🛠️ Core Component: The Altman Z''-Score ($Z''$)

The Z''-Score is selected as the foundational metric for this model due to its broader applicability across all industrial sectors (non-manufacturing and services included), making it ideal for the diverse universe of US publicly traded companies.

- Purpose: To provide a standardized, fundamental measure of a company's financial stability and default probability.

- Method: Accurately extract the necessary four financial variables ($X_1$ to $X_4$) from 10-K/10-Q filings and calculate the score.

**$$ Z'' = 6.56X_1 + 3.26X_2 + 6.72X_3 + 1.05X_4 $$**

| Variable | Description |
| ----- | ----- |
| $X_1$ | Working Capital / Total Assets (Liquidity) |
| $X_2$ | Retained Earnings / Total Assets (Age of Firm's Earnings) |
| $X_3$ | Earnings Before Interest & Taxes (EBIT) / Total Assets (Profitability) |
| $X_4$ | Book Value of Equity / Total Liabilities (Leverage/Solvency) |

## 💻 Technology Stack

- Language: Python

- Data Acquisition/Processing: pandas, numpy, SEC EDGAR API (or related libraries)

- Machine Learning: scikit-learn, LightGBM / XGBoost (Classification)

- Visualization: matplotlib, seaborn

## 🗺️ Development Roadmap

1. Data Acquisition & Preprocessing: Extract and standardize financial statement data from SEC 10-K/10-Q filings.

2. Feature Engineering:
    - Calculate Z''-Score and its constituent components.
    - Derive supplementary financial health indicators (liquidity, profitability, growth metrics).

3. Model Training & Evaluation: Train classification models using financial distress (e.g., bankruptcy, default) as the target variable. Evaluate performance using metrics like AUC, Accuracy, and F1-Score.

4. Interpretation & Deployment: Analyze model predictions (e.g., Feature Importance) and integrate the model into a practical tool for investment decision-making.

## 🤝 Contributing

Bug reports and feature suggestions are always welcome.
