import pandas as pd
from edgar import Company, set_identity
from typing import Optional
from tqdm import tqdm  # 진행률 표시줄 라이브러리

set_identity("Min min@example.com")

class HealthCheck:
    def __init__(self, config: dict = None):
        self.config = config or {}

    def get_z_double_prime(self, ticker: str) -> Optional[float]:
        try:
            # 실시간 로그: 어떤 기업을 분석 중인지 출력
            # print(f"    [>] Processing {ticker}...", end="\r") 
            
            company = Company(ticker)
            bs_stmt = company.balance_sheet(periods=1, annual=True)
            is_stmt = company.income_statement(periods=1, annual=True)

            if not bs_stmt or not is_stmt:
                return None

            # 최신 API 규격: 객체를 직접 DataFrame으로 변환
            bs_df = pd.DataFrame(bs_stmt)
            is_df = pd.DataFrame(is_stmt)

            def extract(df, labels):
                for label in labels:
                    if label in df.index:
                        return float(df.loc[label].iloc[0])
                return None

            assets = extract(bs_df, ["Total Assets", "Assets"])
            curr_assets = extract(bs_df, ["Total Current Assets", "Assets, Current"])
            curr_liab = extract(bs_df, ["Total Current Liabilities", "Liabilities, Current"])
            total_liab = extract(bs_df, ["Total Liabilities", "Liabilities"])
            retained_earnings = extract(bs_df, ["Retained Earnings", "Retained earnings", "Accumulated Deficit"])
            equity = extract(bs_df, ["Total Equity", "Stockholders' Equity", "Total stockholders' equity"])
            ebit = extract(is_df, ["Operating Income", "Operating Income (Loss)", "Operating profit"])

            if any(v is None for v in [assets, curr_assets, curr_liab, total_liab, ebit]) or assets == 0:
                return None

            x1 = (curr_assets - curr_liab) / assets
            x2 = (retained_earnings or 0) / assets
            x3 = ebit / assets
            x4 = (equity or 0) / total_liab

            return round(float((6.56 * x1) + (3.26 * x2) + (6.72 * x3) + (1.05 * x4)), 2)

        except Exception as e:
            # 에러가 나도 멈추지 않고 출력 후 다음으로 넘어감
            print(f"\n[!] Error for {ticker}: {str(e)}")
            return None

    def run_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'ticker' not in df.columns:
            print("[X] Error: 'ticker' column missing.")
            return df

        # 리스트로 변환하여 tqdm 진행률 표시줄 적용
        tickers = df['ticker'].tolist()
        results = []

        print(f"[*] Starting Health Check for {len(tickers)} companies...")
        
        # tqdm을 사용하여 실시간 진행 상황 표시
        for ticker in tqdm(tickers, desc="Analyzing Financials", unit="company"):
            score = self.get_z_double_prime(ticker)
            results.append(score)

        df['z_score'] = results
        
        print("\n[*] Analysis completed.")
        return df