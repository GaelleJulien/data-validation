import pandas as pd
from typing import Optional, List, Tuple
from rule import Rule   
import numpy as np
class ValidationEngine:
    def __init__(self, df, rules):
        self.df = df
        self.rules = rules
        
        self.raw_results: Optional[list[tuple[int, list]]] = None
        self.results: Optional[pd.DataFrame] = None
        self.summary: Optional[dict] = None
        self.summary_by_rule: Optional[pd.DataFrame] = None

    def run(self):
        raw_results = []

        for index, row in self.df.iterrows():
            row_results = []

            for rule in self.rules:
                if rule.column not in row:
                    continue

                row_results.append(rule.check(row[rule.column]))

            raw_results.append((index, row_results))

        self.raw_results = raw_results


    def build_results_dataframe(self):
        if self.raw_results is None:
            raise ValueError("No raw results available. Run the engine first.")

        rows = []

        for row_index, rule_results in self.raw_results:
            for result in rule_results:
                rows.append({
                    "row_index": row_index,
                    "rule_name": result.rule_name,
                    "value": result.value,
                    "threshold": result.threshold,
                    "passed": result.passed
                })

        df_results = pd.DataFrame(rows)
        df_results["status"] = np.where(df_results["passed"], "PASS", "FAIL")

        self.results = df_results


    def build_summary(self):

        if self.results is None:
            raise ValueError("Results DataFrame not built. Please build it first.")

        results_df = self.results
        total_checks = len(results_df)
        passed_checks = results_df["passed"].sum()
        failed_checks = total_checks - passed_checks
        pass_rate = (passed_checks / total_checks) * 100 if total_checks > 0 else 0 

        self.summary = {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "pass_rate": pass_rate
            }
        
    def build_summary_by_rule(self):
        if self.results is None or not isinstance(self.results, pd.DataFrame):
            raise ValueError("Results DataFrame not built. Please build it first.")

        df_byrule = self.results.groupby('rule_name').agg(
            total_checks=pd.NamedAgg(column='passed', aggfunc='count'),
            passed_checks=pd.NamedAgg(column='passed', aggfunc='sum')
        ).reset_index()
        
        df_byrule['failed_checks'] = df_byrule['total_checks'] - df_byrule['passed_checks']
        df_byrule['pass_rate'] = np.where( 
            df_byrule['total_checks'] > 0,
            (df_byrule['passed_checks'] / df_byrule['total_checks']) * 100,
            0
        )
        self.summary_by_rule = df_byrule
