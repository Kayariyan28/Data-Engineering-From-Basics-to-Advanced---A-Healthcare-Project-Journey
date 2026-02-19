#!/usr/bin/env python3
"""
data/generate_admissions.py
============================
Generates realistic sample admissions and lab-results CSVs.
Run this to regenerate the raw source data the pipeline ingests.

Usage:
    python3 data/generate_admissions.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

DEPARTMENTS   = ['Cardiology', 'Neurology', 'Orthopedics', 'Emergency', 'ICU', 'Oncology']
DIAGNOSES     = ['Hypertension', 'Fracture', 'Pneumonia', 'Appendicitis', 'Stroke',
                 'Diabetes', 'Atrial Fibrillation', 'Sepsis', 'COPD', 'Heart Failure']
INSURERS      = ['Aetna', 'BlueCross', 'UnitedHealth', 'Cigna', 'Humana', None]
DOCTORS       = [f'DR{str(i).zfill(4)}' for i in range(1, 51)]
LAB_TESTS     = ['CBC', 'BMP', 'LFT', 'Troponin', 'CRP', 'HbA1c']

# Reference ranges per test marker
LAB_MARKERS = {
    'CBC':      [('WBC', 4.5, 11.0, 'K/uL'), ('RBC', 4.5, 5.9, 'M/uL'),
                 ('Hemoglobin', 13.5, 17.5, 'g/dL'), ('Platelets', 150, 400, 'K/uL')],
    'BMP':      [('Glucose', 70, 100, 'mg/dL'), ('Sodium', 136, 145, 'mEq/L'),
                 ('Potassium', 3.5, 5.0, 'mEq/L'), ('Creatinine', 0.6, 1.2, 'mg/dL')],
    'LFT':      [('ALT', 7, 40, 'U/L'), ('AST', 10, 40, 'U/L'),
                 ('Bilirubin', 0.2, 1.2, 'mg/dL')],
    'Troponin': [('TroponinI', 0.0, 0.04, 'ng/mL')],
    'CRP':      [('CRP', 0.0, 10.0, 'mg/L')],
    'HbA1c':    [('HbA1c', 4.0, 5.6, '%')],
}


def generate_admissions(n=10000):
    records = []
    for i in range(n):
        admission_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
        los = random.randint(1, 45)
        discharge_date = admission_date + timedelta(days=los)
        dept = random.choice(DEPARTMENTS)
        insurer = random.choice(INSURERS)

        # Inject realistic data quality issues (~2%)
        age = random.randint(18, 95)
        if random.random() < 0.006:
            age = random.randint(-5, -1)           # negative age
        if random.random() < 0.010:
            discharge_date = admission_date - timedelta(days=random.randint(1, 5))  # bad LOS

        records.append({
            'patient_id':       f'P{random.randint(10000, 99999)}',
            'admission_id':     f'ADM{i:06d}',
            'admission_date':   admission_date.strftime('%Y-%m-%d'),
            'discharge_date':   discharge_date.strftime('%Y-%m-%d'),
            'department':       dept,
            'diagnosis':        random.choice(DIAGNOSES),
            'age':              age,
            'gender':           random.choice(['M', 'F']),
            'attending_doctor': random.choice(DOCTORS),
            'insurance_id':     f'INS{random.randint(1000, 9999)}' if insurer else None,
            'insurer_name':     insurer,
            'readmission':      random.random() < 0.15,
            'icu_stay':         dept == 'ICU' or random.random() < 0.05,
            'num_procedures':   random.randint(0, 8),
            'total_cost_usd':   round(random.uniform(2000, 55000), 2),
        })

    return pd.DataFrame(records)


def generate_labs(admissions_df, avg_labs_per_admission=1.3):
    """Generate lab results linked to the admissions."""
    records = []
    lab_id_counter = 0

    for _, adm in admissions_df.iterrows():
        # Randomly assign 0–3 lab tests per admission
        n_labs = np.random.poisson(avg_labs_per_admission)
        for _ in range(n_labs):
            test_name = random.choice(LAB_TESTS)
            markers   = LAB_MARKERS[test_name]
            adm_date  = datetime.strptime(adm['admission_date'], '%Y-%m-%d')
            collected_at = adm_date + timedelta(hours=random.randint(1, 48))
            resulted_at  = collected_at + timedelta(hours=random.randint(1, 24))

            for marker, ref_low, ref_high, unit in markers:
                # ~10% of results are abnormal (outside reference range)
                is_critical = random.random() < 0.05
                if random.random() < 0.10:
                    # Abnormal: slightly outside range
                    if random.random() < 0.5:
                        value = round(ref_low - random.uniform(0.5, ref_low * 0.3), 3)
                    else:
                        value = round(ref_high + random.uniform(0.5, ref_high * 0.3), 3)
                else:
                    value = round(random.uniform(ref_low, ref_high), 3)

                # Inject ~5% null result values (data quality issue)
                if random.random() < 0.05:
                    value = None

                records.append({
                    'lab_id':         f'LAB{lab_id_counter:06d}_{test_name}',
                    'patient_id':     adm['patient_id'],
                    'admission_id':   adm['admission_id'],
                    'test_name':      test_name,
                    'marker':         marker,
                    'result_value':   value,
                    'unit':           unit,
                    'reference_low':  ref_low,
                    'reference_high': ref_high,
                    'is_critical':    is_critical,
                    'collected_at':   collected_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'resulted_at':    resulted_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'lab_status':     random.choice(['FINAL', 'FINAL', 'FINAL', 'PRELIMINARY']),
                })
            lab_id_counter += 1

    return pd.DataFrame(records)


if __name__ == '__main__':
    out_dir = os.path.dirname(os.path.abspath(__file__))

    print("Generating admissions...")
    admissions = generate_admissions(10_000)
    admissions.to_csv(os.path.join(out_dir, 'admissions_2024.csv'), index=False)
    print(f"  ✓ {len(admissions):,} admission records → data/admissions_2024.csv")

    print("Generating lab results...")
    labs = generate_labs(admissions)
    labs.to_csv(os.path.join(out_dir, 'lab_results_2024.csv'), index=False)
    print(f"  ✓ {len(labs):,} lab records → data/lab_results_2024.csv")

    print("\nDone. Run the pipeline next:")
    print("  python3 scripts/run_pipeline_local.py")
