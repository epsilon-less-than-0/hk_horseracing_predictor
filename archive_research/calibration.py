# calibration.py
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from sklearn.isotonic import IsotonicRegression

def apply_race_softmax(df: pd.DataFrame, logit_col: str = 'xgb_logit', group_col: str = 'race_id') -> pd.DataFrame:
    """
    Transforms XGBRanker ordinal logits into normalized probabilities per race.
    Includes max-subtraction for mathematical stability against overflow.
    """
    logging.info("Applying grouped Softmax transformation...")
    
    # Subtract max logit per race for numerical stability before exponentiation
    df['adjusted_logit'] = df[logit_col] - df.groupby(group_col)[logit_col].transform('max')
    df['exp_score'] = np.exp(df['adjusted_logit'])
    
    # Calculate Softmax probability bounded to 1.0 per race
    df['softmax_prob'] = df['exp_score'] / df.groupby(group_col)['exp_score'].transform('sum')
    
    # Cleanup intermediate columns
    df = df.drop(columns=['adjusted_logit', 'exp_score'])
    
    return df

class WinProbabilityCalibrator:
    def __init__(self):
        # out_of_bounds='clip' handles live edge cases safely
        self.calibrator = IsotonicRegression(out_of_bounds='clip')
        
    def fit(self, validation_df: pd.DataFrame, prob_col: str = 'softmax_prob', target_col: str = 'is_win'):
        """
        Fits the Isotonic calibrator. 
        validation_df MUST be chronologically out-of-sample from XGBRanker training.
        """
        logging.info("Fitting Isotonic Regression Calibrator...")
        X_val = validation_df[prob_col].values
        y_val = validation_df[target_col].values
        
        self.calibrator.fit(X_val, y_val)
        logging.info("Calibrator fitted successfully.")
        
    def transform(self, test_df: pd.DataFrame, prob_col: str = 'softmax_prob') -> pd.DataFrame:
        """
        Applies the fitted calibrator to the live/test dataset.
        """
        X_test = test_df[prob_col].values
        test_df['calibrated_win_prob'] = self.calibrator.predict(X_test)
        return test_df