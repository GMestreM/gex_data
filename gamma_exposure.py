"""
Calculate gamma exposure

Gamma Exposure = Unit Gamma * Open Interest * Spot Price

To further convert into 'per 1% move' quantity, multiply by 1% of Spot Price
"""

import pandas as pd 
import numpy as np 
from scipy.stats import norm

# Get gamma exposure for Black-Scholes European-Options
def calc_gamma(S, K, vol, T, r, q, option_type):
    
    if T == 0 or vol == 0:
        return 0

    dp = (np.log(S/K) + (r - q + 0.5*vol**2)*T) / (vol*np.sqrt(T))
    dm = dp - vol*np.sqrt(T) 

    if option_type == 'call':
        gamma = np.exp(-q*T) * norm.pdf(dp) / (S * vol * np.sqrt(T))
    else: # Gamma is same for calls and puts. This is just to cross-check
        gamma = K * np.exp(-r*T) * norm.pdf(dm) / (S * S * vol * np.sqrt(T))
        
    return gamma
      
        
def calc_gamma_exposure(S, K, vol, T, r, q, option_type, open_interest):
    
    gamma = calc_gamma(S, K, vol, T, r, q, option_type)

    gamma_exposure = open_interest * 100 * (S**2) * 0.01 * gamma 
    
    return gamma_exposure

def calc_gamma_exposure_shares(S, K, vol, T, r, q, option_type, open_interest):
    
    gamma = calc_gamma(S, K, vol, T, r, q, option_type)

    gamma_exposure = open_interest * 100 * (S) * gamma 
    
    return gamma_exposure

def calc_gamma_exposure_theoretical(S, K, vol, T, r, q, option_type, open_interest):
    
    gamma = calc_gamma(S, K, vol, T, r, q, option_type)

    gamma_exposure = open_interest * 100 * gamma 
    
    return gamma_exposure


    
def is_third_friday(d):
    return d.weekday() == 4 and 15 <= d.day <= 21



def calculate_spot_total_gamma_call_puts(option_chain_long: pd.DataFrame, spot_price: float) -> pd.DataFrame:
    
    # Get gamma exposure
    # ==================
    idx_puts = option_chain_long['optionType'] == 'put'
    
    option_chain_long['gammaExposure_theoretical'] = (
        option_chain_long['gamma'] * option_chain_long['openInterest']
    )
    
    option_chain_long['gammaExposure_shares'] = (
        option_chain_long['gamma'] * option_chain_long['openInterest'] * 100 * (spot_price)
    )
        
    option_chain_long['gammaExposure'] = (
        option_chain_long['gamma'] * option_chain_long['openInterest'] * 100 * (spot_price**2) * 0.01
    )
    
    option_chain_long.loc[idx_puts,'gammaExposure'] *= -1
    option_chain_long.loc[idx_puts,'gammaExposure_shares'] *= -1
    option_chain_long.loc[idx_puts,'gammaExposure_theoretical'] *= -1
    
    # Get total gamma
    # ===============
    # Total gamma requires adding the gammaExposure for each strike
    total_gamma = (
        option_chain_long[['expiration','strike','optionType','gammaExposure']]
        .groupby(by=['expiration','optionType','strike'])
        .sum()
        .div(10**9)
    )
    
    total_gamma_agg = (
        total_gamma
        .droplevel(level=[0,1])
        .reset_index()
        .groupby(['strike'])
        .sum()
    )
    
    total_gamma_share = (
        option_chain_long[['expiration','strike','optionType','gammaExposure_shares']]
        .groupby(by=['expiration','optionType','strike'])
        .sum()
        .div(10**9)
    )
    
    total_gamma_agg_share = (
        total_gamma_share
        .droplevel(level=[0,1])
        .reset_index()
        .groupby(['strike'])
        .sum()
    )
    
    total_gamma_theoretical = (
        option_chain_long[['expiration','strike','optionType','gammaExposure_theoretical']]
        .groupby(by=['expiration','optionType','strike'])
        .sum()
        .div(10**9)
    )
    
    total_gamma_agg_theo = (
        total_gamma_theoretical
        .droplevel(level=[0,1])
        .reset_index()
        .groupby(['strike'])
        .sum()
    )
    
    # Now get total gamma by calls and puts
    # =====================================
    gamma_exp_call = option_chain_long.loc[option_chain_long['optionType'] == 'call',['strike','gammaExposure']].groupby(by='strike').sum()/ 10**9
    gamma_exp_put = option_chain_long.loc[option_chain_long['optionType'] == 'put',['strike','gammaExposure']].groupby(by='strike').sum()/ 10**9
    
    gamma_exp_call_share = option_chain_long.loc[option_chain_long['optionType'] == 'call',['strike','gammaExposure_shares']].groupby(by='strike').sum()/ 10**9
    gamma_exp_put_share = option_chain_long.loc[option_chain_long['optionType'] == 'put',['strike','gammaExposure_shares']].groupby(by='strike').sum()/ 10**9
    
    gamma_exp_call_theo = option_chain_long.loc[option_chain_long['optionType'] == 'call',['strike','gammaExposure_theoretical']].groupby(by='strike').sum()/ 10**9
    gamma_exp_put_theo = option_chain_long.loc[option_chain_long['optionType'] == 'put',['strike','gammaExposure_theoretical']].groupby(by='strike').sum()/ 10**9
    
    # Prepare output table
    # ====================
    gamma_strikes = pd.concat([
        total_gamma_agg,
        gamma_exp_call,
        gamma_exp_put,
        total_gamma_agg_share,
        gamma_exp_call_share,
        gamma_exp_put_share,
        total_gamma_agg_theo,
        gamma_exp_call_theo,
        gamma_exp_put_theo,
    ],axis=1)
    
    gamma_strikes.columns = [
        'Total Gamma',
        'Total Gamma Call',
        'Total Gamma Put',
        'Total Gamma (share)',
        'Total Gamma (share) Call',
        'Total Gamma (share) Put',
        'Total Gamma (theo)',
        'Total Gamma (theo) Call',
        'Total Gamma (theo) Put',
    ]
    
    return gamma_strikes


def calculate_gamma_profile(option_chain_long: pd.DataFrame, spot_price: float, last_trade_date:pd.Timestamp ,pct_from:float=0.8, pct_to:float=1.2) -> pd.DataFrame:
    
    
    
    # last_trade_date = pd.to_datetime(option_chain_ticker_info.loc['lastTradeTimestamp','SPX'])
    
    
    from_strike = pct_from * spot_price
    to_strike = pct_to * spot_price
    
    levels = np.linspace(from_strike, to_strike, 60)
    
    # For 0DTE options, I'm setting DTE = 1 day, otherwise they get excluded
    option_chain_long['Days untill Expiry'] = [1/262 if (np.busday_count(last_trade_date.date(), x.date())) == 0 \
                            else np.busday_count(last_trade_date.date(), x.date())/262 for x in option_chain_long['expiration']]

    next_expiry = option_chain_long['expiration'].min()

    option_chain_long['Is Third Friday'] = [is_third_friday(x) for x in option_chain_long['expiration']]
    third_fridays = option_chain_long.loc[option_chain_long['Is Third Friday'] == True]
    next_monthly_exp = third_fridays['expiration'].min()
    
    
    total_gamma = []
    total_gamma_ex_next = []
    total_gamma_ex_fri = []

    # For each spot level, calc gamma exposure at that point
    option_chain_copy = option_chain_long.copy()
    print(f"   * GAMMA PROFILE CALC: starting loop (0/{len(levels)})")
    for i, level in enumerate(levels):
        option_chain_copy['gammaExposure'] = option_chain_copy.apply(lambda row : calc_gamma_exposure(level, row['strike'], row['impliedVolatility'], 
                                                            row['Days untill Expiry'], 0, 0, row['optionType'], row['openInterest']), axis = 1)
        
        option_chain_copy_call = option_chain_copy.loc[option_chain_copy['optionType'] == 'call',:]
        option_chain_copy_put = option_chain_copy.loc[option_chain_copy['optionType'] == 'put',:]
        
        total_gamma.append(option_chain_copy_call['gammaExposure'].sum() - option_chain_copy_put['gammaExposure'].sum())
        
        exp_next = option_chain_copy.loc[option_chain_copy['expiration'] != next_expiry]
        exp_next_call = exp_next.loc[exp_next['optionType'] == 'call',:]
        exp_next_put = exp_next.loc[exp_next['optionType'] == 'put',:]
        
        total_gamma_ex_next.append(exp_next_call['gammaExposure'].sum() - exp_next_put['gammaExposure'].sum())

        exp_fri = option_chain_copy.loc[option_chain_copy['expiration'] != next_monthly_exp]
        exp_fri_call = exp_fri.loc[exp_fri['optionType'] == 'call',:]
        exp_fri_put = exp_fri.loc[exp_fri['optionType'] == 'put',:]
        total_gamma_ex_fri.append(exp_fri_call['gammaExposure'].sum() - exp_fri_put['gammaExposure'].sum())
        
        print(f"   * GAMMA PROFILE CALC: finished loop ({i}/{len(levels)})")
        
        
    total_gamma = np.array(total_gamma) / 10**9
    total_gamma_ex_next = np.array(total_gamma_ex_next) / 10**9
    total_gamma_ex_fri = np.array(total_gamma_ex_fri) / 10**9

    # Find Gamma Flip Point
    zero_cross_idx = np.where(np.diff(np.sign(total_gamma)))[0]

    neg_gamma = total_gamma[zero_cross_idx]
    pos_gamma = total_gamma[zero_cross_idx+1]
    neg_strike = levels[zero_cross_idx]
    pos_strike = levels[zero_cross_idx+1]

    zero_gamma = pos_strike - ((pos_strike - neg_strike) * pos_gamma/(pos_gamma-neg_gamma))
    zero_gamma = zero_gamma[0]
    
    # Prepare output
    # ==============
    gamma_profile = pd.DataFrame({
        'Gamma Profile All':total_gamma,
        'Gamma Profile (Ex Next)':total_gamma_ex_next,
        'Gamma Profile (Ex Next Monthly)':total_gamma_ex_fri,
    }, index=levels)
    
    
    return gamma_profile, zero_gamma
    