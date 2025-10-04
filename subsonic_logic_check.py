import numpy as np
from datetime import datetime, time

def is_london_session(dt):
    """Check if datetime is within London session (07:00–11:00 UTC)."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return time(7, 0) <= dt.time() <= time(11, 0)

def body_size(open_, close):
    return abs(close - open_)

def average_body(candles, n=14):
    bodies = [body_size(c['Open'], c['Close']) for c in candles[-n:]]
    return float(np.mean(bodies)) if bodies else 0.0

def subsonic_step_check(bar, last_ema_cross_idx, current_idx, candles, params):
    """
    bar: dict of OHLCV + indicators for the current bar
    last_ema_cross_idx: int (index of last EMA cross)
    current_idx: int (index of current bar)
    candles: list of previous bars (for averages, sweeps, etc)
    params: dict of thresholds/settings for your system
    Returns: (is_candidate, fail_reason, details_dict)
    """

    dt = bar['Datetime']

    # 1. Session Filter
    if not is_london_session(dt):
        return False, "Not London session", {}

    # 2. EMA Cross Day Filter (skip first session after cross)
    if current_idx <= last_ema_cross_idx:
        return False, "Before EMA cross", {}
    if (current_idx - last_ema_cross_idx) == 1:
        return False, "First session after EMA cross", {}

    # 3. Bias Alignment (directional logic)
    if params['side'] == 'long':
        if not (bar['Close'] > bar['EMA200'] and bar['EMA34'] > bar['EMA200']):
            return False, "Bias not aligned for long", {}
    else:
        if not (bar['Close'] < bar['EMA200'] and bar['EMA34'] < bar['EMA200']):
            return False, "Bias not aligned for short", {}

    # 4. Liquidity Sweep Detection
    if not bar.get('liquidity_sweep', False):
        return False, "No liquidity sweep", {}

    # 5. Confirmation Candle
    if len(candles) < 14:
        return False, "Not enough candles for average body", {}
    avg_body = average_body(candles, 14)
    bsize = body_size(bar['Open'], bar['Close'])
    if bsize < avg_body:
        return False, "Body not big enough", {}
    wick_size = (bar['High'] - bar['Close']) if params['side'] == 'long' else (bar['Close'] - bar['Low'])
    if wick_size > params.get('max_entry_wick', 0.1 * avg_body):
        return False, "Wick too large in entry direction", {}

    # 6. Retest Logic
    if not bar.get('retest_confirmed', False):
        return False, "No retest", {}

    # 7. SL/TP Feasibility
    sl_zone = bar.get('sl_zone')
    if not sl_zone:
        return False, "No SL zone info", {}
    sl_distance = abs(bar['EntryPrice'] - sl_zone)
    if sl_distance < params['min_sl_pips']:
        return False, "SL too tight", {}
    if sl_distance > params['max_sl_pips']:
        return False, "SL too wide", {}
    rr = params['rr_target']
    tp_distance = rr * sl_distance
    if not bar.get('can_reach_tp', True):
        return False, "RR target not feasible", {}

    # 8. Lot Size Calculation (needs account/risk logic)
    lot_size = params.get('fixed_lot', 1.0)  # Or use your risk-based calculation
    if lot_size <= 0:
        return False, "Invalid lot size", {}

    # 9. Output/Scoring
    details = {
        "side": params['side'],
        "entry": bar['EntryPrice'],
        "sl": sl_zone,
        "tp": bar['EntryPrice'] + tp_distance if params['side'] == 'long' else bar['EntryPrice'] - tp_distance,
        "score": bar.get('setup_score', 0),
        "lot_size": lot_size
    }
    return True, "Setup candidate", details
