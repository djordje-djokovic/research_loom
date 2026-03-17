"""Generic stats fitting helpers."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple


def fit_with_covariance(model, *, clusters=None, fit_kwargs: Optional[Dict[str, Any]] = None) -> Tuple[Any, str]:
    fit_kwargs = fit_kwargs or {}
    if clusters is not None:
        try:
            if getattr(clusters, "nunique", lambda: 0)() > 1:
                res = model.fit(cov_type="cluster", cov_kwds={"groups": clusters.values}, **fit_kwargs)
                return res, "cluster-robust"
        except Exception:
            pass
    try:
        res = model.fit(cov_type="HC1", **fit_kwargs)
        return res, "HC1"
    except Exception:
        res = model.fit(**fit_kwargs)
        return res, "conventional"
