"""Análise de drift em série temporal: EWT, changepoints e modelo bayesiano.

Série: total mensal de internações renais (Sul+Sudeste), agregado nacional.
Saídas: figuras em reports/figures/timeseries_drift/.
"""

from __future__ import annotations

import logging
from pathlib import Path

import arviz as az
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyewt
import pymc as pm
import ruptures as rpt
import seaborn as sns
from scipy import stats

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

plt.style.use("seaborn-v0_8-whitegrid")
sns.set_context("notebook", font_scale=1.0)

_PANDEMIA_INI = 2020
_PANDEMIA_FIM = 2022
_MCMC_DRAWS = 800
_MCMC_TUNE = 800
_MCMC_CHAINS = 2
_MCMC_CORES = 1
_RNG = 42
_MIN_AMOSTRAS_TESTE = 2
_MIN_LEN_PAR_HIST = 2
_REG_PRE = 0
_REG_PANDEMIA = 1
_REG_POS = 2
_N_BINS_HIST = 18

_REGIME_LABELS = {
    _REG_PRE: "Pré-pandemia",
    _REG_PANDEMIA: "Pandemia (2020–2022)",
    _REG_POS: "Pós-pandemia",
}
_REGIME_COLORS = {
    _REG_PRE: "steelblue",
    _REG_PANDEMIA: "indianred",
    _REG_POS: "seagreen",
}


def _load_monthly_national(interim_dir: Path) -> pd.DataFrame:
    path = interim_dir / "agg_evolucao_temporal.parquet"
    if not path.exists():
        msg = "Execute notebooks/processing/02_aggregate_data.py antes."
        raise FileNotFoundError(msg)
    df = pd.read_parquet(path)
    df = df.sort_values(["ANO", "MES"]).reset_index(drop=True)
    df["DATA"] = pd.to_datetime(
        {"year": df["ANO"], "month": df["MES"], "day": 1}
    )
    return df


def _period_idx(ano: int) -> int:
    if ano < _PANDEMIA_INI:
        return 0
    if ano <= _PANDEMIA_FIM:
        return 1
    return 2


def plot_serie_com_regimes(df: pd.DataFrame, fig_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(df["DATA"], df["TOTAL"], color="steelblue", lw=1.5)
    t0 = pd.Timestamp(year=_PANDEMIA_INI, month=1, day=1)
    t1 = pd.Timestamp(year=_PANDEMIA_FIM, month=12, day=31)
    ax.axvspan(
        t0,
        t1,
        color="crimson",
        alpha=0.12,
        label="Pandemia (2020–2022)",
    )
    ax.set_title("Série mensal — total nacional (escopo Sul+Sudeste)")
    ax.set_ylabel("Internações / mês")
    ax.set_xlabel("Mês")
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    plt.tight_layout()
    out = fig_dir / "01_serie_mensal_regimes.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def plot_ewt_decomp(
    y_demean: np.ndarray,
    y_niveis: np.ndarray,
    dates: pd.Series,
    fig_dir: Path,
) -> None:
    params = pyewt.Default_Params()
    params["N"] = min(5, max(3, len(y_demean) // 25))
    ewt, _mfb, bounds = pyewt.ewt1d(y_demean.astype(float), params)
    n_comp = len(ewt)
    fig, axes = plt.subplots(n_comp + 1, 1, figsize=(14, 2.2 * (n_comp + 1)))
    axes[0].plot(dates, y_niveis, color="black", lw=1.2)
    axes[0].set_title("Sinal original (níveis)")
    for k, comp in enumerate(ewt):
        ax = axes[k + 1]
        c = np.real(np.asarray(comp).ravel())
        ax.plot(dates, c, lw=1.0)
        ax.set_title("EWT componente %s" % (k + 1))
    for ax in axes:
        ax.set_xlabel("Mês")
    fig.autofmt_xdate()
    plt.tight_layout()
    out = fig_dir / "02_ewt_componentes.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s (fronteiras FFT normalizadas: %s)", out, bounds)
    plt.close(fig)


def plot_changepoints(
    y_niveis: np.ndarray, dates: pd.Series, fig_dir: Path
) -> None:
    """PELT com custo `l2` na série em **internações/mês** (sem z-score).

    Penalidade: log(n) * var(y), heurística comum para escala dos dados.
    """
    yf = y_niveis.astype(float)
    n = len(yf)
    algo = rpt.Pelt(model="l2").fit(yf)
    pen = float(np.log(n) * np.var(yf))
    bkps = algo.predict(pen=pen)
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(
        dates,
        yf,
        color="steelblue",
        lw=1.2,
        label="Internações/mês (Sul+Sudeste)",
    )
    for b in bkps[:-1]:
        ax.axvline(
            dates.iloc[b],
            color="darkorange",
            ls="--",
            lw=1.2,
            alpha=0.9,
        )
    ax.set_title(
        "PELT (custo L²) — mudanças de patamar sugeridas "
        "(série original; pen = log(n)·Var(y))"
    )
    ax.set_ylabel("Internações no mês")
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    plt.tight_layout()
    out = fig_dir / "03_changepoints_pelt.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s breakpoints=%s", out, bkps)
    plt.close(fig)


def _par_hist_ok(va: np.ndarray, vb: np.ndarray) -> bool:
    return len(va) >= _MIN_LEN_PAR_HIST and len(vb) >= _MIN_LEN_PAR_HIST


def _split_by_regime(
    y: np.ndarray, period: np.ndarray
) -> dict[int, np.ndarray]:
    return {
        _REG_PRE: y[period == _REG_PRE],
        _REG_PANDEMIA: y[period == _REG_PANDEMIA],
        _REG_POS: y[period == _REG_POS],
    }


def _common_bins(a: np.ndarray, b: np.ndarray, n_bins: int) -> np.ndarray:
    lo = float(min(np.min(a), np.min(b)))
    hi = float(max(np.max(a), np.max(b)))
    if hi <= lo:
        hi = lo + 1.0
    return np.linspace(lo, hi, n_bins + 1)


def _hist_rel_freq(
    x: np.ndarray, bins: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    counts, edges = np.histogram(x, bins=bins)
    total = counts.sum()
    if total == 0:
        freq = np.zeros_like(counts, dtype=float)
    else:
        freq = counts.astype(float) / total
    centers = (edges[:-1] + edges[1:]) / 2.0
    return centers, freq


def plot_histogram_drift_overlap(
    y: np.ndarray, period: np.ndarray, fig_dir: Path
) -> None:
    """Histogramas sobrepostos: contagem de meses por bin."""
    by_r = _split_by_regime(y, period)
    pairs = [
        (_REG_PANDEMIA, _REG_PRE, "Pandemia vs pré"),
        (_REG_POS, _REG_PRE, "Pós vs pré"),
        (_REG_POS, _REG_PANDEMIA, "Pós vs pandemia"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    for ax, (ra, rb, title) in zip(axes, pairs, strict=True):
        va, vb = by_r[ra], by_r[rb]
        if not _par_hist_ok(va, vb):
            ax.set_title("%s (dados insuficientes)" % title)
            continue
        bins = _common_bins(va, vb, _N_BINS_HIST)
        ax.hist(
            va,
            bins=bins,
            density=False,
            alpha=0.55,
            color=_REGIME_COLORS[ra],
            label=_REGIME_LABELS[ra],
        )
        ax.hist(
            vb,
            bins=bins,
            density=False,
            alpha=0.55,
            color=_REGIME_COLORS[rb],
            label=_REGIME_LABELS[rb],
        )
        ax.set_title(title)
        ax.set_xlabel("Internações / mês")
        ax.set_ylabel("N.º de meses (no bin)")
        ax.legend(fontsize=8)
    plt.suptitle(
        "Drift — sobreposição de histogramas (níveis mensais)",
        y=1.02,
    )
    plt.tight_layout()
    out = fig_dir / "06_histograma_drift_sobreposicao.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def plot_histogram_drift_diff(
    y: np.ndarray, period: np.ndarray, fig_dir: Path
) -> None:
    """Δ frequência relativa por bin (referência − comparação)."""
    by_r = _split_by_regime(y, period)
    triples = [
        (_REG_PANDEMIA, _REG_PRE, "Δ freq: pandemia − pré"),
        (_REG_POS, _REG_PRE, "Δ freq: pós − pré"),
        (_REG_POS, _REG_PANDEMIA, "Δ freq: pós − pandemia"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    for ax, (ra, rb, title) in zip(axes, triples, strict=True):
        va, vb = by_r[ra], by_r[rb]
        if not _par_hist_ok(va, vb):
            ax.set_title("%s (dados insuficientes)" % title)
            continue
        bins = _common_bins(va, vb, _N_BINS_HIST)
        centers, fa = _hist_rel_freq(va, bins)
        _c2, fb = _hist_rel_freq(vb, bins)
        delta = fa - fb
        colors = np.where(delta >= 0, "indianred", "steelblue")
        ax.bar(
            centers,
            delta,
            width=float(np.diff(bins).mean()),
            color=colors,
            alpha=0.75,
        )
        ax.axhline(0, color="black", lw=0.8)
        ax.set_title(title)
        ax.set_xlabel("Internações / mês (centro do bin)")
        ax.set_ylabel("Δ frequência relativa")
    plt.suptitle(
        "Drift — diferença entre histogramas "
        "(mesmos bins; Σ freq = 1 por grupo)",
        y=1.03,
    )
    plt.tight_layout()
    out = fig_dir / "07_histograma_drift_delta_frequencia.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def plot_histogram_drift_diff_niveis_milhares(  # noqa: PLR0914
    y: np.ndarray, period: np.ndarray, fig_dir: Path
) -> None:
    """Igual ao ficheiro `07`, com eixo X em milhares de internações/mês."""
    by_r = _split_by_regime(y, period)
    triples = [
        (_REG_PANDEMIA, _REG_PRE, "Δ freq: pandemia − pré"),
        (_REG_POS, _REG_PRE, "Δ freq: pós − pré"),
        (_REG_POS, _REG_PANDEMIA, "Δ freq: pós − pandemia"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.8))
    for ax, (ra, rb, title) in zip(axes, triples, strict=True):
        va, vb = by_r[ra], by_r[rb]
        if not _par_hist_ok(va, vb):
            ax.set_title("%s (dados insuficientes)" % title)
            continue
        bins = _common_bins(va, vb, _N_BINS_HIST)
        centers, fa = _hist_rel_freq(va, bins)
        _c2, fb = _hist_rel_freq(vb, bins)
        delta = fa - fb
        colors = np.where(delta >= 0, "indianred", "steelblue")
        cx = centers / 1000.0
        w = float(np.diff(bins).mean()) / 1000.0
        ax.bar(cx, delta, width=w, color=colors, alpha=0.75)
        ax.axhline(0, color="black", lw=0.8)
        ax.set_title(title)
        ax.set_xlabel("Internações/mês (milhares; centro do bin)")
        ax.set_ylabel("Δ frequência relativa")
    plt.suptitle(
        "Drift — diferença de histogramas em escala original "
        "(não é z-score; mesmos bins que 07, eixo em 10³)",
        y=1.03,
    )
    plt.tight_layout()
    out = fig_dir / "11_histograma_drift_delta_niveis_milhares.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def plot_histogram_drift_zscore(
    y: np.ndarray, period: np.ndarray, fig_dir: Path
) -> None:
    """Mesma lógica em z-score global (comparável ao modelo bayesiano)."""
    z = (y - y.mean()) / y.std()
    by_r = _split_by_regime(z, period)
    pairs = [
        (_REG_PANDEMIA, _REG_PRE, "Pandemia vs pré (z)"),
        (_REG_POS, _REG_PRE, "Pós vs pré (z)"),
        (_REG_POS, _REG_PANDEMIA, "Pós vs pandemia (z)"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    for ax, (ra, rb, title) in zip(axes, pairs, strict=True):
        va, vb = by_r[ra], by_r[rb]
        if not _par_hist_ok(va, vb):
            ax.set_title("%s (dados insuficientes)" % title)
            continue
        bins = _common_bins(va, vb, _N_BINS_HIST)
        ax.hist(
            va,
            bins=bins,
            density=True,
            alpha=0.55,
            color=_REGIME_COLORS[ra],
            label=_REGIME_LABELS[ra],
        )
        ax.hist(
            vb,
            bins=bins,
            density=True,
            alpha=0.55,
            color=_REGIME_COLORS[rb],
            label=_REGIME_LABELS[rb],
        )
        ax.set_title(title)
        ax.set_xlabel("z-score (série mensal)")
        ax.set_ylabel("Densidade")
        ax.legend(fontsize=8)
    plt.suptitle(
        "Drift — histogramas sobrepostos (padronização global)",
        y=1.02,
    )
    plt.tight_layout()
    out = fig_dir / "08_histograma_drift_zscore_sobreposicao.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def plot_histogram_drift_diff_zscore(
    y: np.ndarray, period: np.ndarray, fig_dir: Path
) -> None:
    """Δ frequência relativa em z-score (mesma escala do modelo bayesiano)."""
    z = (y - y.mean()) / y.std()
    by_r = _split_by_regime(z, period)
    triples = [
        (_REG_PANDEMIA, _REG_PRE, "Δ freq (z): pandemia − pré"),
        (_REG_POS, _REG_PRE, "Δ freq (z): pós − pré"),
        (_REG_POS, _REG_PANDEMIA, "Δ freq (z): pós − pandemia"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    for ax, (ra, rb, title) in zip(axes, triples, strict=True):
        va, vb = by_r[ra], by_r[rb]
        if not _par_hist_ok(va, vb):
            ax.set_title("%s (dados insuficientes)" % title)
            continue
        bins = _common_bins(va, vb, _N_BINS_HIST)
        centers, fa = _hist_rel_freq(va, bins)
        _c2, fb = _hist_rel_freq(vb, bins)
        delta = fa - fb
        colors = np.where(delta >= 0, "indianred", "steelblue")
        ax.bar(
            centers,
            delta,
            width=float(np.diff(bins).mean()),
            color=colors,
            alpha=0.75,
        )
        ax.axhline(0, color="black", lw=0.8)
        ax.set_title(title)
        ax.set_xlabel("z-score (centro do bin)")
        ax.set_ylabel("Δ frequência relativa")
    plt.suptitle(
        "Drift — diferença de histogramas na escala z global",
        y=1.03,
    )
    plt.tight_layout()
    out = fig_dir / "09_histograma_drift_delta_frequencia_zscore.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def plot_bayesian_contrasts_niveis(
    trace: az.InferenceData, y: np.ndarray, fig_dir: Path
) -> None:
    """KDE dos contrastes Δμ em internações/mês (Δz vezes desvio-padrão)."""
    sy = float(np.std(y, ddof=0))
    if sy <= 0:
        logger.warning("Desvio-padrão nulo; omitindo KDE bayesiano em níveis.")
        return
    post = trace.posterior
    d1 = (
        post["mu"].sel(regime="pandemia") - post["mu"].sel(regime="pre")
    ).values.ravel() * sy
    d2 = (
        post["mu"].sel(regime="pos") - post["mu"].sel(regime="pre")
    ).values.ravel() * sy
    d3 = (
        post["mu"].sel(regime="pos") - post["mu"].sel(regime="pandemia")
    ).values.ravel() * sy
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2))
    labels = [
        "Δ pandemia − pré",
        "Δ pós − pré",
        "Δ pós − pandemia",
    ]
    subt = (
        "Mesmas amostras MCMC que em 05; eixo × desvio-padrão global "
        "da série (interpretação de patamar em internações/mês)"
    )
    for ax, delta, lab in zip(axes, (d1, d2, d3), labels, strict=True):
        sns.kdeplot(delta, ax=ax, fill=True, color="darkcyan", alpha=0.45)
        ax.axvline(0, color="red", ls="--", lw=1)
        ax.set_title(lab)
        ax.set_xlabel("Δ patamar aprox. (internações/mês)")
    plt.suptitle(
        "Contraste bayesiano (KDE) — escala de internações/mês\n%s" % subt,
        y=1.08,
        fontsize=11,
    )
    plt.tight_layout()
    out = fig_dir / "10_bayes_kde_contrastes_internacoes_mes.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def run_bayesian_regimes(
    y: np.ndarray, period: np.ndarray
) -> tuple[az.InferenceData, dict[str, float]]:
    """Três regimes: 0 pré, 1 pandemia, 2 pós (índice por ano)."""
    y_obs = (y - y.mean()) / y.std()
    coords = {"regime": ["pre", "pandemia", "pos"]}
    with pm.Model(coords=coords):
        mu = pm.Normal("mu", mu=0.0, sigma=1.5, dims="regime")
        sigma = pm.HalfNormal("sigma", sigma=1.0)
        pm.Normal("y", mu=mu[period], sigma=sigma, observed=y_obs)
        trace = pm.sample(
            draws=_MCMC_DRAWS,
            tune=_MCMC_TUNE,
            chains=_MCMC_CHAINS,
            cores=_MCMC_CORES,
            random_seed=_RNG,
            progressbar=False,
        )
    post = trace.posterior
    mu_pre = post["mu"].sel(regime="pre").values.ravel()
    mu_pan = post["mu"].sel(regime="pandemia").values.ravel()
    mu_pos = post["mu"].sel(regime="pos").values.ravel()
    summary = {
        "P_mu_pandemia_gt_pre": float(np.mean(mu_pan > mu_pre)),
        "P_mu_pos_gt_pre": float(np.mean(mu_pos > mu_pre)),
        "P_mu_pos_gt_pandemia": float(np.mean(mu_pos > mu_pan)),
        "mean_mu_pre": float(np.mean(mu_pre)),
        "mean_mu_pandemia": float(np.mean(mu_pan)),
        "mean_mu_pos": float(np.mean(mu_pos)),
    }
    return trace, summary


def plot_bayesian(trace: az.InferenceData, fig_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    az.plot_forest(trace, var_names=["mu"], combined=True, ax=ax)
    ax.set_title("Posterior de μ por regime (série padronizada)")
    plt.tight_layout()
    out = fig_dir / "04_bayes_mu_forest.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def plot_bayesian_contrasts(
    trace: az.InferenceData, fig_dir: Path
) -> None:
    post = trace.posterior
    d1 = (
        post["mu"].sel(regime="pandemia") - post["mu"].sel(regime="pre")
    ).values.ravel()
    d2 = (
        post["mu"].sel(regime="pos") - post["mu"].sel(regime="pre")
    ).values.ravel()
    d3 = (
        post["mu"].sel(regime="pos") - post["mu"].sel(regime="pandemia")
    ).values.ravel()
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    labels = [
        "Δ pandemia − pré",
        "Δ pós − pré",
        "Δ pós − pandemia",
    ]
    for ax, delta, lab in zip(axes, (d1, d2, d3), labels, strict=True):
        sns.kdeplot(delta, ax=ax, fill=True, color="teal", alpha=0.5)
        ax.axvline(0, color="red", ls="--", lw=1)
        ax.set_title(lab)
        ax.set_xlabel("Diferença (posterior)")
    plt.suptitle("Contraste bayesiano entre regimes", y=1.02)
    plt.tight_layout()
    out = fig_dir / "05_bayes_contrastes_kde.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    logger.info("Salvo %s", out)
    plt.close(fig)


def frequentist_tests(y: np.ndarray, period: np.ndarray) -> dict[str, float]:
    g0 = y[period == _REG_PRE]
    g1 = y[period == _REG_PANDEMIA]
    g2 = y[period == _REG_POS]
    out: dict[str, float] = {}
    m = _MIN_AMOSTRAS_TESTE
    if len(g0) > m and len(g1) > m:
        _, p = stats.mannwhitneyu(g1, g0, alternative="two-sided")
        out["mannwhitney_p_pandemia_vs_pre"] = float(p)
    if len(g0) > m and len(g2) > m:
        _, p = stats.mannwhitneyu(g2, g0, alternative="two-sided")
        out["mannwhitney_p_pos_vs_pre"] = float(p)
    if len(g1) > m and len(g2) > m:
        _, p = stats.mannwhitneyu(g2, g1, alternative="two-sided")
        out["mannwhitney_p_pos_vs_pandemia"] = float(p)
    return out


def main() -> None:
    base = Path(__file__).resolve().parents[2]
    interim_dir = base / "data" / "interim"
    fig_dir = base / "reports" / "figures" / "timeseries_drift"
    fig_dir.mkdir(parents=True, exist_ok=True)

    df = _load_monthly_national(interim_dir)
    y = df["TOTAL"].astype(float).values
    y_dm = y - y.mean()
    period = np.array([_period_idx(int(a)) for a in df["ANO"]], dtype=np.int64)

    plot_serie_com_regimes(df, fig_dir)
    plot_ewt_decomp(y_dm, y, df["DATA"], fig_dir)

    plot_changepoints(y, df["DATA"], fig_dir)

    plot_histogram_drift_overlap(y, period, fig_dir)
    plot_histogram_drift_diff(y, period, fig_dir)
    plot_histogram_drift_diff_niveis_milhares(y, period, fig_dir)
    plot_histogram_drift_zscore(y, period, fig_dir)
    plot_histogram_drift_diff_zscore(y, period, fig_dir)

    freq = frequentist_tests(y, period)
    for k, v in freq.items():
        logger.info("%s = %.4g", k, v)

    logger.info("MCMC (PyMC) — pode levar ~1–3 min...")
    trace, bayes_sum = run_bayesian_regimes(y, period)
    for k, v in bayes_sum.items():
        logger.info("%s = %.4f", k, v)

    plot_bayesian(trace, fig_dir)
    plot_bayesian_contrasts(trace, fig_dir)
    plot_bayesian_contrasts_niveis(trace, y, fig_dir)

    summ_path = fig_dir / "resumo_bayesiano.csv"
    pd.DataFrame([bayes_sum]).to_csv(summ_path, index=False)
    logger.info("Resumo bayesiano: %s", summ_path)


if __name__ == "__main__":
    main()
