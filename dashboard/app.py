"""
Dashboard DQ — findata-dq-engine
Lancer : streamlit run dashboard/app.py

Sections
--------
1. Sidebar      — sélection du fichier + options pipeline
2. KPI row      — score global, nb IV, nb mastered, durée
3. Heatmap      — statut par (enregistrement × dimension)
4. Drill-down   — détails des résultats IV par dimension
5. Distribution — histogramme des scores DQ par enregistrement
"""

from __future__ import annotations

import sys
from pathlib import Path

# Assure que findata_dq est importable depuis n'importe où
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from findata_dq.pipeline.orchestrator import DQOrchestrator, OrchestratorConfig
from findata_dq.models.scorecard import Scorecard

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="findata-dq | Dashboard",
    page_icon=":bank:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Helpers CSS ───────────────────────────────────────────────────────────────

STATUS_COLOR = {"V": "#2ecc71", "S": "#f39c12", "IV": "#e74c3c"}
IMPACT_LABEL = {"H": "Critique", "M": "Modéré", "L": "Faible"}

st.markdown("""
<style>
.metric-card {
    background: #1e2130;
    border-radius: 10px;
    padding: 16px 20px;
    margin: 4px;
    border-left: 4px solid #4f8bf9;
}
.metric-card.red  { border-left-color: #e74c3c; }
.metric-card.green{ border-left-color: #2ecc71; }
.metric-card.yellow{ border-left-color: #f39c12; }
.kpi-value { font-size: 2rem; font-weight: 700; color: #fff; }
.kpi-label { font-size: 0.8rem; color: #aaa; text-transform: uppercase; }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("findata-dq")
    st.caption("Buzzelli Extended · v1.0")
    st.divider()

    fixtures = Path(_ROOT) / "tests" / "fixtures"
    csv_files = sorted(fixtures.glob("*.csv")) if fixtures.exists() else []
    csv_labels = [f.name for f in csv_files]

    uploaded = st.file_uploader("Charger un CSV", type=["csv"])
    selected_label = st.selectbox(
        "Ou choisir un fichier de démonstration",
        csv_labels,
        index=0 if csv_labels else None,
    )

    st.divider()
    env = st.selectbox("Environnement", ["development", "staging", "production"])
    ml_enabled = st.checkbox("Isolation Forest (ML)", value=True)
    llm_enabled = st.checkbox("Remédiation LLM", value=False)

    run_btn = st.button("Analyser", type="primary", use_container_width=True)


# ── Fonction de run (mis en cache) ────────────────────────────────────────────

@st.cache_data(show_spinner="Analyse en cours…")
def _run_pipeline(csv_bytes: bytes, filename: str, env: str, ml: bool, llm: bool) -> Scorecard:
    import tempfile, os
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(csv_bytes)
        tmp_path = tmp.name
    try:
        cfg = OrchestratorConfig(pipeline_env=env, ml_enabled=ml, llm_enabled=llm)
        orch = DQOrchestrator(cfg)
        return orch.run_from_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


# ── Chargement du fichier ─────────────────────────────────────────────────────

if "scorecard" not in st.session_state:
    st.session_state.scorecard = None

if run_btn:
    if uploaded:
        sc = _run_pipeline(uploaded.read(), uploaded.name, env, ml_enabled, llm_enabled)
    elif selected_label:
        path = fixtures / selected_label
        sc = _run_pipeline(path.read_bytes(), selected_label, env, ml_enabled, llm_enabled)
    else:
        st.warning("Sélectionnez un fichier CSV pour commencer.")
        sc = None

    if sc:
        st.session_state.scorecard = sc

sc: Scorecard | None = st.session_state.scorecard


# ── Page principale ───────────────────────────────────────────────────────────

st.title("Dashboard Qualité des Données")
st.caption("Buzzelli Extended · 12 dimensions · Raw → Staged → Mastered")

if sc is None:
    st.info("Choisissez un fichier dans la barre latérale et cliquez sur **Analyser**.")
    st.stop()


# ── Section 1 : KPIs ─────────────────────────────────────────────────────────

score_color = "green" if sc.global_dq_score >= 80 else ("yellow" if sc.global_dq_score >= 60 else "red")
iv_color = "red" if sc.nb_iv_high_impact > 0 else "yellow"

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.markdown(f"""
    <div class="metric-card {score_color}">
        <div class="kpi-value">{sc.global_dq_score:.1f}<span style="font-size:1rem">/100</span></div>
        <div class="kpi-label">Score DQ global</div>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="metric-card {iv_color}">
        <div class="kpi-value">{sc.nb_iv_total}</div>
        <div class="kpi-label">Résultats IV ({sc.nb_iv_high_impact} critiques)</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="kpi-value">{sc.nb_s_total}</div>
        <div class="kpi-label">Résultats Suspect</div>
    </div>""", unsafe_allow_html=True)

with k4:
    pct_mastered = (sc.nb_records_mastered_eligible / sc.total_records * 100) if sc.total_records else 0
    mc = "green" if pct_mastered >= 90 else ("yellow" if pct_mastered >= 70 else "red")
    st.markdown(f"""
    <div class="metric-card {mc}">
        <div class="kpi-value">{sc.nb_records_mastered_eligible}<span style="font-size:1rem">/{sc.total_records}</span></div>
        <div class="kpi-label">Éligibles Mastered ({pct_mastered:.0f}%)</div>
    </div>""", unsafe_allow_html=True)

with k5:
    dur = f"{sc.pipeline_duration_seconds:.2f}s" if sc.pipeline_duration_seconds else "—"
    st.markdown(f"""
    <div class="metric-card">
        <div class="kpi-value">{dur}</div>
        <div class="kpi-label">Durée pipeline</div>
    </div>""", unsafe_allow_html=True)

st.divider()


# ── Section 2 : Heatmap dimension × enregistrement ───────────────────────────

st.subheader("Heatmap — Statut par dimension")

heatmap_data = sc.to_heatmap_data()

if heatmap_data:
    df_heat = pd.DataFrame(heatmap_data)

    # Pivot : lignes = record_id, colonnes = dimension, valeurs = status → score
    status_num = {"V": 1.0, "S": 0.5, "IV": 0.0}
    df_heat["score_num"] = df_heat["status"].map(status_num)

    # Limiter à max 200 enregistrements pour lisibilité
    all_records = df_heat["record_id"].unique()
    if len(all_records) > 200:
        sample_ids = list(all_records[:200])
        df_heat = df_heat[df_heat["record_id"].isin(sample_ids)]
        st.caption(f"Affichage limité aux 200 premiers enregistrements (sur {len(all_records)}).")

    pivot = df_heat.pivot_table(
        index="record_id",
        columns="dimension",
        values="score_num",
        aggfunc="min",   # pire score par (record, dimension)
    ).fillna(1.0)

    fig_heat = go.Figure(go.Heatmap(
        z=pivot.values,
        x=list(pivot.columns),
        y=list(pivot.index),
        zmin=0.0, zmax=1.0,
        colorscale=[[0.0, "#e74c3c"], [0.5, "#f39c12"], [1.0, "#2ecc71"]],
        colorbar=dict(
            title="Score DQ",
            tickvals=[0.0, 0.5, 1.0],
            ticktext=["IV", "S", "V"],
        ),
    ))
    fig_heat.update_layout(
        title=f"Heatmap DQ — {sc.dataset} ({sc.total_records} enregistrements)",
        height=max(300, min(60 * len(pivot), 800)),
        xaxis_title="Dimension",
        yaxis_title="Enregistrement",
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font_color="#fff",
    )
    st.plotly_chart(fig_heat, width='stretch')
else:
    st.info("Aucun résultat à afficher.")

st.divider()


# ── Section 3 : Score par dimension (barres) ─────────────────────────────────

st.subheader("Score par dimension")

if sc.by_dimension:
    dim_rows = [
        {
            "Dimension": name,
            "Score": round(ds.dimension_score * 100, 1),
            "V": ds.nb_v,
            "S": ds.nb_s,
            "IV": ds.nb_iv,
            "IV Critiques": ds.nb_iv_high,
            "Testés": ds.nb_tested,
        }
        for name, ds in sorted(sc.by_dimension.items(), key=lambda x: x[1].dimension_score)
    ]
    df_dims = pd.DataFrame(dim_rows)
    # Cast explicite : pyarrow (vieille version) ne tolère pas numpy.float64 implicite
    df_dims["Score"] = df_dims["Score"].apply(float)
    for col in ("V", "S", "IV", "IV Critiques", "Testés"):
        df_dims[col] = df_dims[col].apply(int)

    bar_colors = [
        "#2ecc71" if s >= 80 else ("#f39c12" if s >= 60 else "#e74c3c")
        for s in df_dims["Score"]
    ]
    fig_bar = go.Figure(go.Bar(
        x=df_dims["Score"],
        y=df_dims["Dimension"],
        orientation="h",
        marker_color=bar_colors,
        text=[f"{s:.1f}" for s in df_dims["Score"]],
        textposition="outside",
    ))
    fig_bar.update_layout(
        title="Score DQ par dimension (sur 100)",
        height=max(250, 45 * len(df_dims)),
        showlegend=False,
        xaxis=dict(range=[0, 110], title="Score /100"),
        yaxis_title="Dimension",
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font_color="#fff",
    )
    st.plotly_chart(fig_bar, width='stretch')

    # Tableau détaillé (HTML natif — contourne les incompatibilités Arrow/pyarrow)
    with st.expander("Détail par dimension"):
        st.markdown(
            df_dims.set_index("Dimension").to_html(),
            unsafe_allow_html=True,
        )

st.divider()


# ── Section 4 : Drill-down IV ────────────────────────────────────────────────

st.subheader("Résultats Invalides (IV)")

iv_results = sc.get_iv_results()

if iv_results:
    iv_rows = [
        {
            "Enregistrement": r.record_id,
            "Dimension": r.dimension,
            "Champ": r.field_name,
            "Valeur": r.field_value or "—",
            "Impact": IMPACT_LABEL.get(r.impact, r.impact),
            "Règle": r.rule_applied[:80] + "…" if len(r.rule_applied) > 80 else r.rule_applied,
            "Remédiation": (
                r.remediation.action if r.remediation else "—"
            ),
        }
        for r in iv_results
    ]
    df_iv = pd.DataFrame(iv_rows)

    # Filtre par dimension
    dims_with_iv = sorted(df_iv["Dimension"].unique().tolist())
    selected_dim = st.multiselect("Filtrer par dimension", dims_with_iv, default=dims_with_iv)
    df_filtered = df_iv[df_iv["Dimension"].isin(selected_dim)] if selected_dim else df_iv

    # HTML natif — contourne les incompatibilités Arrow/pyarrow
    st.markdown(
        df_filtered.to_html(index=False, max_rows=500),
        unsafe_allow_html=True,
    )
    st.caption(f"{len(df_filtered)} résultats IV affichés.")
else:
    st.success("Aucun résultat IV — toutes les données sont valides ou suspectes.")

st.divider()


# ── Section 5 : Distribution des scores par enregistrement ───────────────────

st.subheader("Distribution des scores par enregistrement")

if sc.by_record:
    scores = [rec.global_score for rec in sc.by_record.values()]
    fig_hist = go.Figure(go.Histogram(
        x=scores,
        nbinsx=20,
        marker_color="#4f8bf9",
        name="Enregistrements",
    ))
    fig_hist.add_vline(
        x=sc.global_dq_score / 100,
        line_dash="dash",
        line_color="#f39c12",
        annotation_text=f"Moyenne : {sc.global_dq_score:.1f}",
        annotation_position="top right",
    )
    fig_hist.update_layout(
        title="Distribution du score DQ global par enregistrement",
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font_color="#fff",
        xaxis_title="Score DQ (0 = IV, 0.5 = S, 1 = V)",
        yaxis_title="Nombre d'enregistrements",
        showlegend=False,
    )
    st.plotly_chart(fig_hist, width='stretch')


# ── Section 6 : Anomalies ML ────────────────────────────────────────────────

if sc.nb_ml_anomalies > 0:
    st.divider()
    st.subheader(f"Anomalies ML — Isolation Forest ({sc.nb_ml_anomalies} détectées)")
    st.markdown(
        pd.DataFrame({"Enregistrement": sc.ml_anomaly_record_ids}).to_html(index=False),
        unsafe_allow_html=True,
    )


# ── Footer ────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    f"findata-dq-engine · Scorecard `{sc.scorecard_id[:8]}…` · "
    f"Évalué le {sc.evaluated_at.strftime('%Y-%m-%d %H:%M UTC')} · "
    f"Env : {sc.pipeline_env}"
)
