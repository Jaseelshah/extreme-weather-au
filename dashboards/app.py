# Plotly Dash dashboard — three pages, dark navy theme.
# All data is loaded from SQLite at startup, charts built in memory.
# Run with: python dashboards/app.py  →  http://localhost:8050
#
# TODO: add date range filter to Page 1 (currently shows all years at once)

import sys
import sqlite3
import threading
import time
import webbrowser
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
DB_PATH = PROJECT_ROOT / "data" / "extreme_weather.db"

BG         = "#0a192f"       # deep navy
CARD_BG    = "#112240"
BORDER     = "#1e3a5f"
TEXT       = "#ccd6f6"
TEXT_MUTED = "#8892b0"
ACCENT     = "#64ffda"       # teal — used for headings and KPI values
ACCENT2    = "#4cc9f0"

HAZARD_COLORS = {
    "rain":      "#4895ef",
    "wind":      "#56cfe1",
    "hail":      "#9d4edd",
    "tornado":   "#f77f00",
    "lightning": "#ffd60a",
}

STATE_COLORS = {
    "NSW":       "#ef4444",
    "QLD":       "#10b981",
    "WA":        "#f59e0b",
    "VIC":       "#3b82f6",
    "SA":        "#ec4899",
    "NT":        "#06b6d4",
    "TAS":       "#8b5cf6",
    "NATIONAL":  "#94a3b8",
    "SEQ":       "#22d3ee",
    "OTHER":     "#6b7280",
}
STATE_ORDER = ["NSW", "QLD", "WA", "VIC", "SA", "NT", "TAS"]

CHART_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d", "toImage"],
    "scrollZoom": False,
}


def hex_to_rgba(hex_color: str, alpha: float = 0.75) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def apply_dark_theme(fig: go.Figure, height: int = 380, title: str = "",
                     legend_horiz: bool = True) -> go.Figure:
    """Apply consistent dark theme to any figure."""
    legend_kw = (
        dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        if legend_horiz
        else dict(orientation="v", x=1.02, xanchor="left", y=1, yanchor="top")
    )
    fig.update_layout(
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color=TEXT, family="'Segoe UI', 'Helvetica Neue', sans-serif", size=12),
        title=dict(text=title, font=dict(color=TEXT, size=14), x=0.01, xanchor="left"),
        xaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER, linecolor=BORDER, tickcolor=TEXT_MUTED),
        yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER, linecolor=BORDER, tickcolor=TEXT_MUTED),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT, size=11), **legend_kw),
        margin=dict(l=55, r=20, t=55, b=50),
        height=height,
        hoverlabel=dict(bgcolor="#0d2137", font_color=TEXT, bordercolor=BORDER),
    )
    return fig


def load_all_data() -> dict:
    conn = sqlite3.connect(str(DB_PATH))

    mbs = pd.read_sql(
        "SELECT year_month, state, hazard_type, event_count FROM monthly_events_by_state", conn
    )
    mbh = pd.read_sql(
        "SELECT year_month, hazard_type, event_count FROM monthly_events_by_hazard", conn
    )
    afbs = pd.read_sql(
        "SELECT year, state, total_losses_m, total_claims, event_count FROM annual_financial_by_state", conn
    )
    sig = pd.read_sql(
        "SELECT year, state, event_name, hazard_type, insured_losses_m, claims_count FROM significant_events", conn
    )
    haz = pd.read_sql("SELECT hazard_type, total_events FROM events_by_hazard_type", conn)
    storms = pd.read_sql(
        """SELECT event_date, state, latitude, longitude, hazard_type, nearest_town, description
           FROM storm_events WHERE latitude IS NOT NULL AND longitude IS NOT NULL""",
        conn,
    )
    fi = pd.read_sql(
        "SELECT year, event_name, hazard_type, state, insured_losses_m, claims_count FROM financial_impacts",
        conn,
    )
    conn.close()

    mbs["year_month"] = pd.to_datetime(mbs["year_month"] + "-01")
    mbh["year_month"] = pd.to_datetime(mbh["year_month"] + "-01")
    storms["event_date"] = pd.to_datetime(storms["event_date"])

    # ICA state values come in mixed case from the spreadsheet; normalise to UPPER
    afbs["state"] = afbs["state"].str.upper()
    sig["state"]  = sig["state"].str.upper()
    fi["state"]   = fi["state"].str.upper()

    return dict(mbs=mbs, mbh=mbh, afbs=afbs, sig=sig, haz=haz, storms=storms, fi=fi)


DATA = load_all_data()

# Compute once at startup — callbacks read these rather than re-querying
KPI_TOTAL_EVENTS = 8_639
KPI_STATES       = DATA["mbs"]["state"].nunique()
KPI_TOP_HAZARD   = (
    DATA["haz"].sort_values("total_events", ascending=False).iloc[0]["hazard_type"].title()
)
KPI_TOTAL_LOSSES_B  = round(DATA["fi"]["insured_losses_m"].sum() / 1_000, 1)
KPI_TOTAL_CLAIMS    = DATA["fi"]["claims_count"].sum()
KPI_NUM_CATS        = len(DATA["fi"])

YEAR_MIN = int(DATA["fi"]["year"].min())
YEAR_MAX = int(DATA["fi"]["year"].max())

STATE_OPTIONS = [{"label": "All States", "value": "All"}] + [
    {"label": s, "value": s} for s in STATE_ORDER
]


def kpi_card(label: str, value: str, subtitle: str = "", accent: str = ACCENT) -> html.Div:
    return html.Div(
        [
            html.P(label, style={
                "color": TEXT_MUTED, "fontSize": "11px", "fontWeight": "600",
                "letterSpacing": "0.08em", "textTransform": "uppercase",
                "margin": "0 0 6px 0",
            }),
            html.H2(value, style={
                "color": accent, "fontSize": "2rem", "fontWeight": "700",
                "margin": "0", "lineHeight": "1.1", "fontFamily": "'Segoe UI', sans-serif",
            }),
            html.P(subtitle, style={
                "color": TEXT_MUTED, "fontSize": "12px", "margin": "5px 0 0 0",
            }),
        ],
        style={
            "backgroundColor": CARD_BG,
            "border": f"1px solid {BORDER}",
            "borderLeft": f"3px solid {accent}",
            "borderRadius": "6px",
            "padding": "18px 22px",
            "flex": "1",
            "minWidth": "160px",
            "margin": "0 8px 0 0",
        },
    )


def section_card(children, style_extra=None) -> html.Div:
    style = {
        "backgroundColor": CARD_BG,
        "border": f"1px solid {BORDER}",
        "borderRadius": "8px",
        "padding": "20px",
        "marginBottom": "16px",
    }
    if style_extra:
        style.update(style_extra)
    return html.Div(children, style=style)


def filter_label(text: str) -> html.P:
    return html.P(text, style={
        "color": TEXT_MUTED, "fontSize": "11px", "fontWeight": "600",
        "letterSpacing": "0.07em", "textTransform": "uppercase",
        "margin": "0 0 6px 0",
    })


TABLE_STYLE = {
    "backgroundColor": CARD_BG,
    "color": TEXT,
    "border": "none",
    "fontFamily": "'Segoe UI', sans-serif",
    "fontSize": "13px",
}
TABLE_HEADER_STYLE = {
    "backgroundColor": BG,
    "color": ACCENT,
    "fontWeight": "600",
    "border": f"1px solid {BORDER}",
    "textAlign": "left",
}
TABLE_CELL_STYLE = {
    "backgroundColor": CARD_BG,
    "color": TEXT,
    "border": f"1px solid {BORDER}",
    "textAlign": "left",
    "padding": "10px 14px",
    "whiteSpace": "normal",
    "height": "auto",
}
TABLE_FILTER_STYLE = {
    "backgroundColor": BG,
    "color": TEXT,
    "border": f"1px solid {BORDER}",
}


def build_monthly_bar(state_filter: str = "All") -> go.Figure:
    """Stacked bar: monthly events. All states when unfiltered; hazard breakdown when state selected."""
    fig = go.Figure()
    df  = DATA["mbs"].copy()

    if state_filter == "All":
        # Aggregate by year_month + state
        df = df.groupby(["year_month", "state"], as_index=False)["event_count"].sum()
        for state in STATE_ORDER:
            sub = df[df["state"] == state].sort_values("year_month")
            color = STATE_COLORS.get(state, "#aaaaaa")
            fig.add_trace(go.Bar(
                x=sub["year_month"],
                y=sub["event_count"],
                name=state,
                marker_color=color,
                hovertemplate=f"<b>{state}</b><br>%{{x|%b %Y}}<br>%{{y}} events<extra></extra>",
            ))
        title = "Monthly Storm Events by State"
    else:
        df = df[df["state"] == state_filter]
        df = df.groupby(["year_month", "hazard_type"], as_index=False)["event_count"].sum()
        for hazard in ["rain", "wind", "hail", "tornado", "lightning"]:
            sub = df[df["hazard_type"] == hazard].sort_values("year_month")
            if sub.empty:
                continue
            fig.add_trace(go.Bar(
                x=sub["year_month"],
                y=sub["event_count"],
                name=hazard.title(),
                marker_color=HAZARD_COLORS.get(hazard, "#aaaaaa"),
                hovertemplate=f"<b>{hazard.title()}</b><br>%{{x|%b %Y}}<br>%{{y}} events<extra></extra>",
            ))
        title = f"Monthly Storm Events — {state_filter} (by Hazard Type)"

    apply_dark_theme(fig, height=360, title=title)
    fig.update_layout(
        barmode="stack",
        xaxis_type="date",
        xaxis_title="",
        yaxis_title="Events",
    )
    return fig


def build_monthly_area(state_filter: str = "All") -> go.Figure:
    """Stacked area: monthly events by hazard type (all states or filtered state)."""
    fig = go.Figure()

    if state_filter == "All":
        df = DATA["mbh"].copy()
        df = df.groupby(["year_month", "hazard_type"], as_index=False)["event_count"].sum()
        title = "Monthly Events by Hazard Type (All States)"
    else:
        df = DATA["mbs"][DATA["mbs"]["state"] == state_filter].copy()
        df = df.groupby(["year_month", "hazard_type"], as_index=False)["event_count"].sum()
        title = f"Monthly Events by Hazard Type — {state_filter}"

    for hazard in ["rain", "wind", "hail", "tornado", "lightning"]:
        sub = df[df["hazard_type"] == hazard].sort_values("year_month")
        if sub.empty:
            continue
        color = HAZARD_COLORS[hazard]
        fig.add_trace(go.Scatter(
            x=sub["year_month"],
            y=sub["event_count"],
            name=hazard.title(),
            stackgroup="one",
            mode="lines",
            line=dict(color=color, width=1),
            fillcolor=hex_to_rgba(color, 0.70),
            hovertemplate=f"<b>{hazard.title()}</b><br>%{{x|%b %Y}}<br>%{{y}} events<extra></extra>",
        ))

    apply_dark_theme(fig, height=320, title=title)
    fig.update_layout(xaxis_type="date", xaxis_title="", yaxis_title="Events")
    return fig


def build_annual_financial(year_range: tuple) -> go.Figure:
    """Grouped bar: annual insured losses by state for the selected year range."""
    df = DATA["afbs"].copy()
    df = df[df["year"].between(year_range[0], year_range[1])]

    years_in_range = sorted(df["year"].unique())
    fig = go.Figure()

    for state in sorted(df["state"].unique()):
        sub = df[df["state"] == state].sort_values("year")
        color = STATE_COLORS.get(state.upper(), "#6b7280")
        fig.add_trace(go.Bar(
            x=sub["year"],
            y=sub["total_losses_m"],
            name=state,
            marker_color=color,
            hovertemplate=(
                f"<b>{state}</b><br>Year: %{{x}}<br>"
                "Insured Losses: $%{y:,.0f}M<br>"
                "<extra></extra>"
            ),
        ))

    apply_dark_theme(fig, height=380, title="Annual Insured Losses by State (A$M)")
    fig.update_layout(
        barmode="group",
        xaxis=dict(tickmode="array", tickvals=years_in_range, title=""),
        yaxis_title="Insured Losses (A$M)",
    )
    return fig


def significant_events_records(year_range: tuple) -> list[dict]:
    df = DATA["sig"].copy()
    df = df[df["year"].between(year_range[0], year_range[1])]
    df = df.sort_values(["year", "insured_losses_m"], ascending=[False, False])
    df["insured_losses_m"] = df["insured_losses_m"].apply(
        lambda x: f"${x:,.1f}M" if pd.notna(x) else "N/A"
    )
    df["claims_count"] = df["claims_count"].apply(
        lambda x: f"{int(x):,}" if pd.notna(x) else "N/A"
    )
    return df.rename(columns={
        "year": "Year", "state": "State", "event_name": "Event Name",
        "hazard_type": "Hazard", "insured_losses_m": "Insured Losses",
        "claims_count": "Claims",
    }).to_dict("records")


def build_donut() -> go.Figure:
    """Donut chart: total events by hazard type."""
    df = DATA["haz"].sort_values("total_events", ascending=False)
    colors = [HAZARD_COLORS.get(h, "#aaaaaa") for h in df["hazard_type"]]
    total  = int(df["total_events"].sum())

    fig = go.Figure(go.Pie(
        labels=[h.title() for h in df["hazard_type"]],
        values=df["total_events"],
        hole=0.58,
        marker_colors=colors,
        textinfo="label+percent",
        textfont=dict(color=TEXT, size=11),
        hovertemplate="<b>%{label}</b><br>Events: %{value:,}<br>%{percent}<extra></extra>",
        sort=False,
    ))
    fig.add_annotation(
        text=f"<b>{total:,}</b><br><span style='font-size:11px'>Total<br>Events</span>",
        x=0.5, y=0.5,
        font=dict(size=19, color=ACCENT),
        showarrow=False,
        align="center",
    )

    apply_dark_theme(fig, height=360, title="Storm Events by Hazard Type (2005–2021)",
                     legend_horiz=False)
    fig.update_layout(margin=dict(l=20, r=130, t=55, b=20))
    return fig


def build_map() -> go.Figure:
    """Scatter geo map of all storm event locations, coloured by hazard type."""
    df  = DATA["storms"]
    fig = go.Figure()

    for hazard in ["rain", "wind", "hail", "tornado", "lightning"]:
        sub = df[df["hazard_type"] == hazard]
        if sub.empty:
            continue
        color = HAZARD_COLORS[hazard]
        fig.add_trace(go.Scattergeo(
            lat=sub["latitude"],
            lon=sub["longitude"],
            name=hazard.title(),
            mode="markers",
            marker=dict(size=4, color=color, opacity=0.55, line=dict(width=0)),
            text=sub["nearest_town"].fillna("") + " (" + sub["state"].fillna("") + ")",
            customdata=sub[["event_date", "description"]].values,
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Date: %{customdata[0]}<br>"
                "%{customdata[1]}"
                "<extra></extra>"
            ),
        ))

    fig.update_geos(
        fitbounds="locations",
        bgcolor=BG,
        landcolor="#12294a",
        oceancolor="#061223",
        lakecolor="#061223",
        showland=True,
        showocean=True,
        showlakes=True,
        showcoastlines=True,
        coastlinecolor="#2a4a6e",
        showrivers=False,
        showframe=False,
        resolution=50,
        showsubunits=True,
        subunitcolor=BORDER,
    )

    apply_dark_theme(fig, height=420, title="Storm Event Locations — Australia (2005–2021)",
                     legend_horiz=False)
    fig.update_layout(
        margin=dict(l=0, r=130, t=55, b=0),
        geo=dict(bgcolor=BG),
    )
    return fig


def monthly_summary_records() -> tuple[list[dict], list[dict]]:
    """Build month-by-month summary table for Page 3."""
    df  = DATA["mbh"].copy()
    df["year_month_str"] = df["year_month"].dt.strftime("%Y-%m")

    pivot = df.pivot_table(
        index="year_month_str",
        columns="hazard_type",
        values="event_count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    pivot.columns.name = None

    num_cols = [c for c in pivot.columns if c != "year_month_str"]
    pivot["Total"] = pivot[num_cols].sum(axis=1)
    pivot = pivot.sort_values("year_month_str", ascending=False)

    rename_map = {
        "year_month_str": "Month", "rain": "Rain", "wind": "Wind",
        "hail": "Hail", "tornado": "Tornado", "lightning": "Lightning",
    }
    pivot = pivot.rename(columns=rename_map)
    ordered = [c for c in ["Month", "Rain", "Wind", "Hail", "Tornado", "Lightning", "Total"]
               if c in pivot.columns]
    pivot = pivot[ordered]

    columns = [{"name": c, "id": c} for c in ordered]
    return pivot.to_dict("records"), columns


def page1_layout() -> html.Div:
    return html.Div([
        # KPI row
        html.Div([
            kpi_card("Total Storm Events", f"{KPI_TOTAL_EVENTS:,}", "2005 – 2021"),
            kpi_card("States Covered", str(KPI_STATES), "All territories", accent=ACCENT2),
            kpi_card("Most Common Hazard", KPI_TOP_HAZARD, "3,707 rain events",
                     accent=HAZARD_COLORS["rain"]),
        ], style={"display": "flex", "marginBottom": "16px"}),

        # Filter row
        section_card([
            html.Div([
                filter_label("Filter by State"),
                dcc.Dropdown(
                    id="p1-state-filter",
                    options=STATE_OPTIONS,
                    value="All",
                    clearable=False,
                    style={
                        "backgroundColor": BG, "color": BG, "borderColor": BORDER,
                        "fontSize": "13px",
                    },
                ),
            ], style={"maxWidth": "280px"}),
        ], style_extra={"padding": "14px 20px", "marginBottom": "16px"}),

        # Stacked bar chart
        section_card([
            dcc.Graph(id="p1-monthly-bar", config=CHART_CONFIG,
                      figure=build_monthly_bar()),
        ]),

        # Stacked area chart
        section_card([
            dcc.Graph(id="p1-monthly-area", config=CHART_CONFIG,
                      figure=build_monthly_area()),
        ]),
    ])


def page2_layout() -> html.Div:
    default_records = significant_events_records((YEAR_MIN, YEAR_MAX))
    sig_cols = [{"name": c, "id": c} for c in
                ["Year", "State", "Event Name", "Hazard", "Insured Losses", "Claims"]]

    return html.Div([
        # KPI row
        html.Div([
            kpi_card("Total Insured Losses", f"${KPI_TOTAL_LOSSES_B}B",
                     "2005 – 2025 · All perils", accent="#ef4444"),
            kpi_card("Total Insurance Claims", f"{KPI_TOTAL_CLAIMS:,.0f}",
                     "Across all declared catastrophes", accent="#f59e0b"),
            kpi_card("Catastrophes Declared", str(KPI_NUM_CATS),
                     "ICA declared events", accent=ACCENT2),
        ], style={"display": "flex", "marginBottom": "16px"}),

        # Year range filter
        section_card([
            filter_label("Filter by Year Range"),
            dcc.RangeSlider(
                id="p2-year-slider",
                min=YEAR_MIN, max=YEAR_MAX,
                value=[YEAR_MIN, YEAR_MAX],
                step=1,
                marks={yr: {"label": str(yr), "style": {"color": TEXT_MUTED, "fontSize": "12px"}}
                       for yr in range(YEAR_MIN, YEAR_MAX + 1)},
                tooltip={"placement": "bottom", "always_visible": False},
            ),
        ], style_extra={"padding": "16px 24px 20px", "marginBottom": "16px"}),

        # Grouped bar: annual losses
        section_card([
            dcc.Graph(id="p2-annual-bar", config=CHART_CONFIG,
                      figure=build_annual_financial((YEAR_MIN, YEAR_MAX))),
        ]),

        # Significant events table
        section_card([
            html.H3("Most Significant Event per State per Year", style={
                "color": ACCENT, "fontSize": "14px", "margin": "0 0 14px 0",
                "fontFamily": "'Segoe UI', sans-serif", "fontWeight": "600",
            }),
            dash_table.DataTable(
                id="p2-sig-table",
                columns=sig_cols,
                data=default_records,
                sort_action="native",
                page_action="native",
                page_size=10,
                style_table={"overflowX": "auto"},
                style_as_list_view=False,
                style_header=TABLE_HEADER_STYLE,
                style_cell=TABLE_CELL_STYLE,
                style_data_conditional=[{
                    "if": {"row_index": "odd"},
                    "backgroundColor": "#0d1c30",
                }],
                style_filter=TABLE_FILTER_STYLE,
            ),
        ]),
    ])


def page3_layout() -> html.Div:
    table_records, table_cols = monthly_summary_records()

    return html.Div([
        # Donut + Map side by side
        html.Div([
            section_card([
                dcc.Graph(id="p3-donut", config=CHART_CONFIG, figure=build_donut()),
            ], style_extra={"flex": "0 0 420px", "marginRight": "12px", "marginBottom": "0"}),

            section_card([
                dcc.Graph(id="p3-map", config=CHART_CONFIG, figure=build_map()),
            ], style_extra={"flex": "1", "marginBottom": "0"}),
        ], style={"display": "flex", "alignItems": "stretch", "marginBottom": "16px"}),

        # Monthly summary table
        section_card([
            html.H3("Month-by-Month Event Summary", style={
                "color": ACCENT, "fontSize": "14px", "margin": "0 0 14px 0",
                "fontFamily": "'Segoe UI', sans-serif", "fontWeight": "600",
            }),
            html.P("Sort any column · Use the filter row to search", style={
                "color": TEXT_MUTED, "fontSize": "12px", "margin": "0 0 12px 0",
            }),
            dash_table.DataTable(
                id="p3-monthly-table",
                columns=table_cols,
                data=table_records,
                sort_action="native",
                filter_action="native",
                page_action="native",
                page_size=20,
                style_table={"overflowX": "auto"},
                style_header=TABLE_HEADER_STYLE,
                style_cell={**TABLE_CELL_STYLE, "textAlign": "center", "minWidth": "70px"},
                style_cell_conditional=[{
                    "if": {"column_id": "Month"},
                    "textAlign": "left", "minWidth": "90px",
                }],
                style_data_conditional=[{
                    "if": {"row_index": "odd"},
                    "backgroundColor": "#0d1c30",
                }],
                style_filter=TABLE_FILTER_STYLE,
            ),
        ]),
    ])


app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    title="Australian Extreme Weather Dashboard",
)

TAB_STYLE = {
    "backgroundColor": CARD_BG,
    "color": TEXT_MUTED,
    "border": f"1px solid {BORDER}",
    "borderBottom": "none",
    "padding": "10px 22px",
    "fontFamily": "'Segoe UI', sans-serif",
    "fontSize": "13px",
    "fontWeight": "500",
    "letterSpacing": "0.02em",
}
TAB_SELECTED_STYLE = {
    **TAB_STYLE,
    "backgroundColor": BG,
    "color": ACCENT,
    "borderTop": f"2px solid {ACCENT}",
    "fontWeight": "600",
}
TABS_CONTAINER_STYLE = {
    "backgroundColor": CARD_BG,
    "borderBottom": f"1px solid {BORDER}",
}

app.layout = html.Div(
    [
        html.Div(
            [
                html.Div([
                    html.H1(
                        "🌩  Australian Extreme Weather Events",
                        style={
                            "color": TEXT, "margin": "0 0 4px 0",
                            "fontFamily": "'Segoe UI', sans-serif",
                            "fontSize": "1.5rem", "fontWeight": "700",
                        },
                    ),
                    html.P(
                        "BOM Severe Storms Archive 2005–2021  ·  ICA Historical Catastrophe List 2005–2025",
                        style={"color": TEXT_MUTED, "margin": 0, "fontSize": "13px"},
                    ),
                ]),
                html.Div([
                    html.Span("LIVE DB", style={
                        "color": ACCENT, "fontSize": "11px", "fontWeight": "700",
                        "letterSpacing": "0.1em", "border": f"1px solid {ACCENT}",
                        "padding": "3px 8px", "borderRadius": "3px",
                    }),
                ]),
            ],
            style={
                "display": "flex", "alignItems": "center", "justifyContent": "space-between",
                "backgroundColor": CARD_BG,
                "padding": "18px 28px",
                "borderBottom": f"1px solid {BORDER}",
            },
        ),

        dcc.Tabs(
            id="main-tabs",
            value="tab-1",
            style=TABS_CONTAINER_STYLE,
            children=[
                dcc.Tab(label="📊  Monthly Events Overview", value="tab-1",
                        style=TAB_STYLE, selected_style=TAB_SELECTED_STYLE),
                dcc.Tab(label="💰  Financial Impact",         value="tab-2",
                        style=TAB_STYLE, selected_style=TAB_SELECTED_STYLE),
                dcc.Tab(label="🗺  Hazard Analysis & Map",    value="tab-3",
                        style=TAB_STYLE, selected_style=TAB_SELECTED_STYLE),
            ],
        ),

        html.Div(id="tab-content", style={"padding": "20px 24px"}),
    ],
    style={"backgroundColor": BG, "minHeight": "100vh"},
)


@app.callback(Output("tab-content", "children"), Input("main-tabs", "value"))
def render_tab(tab: str) -> html.Div:
    if tab == "tab-1":
        return page1_layout()
    elif tab == "tab-2":
        return page2_layout()
    else:
        return page3_layout()


@app.callback(
    Output("p1-monthly-bar",  "figure"),
    Output("p1-monthly-area", "figure"),
    Input("p1-state-filter",  "value"),
)
def update_page1_charts(state: str):
    return build_monthly_bar(state), build_monthly_area(state)


@app.callback(
    Output("p2-annual-bar", "figure"),
    Output("p2-sig-table",  "data"),
    Input("p2-year-slider", "value"),
)
def update_page2(year_range: list):
    yr = tuple(year_range)
    return build_annual_financial(yr), significant_events_records(yr)


if __name__ == "__main__":
    def open_browser():
        time.sleep(1.5)
        webbrowser.open("http://localhost:8050")

    print("\n" + "═" * 55)
    print("  Australian Extreme Weather Dashboard")
    print("  http://localhost:8050")
    print("  Press Ctrl-C to stop")
    print("═" * 55 + "\n")

    threading.Thread(target=open_browser, daemon=True).start()
    app.run(debug=False, host="0.0.0.0", port=8050)
