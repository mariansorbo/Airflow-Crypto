#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════╗
║         CRYPTO MARKET DASHBOARD                      ║
║         Tkinter + Matplotlib + Seaborn               ║
║         Fuente: crypto_data (Airflow pipeline)       ║
╚══════════════════════════════════════════════════════╝

Ejecutar:
    python dashboard.py
"""

import sys
import threading
from datetime import datetime
from typing import Optional, List, Dict

import tkinter as tk
from tkinter import ttk, messagebox

import psycopg2
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import seaborn as sns

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DB = dict(host="localhost", port=5432, dbname="crypto_data",
          user="airflow", password="airflow")

AUTO_REFRESH_MS = 5 * 60 * 1000   # refresca sola cada 5 minutos

# ─────────────────────────────────────────────────────────────────────────────
# PALETA / TEMA
# ─────────────────────────────────────────────────────────────────────────────
C = {
    "bg":      "#0d0d1a",
    "bg2":     "#12122a",
    "bg3":     "#1a1a35",
    "panel":   "#1e1e3a",
    "border":  "#2a2a4a",
    "accent":  "#7c6af7",
    "green":   "#2ecc71",
    "red":     "#e74c3c",
    "yellow":  "#f1c40f",
    "text":    "#e8e8f0",
    "text2":   "#8888aa",
    "white":   "#ffffff",
}

PALETTE = [
    "#7c6af7", "#2ecc71", "#e74c3c", "#f1c40f",
    "#3498db", "#e67e22", "#1abc9c", "#9b59b6",
    "#e91e63", "#00bcd4", "#8bc34a", "#ff5722",
    "#00b894", "#a29bfe", "#fd79a8", "#fdcb6e",
]

matplotlib.rcParams.update({
    "figure.facecolor":  C["bg2"],
    "axes.facecolor":    C["bg3"],
    "axes.edgecolor":    C["border"],
    "axes.labelcolor":   C["text2"],
    "xtick.color":       C["text2"],
    "ytick.color":       C["text2"],
    "text.color":        C["text"],
    "grid.color":        C["border"],
    "grid.linestyle":    "--",
    "grid.alpha":        0.4,
    "legend.facecolor":  C["panel"],
    "legend.edgecolor":  C["border"],
    "legend.fontsize":   8,
    "font.family":       "sans-serif",
    "axes.titlepad":     10,
    "axes.titlesize":    11,
    "axes.labelsize":    9,
    "xtick.labelsize":   8,
    "ytick.labelsize":   8,
})

# ─────────────────────────────────────────────────────────────────────────────
# FORMATTERS
# ─────────────────────────────────────────────────────────────────────────────
def fmt_usd(v) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    if v >= 1e12: return f"${v/1e12:.2f}T"
    if v >= 1e9:  return f"${v/1e9:.2f}B"
    if v >= 1e6:  return f"${v/1e6:.2f}M"
    if v >= 1e3:  return f"${v/1e3:.2f}K"
    return f"${v:.4f}"

def fmt_price(v) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    if v >= 10000: return f"${v:,.0f}"
    if v >= 1000:  return f"${v:,.2f}"
    if v >= 1:     return f"${v:.4f}"
    return f"${v:.8f}"

def pct(open_p, close_p) -> Optional[float]:
    if open_p and close_p and open_p != 0:
        return (close_p - open_p) / open_p * 100
    return None


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
class CryptoDashboard:

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("◈  Crypto Market Dashboard")
        self.root.geometry("1440x880")
        self.root.configure(bg=C["bg"])
        self.root.minsize(1100, 700)

        self.conn: Optional[psycopg2.extensions.connection] = None
        self.df_summary  = pd.DataFrame()
        self.df_history  = pd.DataFrame()
        self.df_daily_ts = pd.DataFrame()   # snapshots last 7 days, all coins

        self._status_var  = tk.StringVar(value="Conectando…")
        self._refresh_job = None
        self._all_coins:  List[str] = []
        self._coin_labels: Dict[str, str] = {}
        self._listbox_coins: List[str] = []

        self._tree_sort_col = None
        self._tree_sort_rev = False

        self._setup_styles()
        self._build_ui()
        self.root.after(200, self._initial_load)

    # =========================================================================
    # ESTILOS TTK
    # =========================================================================
    def _setup_styles(self):
        s = ttk.Style()
        s.theme_use("clam")
        s.configure(".", background=C["bg"], foreground=C["text"],
                    fieldbackground=C["bg2"], font=("Segoe UI", 10))

        s.configure("TNotebook", background=C["bg"], tabmargins=[0, 0, 0, 0])
        s.configure("TNotebook.Tab", background=C["bg2"], foreground=C["text2"],
                    padding=[18, 9], font=("Segoe UI", 10))
        s.map("TNotebook.Tab",
              background=[("selected", C["panel"])],
              foreground=[("selected", C["white"])])

        s.configure("Treeview", background=C["bg2"], foreground=C["text"],
                    fieldbackground=C["bg2"], rowheight=30, font=("Segoe UI", 9))
        s.configure("Treeview.Heading", background=C["panel"],
                    foreground=C["accent"], relief="flat",
                    font=("Segoe UI", 9, "bold"))
        s.map("Treeview", background=[("selected", C["accent"])])

        s.configure("TScrollbar", background=C["bg3"],
                    troughcolor=C["bg"], arrowcolor=C["text2"])
        s.configure("TFrame",  background=C["bg"])
        s.configure("TLabel",  background=C["bg"],    foreground=C["text"])
        s.configure("TButton", background=C["panel"], foreground=C["text"],
                    relief="flat", padding=[14, 7])
        s.map("TButton", background=[("active", C["accent"])],
              foreground=[("active", C["white"])])
        s.configure("TEntry",  fieldbackground=C["bg3"],
                    foreground=C["text"], insertcolor=C["text"])

    # =========================================================================
    # LAYOUT PRINCIPAL
    # =========================================================================
    def _build_ui(self):
        # ── HEADER ──────────────────────────────────────────────────────────
        hdr = tk.Frame(self.root, bg=C["bg2"], height=62)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        tk.Label(hdr, text="◈  CRYPTO MARKET DASHBOARD",
                 font=("Segoe UI", 17, "bold"),
                 bg=C["bg2"], fg=C["accent"]).pack(side="left", padx=22, pady=14)

        right = tk.Frame(hdr, bg=C["bg2"])
        right.pack(side="right", padx=20)
        ttk.Button(right, text="⟳  Refresh", command=self._refresh).pack(side="right", padx=(8, 0))
        tk.Label(right, textvariable=self._status_var,
                 font=("Segoe UI", 9), bg=C["bg2"], fg=C["text2"]).pack(side="right")

        # ── NOTEBOOK ────────────────────────────────────────────────────────
        self.nb = ttk.Notebook(self.root)
        self.nb.pack(fill="both", expand=True)

        self.tab_summary   = ttk.Frame(self.nb)
        self.tab_history   = ttk.Frame(self.nb)
        self.tab_analytics = ttk.Frame(self.nb)
        self.tab_heatmap   = ttk.Frame(self.nb)

        self.nb.add(self.tab_summary,   text="  📊  Resumen Diario  ")
        self.nb.add(self.tab_history,   text="  📈  Evolución de Precio  ")
        self.nb.add(self.tab_analytics, text="  🔬  Análisis de Mercado  ")
        self.nb.add(self.tab_heatmap,   text="  🌡️  Heatmap Histórico  ")

        self._build_summary_tab(self.tab_summary)
        self._build_history_tab(self.tab_history)
        self._build_analytics_tab(self.tab_analytics)
        self._build_heatmap_tab(self.tab_heatmap)

    # =========================================================================
    # TAB 1 — RESUMEN DIARIO
    # =========================================================================
    def _build_summary_tab(self, parent):
        # ── STAT CARDS ──────────────────────────────────────────────────────
        cards_outer = tk.Frame(parent, bg=C["bg"])
        cards_outer.pack(fill="x", padx=14, pady=(14, 8))

        self._kpi = {}
        kpis = [
            ("Total Coins",       "🪙"),
            ("BTC Price",         "₿"),
            ("Total Market Cap",  "📦"),
            ("Total Vol 24h",     "💧"),
            ("Snapshots Hoy",     "📸"),
        ]
        for label, icon in kpis:
            card = tk.Frame(cards_outer, bg=C["panel"], padx=14, pady=10)
            card.pack(side="left", expand=True, fill="both", padx=5)
            tk.Label(card, text=f"{icon}  {label}",
                     font=("Segoe UI", 8), bg=C["panel"], fg=C["text2"]).pack(anchor="w")
            var = tk.StringVar(value="—")
            self._kpi[label] = var
            tk.Label(card, textvariable=var,
                     font=("Segoe UI", 14, "bold"), bg=C["panel"], fg=C["text"]).pack(anchor="w")

        # ── TREEVIEW ────────────────────────────────────────────────────────
        tf = tk.Frame(parent, bg=C["bg"])
        tf.pack(fill="both", expand=True, padx=14, pady=(0, 14))

        cols = ["#", "Nombre", "Symbol", "Precio", "Mkt Cap", "Vol 24h",
                "Open", "High", "Low", "Close", "Cambio %", "Snaps"]
        self.tree = ttk.Treeview(tf, columns=cols, show="headings", selectmode="browse")

        widths  = [36, 155, 68, 118, 140, 130, 110, 110, 110, 110, 80, 55]
        anchors = ["c",  "w", "c",  "e",  "e",  "e",  "e",  "e",  "e",  "e", "e", "c"]
        for col, w, a in zip(cols, widths, anchors):
            self.tree.heading(col, text=col,
                              command=lambda c=col: self._sort_tree(c))
            self.tree.column(col, width=w, anchor=a, minwidth=36)

        self.tree.tag_configure("up",    foreground=C["green"])
        self.tree.tag_configure("down",  foreground=C["red"])
        self.tree.tag_configure("flat",  foreground=C["text"])
        self.tree.tag_configure("odd",   background="#14142a")
        self.tree.tag_configure("even",  background=C["bg2"])

        vsb = ttk.Scrollbar(tf, orient="vertical",   command=self.tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)

    # =========================================================================
    # TAB 2 — EVOLUCIÓN DE PRECIO
    # =========================================================================
    def _build_history_tab(self, parent):
        # Panel izquierdo — selector de monedas
        left = tk.Frame(parent, bg=C["bg2"], width=210)
        left.pack(side="left", fill="y", padx=(12, 0), pady=12)
        left.pack_propagate(False)

        tk.Label(left, text="Seleccionar monedas",
                 font=("Segoe UI", 10, "bold"),
                 bg=C["bg2"], fg=C["accent"]).pack(pady=(12, 6), padx=10, anchor="w")

        # Filtro de búsqueda
        self._coin_filter = tk.StringVar()
        self._coin_filter.trace_add("write", self._filter_coin_list)
        ttk.Entry(left, textvariable=self._coin_filter).pack(fill="x", padx=8, pady=(0, 6))

        # Listbox multi-select
        lf = tk.Frame(left, bg=C["bg2"])
        lf.pack(fill="both", expand=True, padx=8)

        self.coin_listbox = tk.Listbox(
            lf, selectmode="multiple", bg=C["bg3"], fg=C["text"],
            selectbackground=C["accent"], selectforeground="#fff",
            activestyle="none", highlightthickness=0, relief="flat",
            font=("Segoe UI", 9), bd=0,
        )
        lb_sb = ttk.Scrollbar(lf, orient="vertical", command=self.coin_listbox.yview)
        self.coin_listbox.configure(yscrollcommand=lb_sb.set)
        self.coin_listbox.pack(side="left", fill="both", expand=True)
        lb_sb.pack(side="right", fill="y")

        # Modo de visualización
        tk.Label(left, text="Modo", font=("Segoe UI", 9),
                 bg=C["bg2"], fg=C["text2"]).pack(padx=10, anchor="w", pady=(8, 2))
        self._hist_mode = tk.StringVar(value="precio")
        for val, lbl in [("precio", "Precio absoluto"), ("pct", "% cambio normalizado")]:
            rb = tk.Radiobutton(left, text=lbl, variable=self._hist_mode, value=val,
                                bg=C["bg2"], fg=C["text"], selectcolor=C["bg3"],
                                activebackground=C["bg2"], font=("Segoe UI", 8))
            rb.pack(anchor="w", padx=10)

        btn_f = tk.Frame(left, bg=C["bg2"])
        btn_f.pack(fill="x", padx=8, pady=10)
        ttk.Button(btn_f, text="📊  Graficar", command=self._plot_history).pack(fill="x")
        ttk.Button(btn_f, text="✕  Limpiar",  command=self._clear_history).pack(fill="x", pady=(5, 0))

        # Panel derecho — gráfico
        right = tk.Frame(parent, bg=C["bg"])
        right.pack(side="left", fill="both", expand=True, padx=12, pady=12)

        self.fig_hist = Figure(figsize=(10, 6), tight_layout=True)
        self.ax_hist  = self.fig_hist.add_subplot(111)
        self.ax_hist.set_title("Selecciona monedas y presiona  📊 Graficar",
                                color=C["text2"], fontsize=11)

        self.canvas_hist = FigureCanvasTkAgg(self.fig_hist, master=right)
        self.canvas_hist.get_tk_widget().pack(fill="both", expand=True)

    # =========================================================================
    # TAB 3 — ANÁLISIS DE MERCADO (2 × 2 charts)
    # =========================================================================
    def _build_analytics_tab(self, parent):
        self.fig_ana = Figure(figsize=(14, 8), tight_layout=True)
        self.canvas_ana = FigureCanvasTkAgg(self.fig_ana, master=parent)
        self.canvas_ana.get_tk_widget().pack(fill="both", expand=True, padx=8, pady=8)

    # =========================================================================
    # TAB 4 — HEATMAP HISTÓRICO
    # =========================================================================
    def _build_heatmap_tab(self, parent):
        self.fig_heat = Figure(figsize=(14, 8), tight_layout=True)
        self.canvas_heat = FigureCanvasTkAgg(self.fig_heat, master=parent)
        self.canvas_heat.get_tk_widget().pack(fill="both", expand=True, padx=8, pady=8)

    # =========================================================================
    # DATA LOADING
    # =========================================================================
    def _get_conn(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**DB)
        return self.conn

    def _initial_load(self):
        threading.Thread(target=self._load_data, daemon=True).start()

    def _refresh(self):
        self._status_var.set("Actualizando…")
        threading.Thread(target=self._load_data, daemon=True).start()

    def _load_data(self):
        try:
            conn = self._get_conn()

            # ── 1. Resumen diario de hoy ──────────────────────────────────
            df_sum = pd.read_sql("""
                SELECT
                    ds.date,
                    ds.coin_id,
                    COALESCE(cd.symbol, UPPER(ds.coin_id)) AS symbol,
                    COALESCE(cd.name,   ds.coin_id)        AS name,
                    ds.open_price_usd,
                    ds.high_price_usd,
                    ds.low_price_usd,
                    ds.close_price_usd,
                    ds.avg_volume_24h_usd,
                    ds.avg_market_cap_usd,
                    ds.snapshot_count,
                    ROW_NUMBER() OVER (
                        ORDER BY ds.avg_market_cap_usd DESC NULLS LAST
                    ) AS rank
                FROM coin_daily_summary ds
                LEFT JOIN coins_dim cd ON cd.coin_id = ds.coin_id
                WHERE ds.date = CURRENT_DATE
                ORDER BY ds.avg_market_cap_usd DESC NULLS LAST
            """, conn)

            # ── 2. Snapshots históricos — últimos 7 días ──────────────────
            df_hist = pd.read_sql("""
                SELECT snapshot_ts, coin_id, price_usd,
                       market_cap_usd, volume_24h_usd
                FROM coin_market_snapshots
                WHERE snapshot_ts >= NOW() - INTERVAL '7 days'
                ORDER BY snapshot_ts
            """, conn)

            # ── 3. Daily summary histórico para heatmap ───────────────────
            df_daily = pd.read_sql("""
                SELECT
                    ds.date,
                    ds.coin_id,
                    COALESCE(cd.symbol, UPPER(ds.coin_id)) AS symbol,
                    ds.open_price_usd,
                    ds.close_price_usd,
                    ds.avg_market_cap_usd
                FROM coin_daily_summary ds
                LEFT JOIN coins_dim cd ON cd.coin_id = ds.coin_id
                WHERE ds.date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY ds.date
            """, conn)

            self.df_summary  = df_sum
            self.df_history  = df_hist
            self.df_daily_ts = df_daily

            self.root.after(0, self._update_all)

        except Exception as exc:
            self.root.after(0, lambda e=exc: (
                self._status_var.set(f"Error: {e}"),
                messagebox.showerror("Error de conexión", str(e))
            ))

    # =========================================================================
    # UPDATE UI
    # =========================================================================
    def _update_all(self):
        self._update_summary_tab()
        self._update_coin_listbox()
        self._update_analytics_tab()
        self._update_heatmap_tab()

        now = datetime.now().strftime("%H:%M:%S")
        self._status_var.set(f"Última actualización: {now}")

        if self._refresh_job:
            self.root.after_cancel(self._refresh_job)
        self._refresh_job = self.root.after(AUTO_REFRESH_MS, self._refresh)

    # ── TAB 1 update ──────────────────────────────────────────────────────────
    def _update_summary_tab(self):
        df = self.df_summary
        if df.empty:
            return

        # KPI cards
        btc = df[df["coin_id"] == "bitcoin"]["close_price_usd"]
        self._kpi["Total Coins"].set(str(len(df)))
        self._kpi["BTC Price"].set(fmt_price(btc.values[0] if len(btc) else None))
        self._kpi["Total Market Cap"].set(fmt_usd(df["avg_market_cap_usd"].sum()))
        self._kpi["Total Vol 24h"].set(fmt_usd(df["avg_volume_24h_usd"].sum()))
        self._kpi["Snapshots Hoy"].set(str(int(df["snapshot_count"].max())))

        # Treeview
        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, r in df.iterrows():
            chg = pct(r["open_price_usd"], r["close_price_usd"])
            chg_str = f"{chg:+.2f}%" if chg is not None else "—"
            tag_dir = "up" if (chg or 0) > 0 else ("down" if (chg or 0) < 0 else "flat")
            tag_alt = "odd" if int(r["rank"]) % 2 else "even"

            self.tree.insert("", "end", tags=(tag_dir, tag_alt), values=(
                int(r["rank"]),
                r["name"],
                r["symbol"],
                fmt_price(r["close_price_usd"]),
                fmt_usd(r["avg_market_cap_usd"]),
                fmt_usd(r["avg_volume_24h_usd"]),
                fmt_price(r["open_price_usd"]),
                fmt_price(r["high_price_usd"]),
                fmt_price(r["low_price_usd"]),
                fmt_price(r["close_price_usd"]),
                chg_str,
                int(r["snapshot_count"]),
            ))

    # ── TAB 2 — coin listbox ──────────────────────────────────────────────────
    def _update_coin_listbox(self):
        df = self.df_summary
        if df.empty:
            return
        self._all_coins = list(df.sort_values("avg_market_cap_usd", ascending=False)["coin_id"])
        self._coin_labels = {
            r["coin_id"]: f"{r['symbol']:6s}  {r['name']}"
            for _, r in df.iterrows()
        }
        self._populate_listbox(self._all_coins)

    def _populate_listbox(self, coins: List[str]):
        self.coin_listbox.delete(0, "end")
        for cid in coins:
            self.coin_listbox.insert("end", self._coin_labels.get(cid, cid))
        self._listbox_coins = coins

    def _filter_coin_list(self, *_):
        q = self._coin_filter.get().lower()
        if not self._all_coins:
            return
        filtered = [c for c in self._all_coins
                    if q in c.lower() or q in self._coin_labels.get(c, "").lower()]
        self._populate_listbox(filtered)

    def _plot_history(self):
        sel = self.coin_listbox.curselection()
        if not sel:
            messagebox.showinfo("Info", "Selecciona al menos una moneda de la lista.")
            return

        ids = [self._listbox_coins[i] for i in sel]
        df  = self.df_history[self.df_history["coin_id"].isin(ids)].copy()

        if df.empty:
            messagebox.showinfo(
                "Sin datos históricos",
                "Todavía no hay snapshots suficientes.\n"
                "El pipeline corre cada 30 min — espera unos ciclos y vuelve a intentarlo."
            )
            return

        mode = self._hist_mode.get()

        self.ax_hist.clear()
        self.ax_hist.grid(True, alpha=0.3)

        for idx, coin_id in enumerate(ids):
            sub = df[df["coin_id"] == coin_id].sort_values("snapshot_ts").copy()
            if sub.empty:
                continue

            ts    = pd.to_datetime(sub["snapshot_ts"])
            color = PALETTE[idx % len(PALETTE)]
            label = self._coin_labels.get(coin_id, coin_id)

            if mode == "pct" and len(ids) > 1:
                first = sub["price_usd"].iloc[0]
                y = (sub["price_usd"] - first) / first * 100 if first else sub["price_usd"]
                ylabel = "Cambio % vs primer snapshot"
            else:
                y = sub["price_usd"]
                ylabel = "Precio USD"

            self.ax_hist.plot(ts, y, label=label, color=color,
                              linewidth=2, marker="o", markersize=4, zorder=3)
            self.ax_hist.fill_between(ts, y, alpha=0.07, color=color)

        title_str = ", ".join(ids[:4]) + ("  …" if len(ids) > 4 else "")
        self.ax_hist.set_title(f"Evolución  —  {title_str}", color=C["text"])
        self.ax_hist.set_xlabel("Tiempo (UTC)")
        self.ax_hist.set_ylabel(ylabel)
        self.ax_hist.legend(loc="best")
        self.ax_hist.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
        self.ax_hist.xaxis.set_major_locator(mdates.AutoDateLocator())
        self.fig_hist.autofmt_xdate(rotation=30)
        self.canvas_hist.draw()

    def _clear_history(self):
        self.coin_listbox.selection_clear(0, "end")
        self.ax_hist.clear()
        self.ax_hist.set_title("Selecciona monedas y presiona  📊 Graficar",
                                color=C["text2"])
        self.canvas_hist.draw()

    # ── TAB 3 — Analytics (2×2) ───────────────────────────────────────────────
    def _update_analytics_tab(self):
        df = self.df_summary
        if df.empty:
            return

        self.fig_ana.clear()
        ax1 = self.fig_ana.add_subplot(2, 2, 1)
        ax2 = self.fig_ana.add_subplot(2, 2, 2)
        ax3 = self.fig_ana.add_subplot(2, 2, 3)
        ax4 = self.fig_ana.add_subplot(2, 2, 4)

        # ── 1. Top 10 Market Cap ─────────────────────────────────────────
        top10 = df.nlargest(10, "avg_market_cap_usd").sort_values("avg_market_cap_usd")
        colors1 = [PALETTE[i % len(PALETTE)] for i in range(len(top10))]
        bars = ax1.barh(top10["symbol"], top10["avg_market_cap_usd"] / 1e9,
                        color=colors1, edgecolor="none", height=0.6)
        ax1.set_xlabel("Market Cap (USD Billones)")
        ax1.set_title("🏆  Top 10  —  Market Cap", color=C["text"], fontweight="bold")
        ax1.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"${x:.0f}B"))
        for bar, val in zip(bars, top10["avg_market_cap_usd"] / 1e9):
            ax1.text(bar.get_width() * 1.01,
                     bar.get_y() + bar.get_height() / 2,
                     f"${val:.1f}B", va="center", fontsize=7.5, color=C["text2"])
        ax1.grid(axis="x", alpha=0.3)

        # ── 2. Rendimiento intradiario (Open→Close %) ─────────────────────
        df2 = df.copy()
        df2["chg"] = df2.apply(
            lambda r: pct(r["open_price_usd"], r["close_price_usd"]) or 0, axis=1)
        df2 = df2.sort_values("chg")
        col2 = [C["green"] if v >= 0 else C["red"] for v in df2["chg"]]
        ax2.barh(df2["symbol"], df2["chg"], color=col2, edgecolor="none", height=0.6)
        ax2.axvline(0, color=C["text2"], linewidth=0.8, zorder=3)
        ax2.set_xlabel("Cambio %  (Open → Close)")
        ax2.set_title("📉📈  Rendimiento Intradiario", color=C["text"], fontweight="bold")
        ax2.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x:+.1f}%"))
        ax2.grid(axis="x", alpha=0.3)

        # ── 3. Scatter Market Cap vs Volumen ──────────────────────────────
        df3 = df[df["avg_market_cap_usd"] > 0].copy()
        mc  = df3["avg_market_cap_usd"].values
        vol = df3["avg_volume_24h_usd"].values
        sizes = np.interp(mc, (mc.min(), mc.max()), (25, 450))
        cols3 = [PALETTE[i % len(PALETTE)] for i in range(len(df3))]
        ax3.scatter(mc / 1e9, vol / 1e9, s=sizes,
                    c=cols3, alpha=0.75, edgecolors="none", zorder=3)
        for _, r in df3.nlargest(6, "avg_market_cap_usd").iterrows():
            ax3.annotate(
                r["symbol"],
                (r["avg_market_cap_usd"] / 1e9, r["avg_volume_24h_usd"] / 1e9),
                xytext=(4, 4), textcoords="offset points",
                fontsize=7.5, color=C["text2"]
            )
        ax3.set_xlabel("Market Cap (B USD)")
        ax3.set_ylabel("Volumen 24h (B USD)")
        ax3.set_title("💧  Market Cap  vs  Volumen", color=C["text"], fontweight="bold")
        ax3.grid(True, alpha=0.3)

        # ── 4. Volatilidad intradiaria  (High–Low) / Close × 100 ─────────
        df4 = df.copy()
        df4["volatility"] = (
            (df4["high_price_usd"] - df4["low_price_usd"])
            / df4["close_price_usd"].replace(0, np.nan) * 100
        ).fillna(0)
        df4 = df4.sort_values("volatility", ascending=False).head(20)
        cols4 = [PALETTE[i % len(PALETTE)] for i in range(len(df4))]
        ax4.bar(df4["symbol"], df4["volatility"], color=cols4,
                edgecolor="none", width=0.6)
        ax4.set_ylabel("(High − Low) / Close  ×  100")
        ax4.set_title("⚡  Volatilidad Intradiaria  —  Top 20",
                      color=C["text"], fontweight="bold")
        ax4.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x:.2f}%"))
        ax4.grid(axis="y", alpha=0.3)
        plt.setp(ax4.get_xticklabels(), rotation=40, ha="right", fontsize=7.5)

        self.fig_ana.tight_layout(pad=2.5)
        self.canvas_ana.draw()

    # ── TAB 4 — Heatmap histórico ─────────────────────────────────────────────
    def _update_heatmap_tab(self):
        df = self.df_daily_ts
        self.fig_heat.clear()

        if df.empty or df["coin_id"].nunique() < 2 or df["date"].nunique() < 2:
            ax = self.fig_heat.add_subplot(111)
            ax.text(0.5, 0.5,
                    "Se necesitan datos de al menos\n2 días para mostrar el heatmap.\n\n"
                    "El pipeline corre cada 30 min —\nvuelve mañana 😉",
                    ha="center", va="center", color=C["text2"], fontsize=13,
                    transform=ax.transAxes)
            ax.set_axis_off()
            self.canvas_heat.draw()
            return

        # Calcular % cambio diario
        df = df.copy()
        df["chg"] = df.apply(
            lambda r: pct(r["open_price_usd"], r["close_price_usd"]) or 0, axis=1)

        # Pivot: filas = monedas (top 30 por market cap), columnas = fechas
        top_ids = (
            df.groupby("coin_id")["avg_market_cap_usd"]
            .mean().nlargest(30).index.tolist()
        )
        pivot = (
            df[df["coin_id"].isin(top_ids)]
            .pivot_table(index="symbol", columns="date", values="chg", aggfunc="mean")
        )
        pivot.columns = [str(d) for d in pivot.columns]

        # Ordenar filas por market cap promedio
        order = (
            df[df["coin_id"].isin(top_ids)]
            .groupby("symbol")["avg_market_cap_usd"].mean()
            .sort_values(ascending=False).index.tolist()
        )
        pivot = pivot.reindex([s for s in order if s in pivot.index])

        ax = self.fig_heat.add_subplot(111)
        sns.heatmap(
            pivot,
            ax=ax,
            cmap=sns.diverging_palette(10, 133, as_cmap=True),
            center=0,
            annot=len(pivot.columns) <= 14,   # números solo si hay pocos días
            fmt=".1f",
            linewidths=0.4,
            linecolor=C["bg"],
            cbar_kws={"label": "Cambio % diario", "shrink": 0.6},
        )
        ax.set_title("🌡️  Rendimiento Diario por Moneda  (% Open→Close)",
                     color=C["text"], fontweight="bold", fontsize=12)
        ax.set_xlabel("Fecha")
        ax.set_ylabel("")
        ax.tick_params(axis="x", rotation=30)
        ax.tick_params(axis="y", labelsize=8)

        self.fig_heat.tight_layout(pad=1.5)
        self.canvas_heat.draw()

    # =========================================================================
    # SORT TREEVIEW
    # =========================================================================
    def _sort_tree(self, col: str):
        def _key(val: str):
            clean = val.replace(",", "").replace("$", "").replace("%", "") \
                       .replace("+", "").replace("B", "e9").replace("M", "e6") \
                       .replace("K", "e3").replace("T", "e12").replace("—", "0")
            try:
                return float(clean)
            except ValueError:
                return val.lower()

        rows = [(self.tree.set(k, col), k) for k in self.tree.get_children("")]
        reverse = (self._tree_sort_rev
                   if self._tree_sort_col == col else False)
        rows.sort(key=lambda t: _key(t[0]), reverse=reverse)

        for idx, (_, k) in enumerate(rows):
            self.tree.move(k, "", idx)

        self._tree_sort_rev = not reverse
        self._tree_sort_col = col


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def main():
    root = tk.Tk()
    try:
        root.iconbitmap(default="")
    except Exception:
        pass

    app = CryptoDashboard(root)

    # Centrar ventana
    root.update_idletasks()
    sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
    w, h   = 1440, 880
    root.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    root.mainloop()


if __name__ == "__main__":
    main()
