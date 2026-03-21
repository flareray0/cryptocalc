from __future__ import annotations

from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.routes_analysis import router as analysis_router
from app.api.routes_api_connections import router as api_connections_router
from app.api.routes_calc import router as calc_router
from app.api.routes_import import _save_upload_to_temp
from app.api.routes_import import router as import_router
from app.api.routes_reports import router as reports_router
from app.api.routes_settings import router as settings_router
from app.domain.enums import CalculationMethod
from app.services.analysis_service import AnalysisService
from app.services.calc_service import CalcService
from app.services.exchange_sync_service import ExchangeSyncService
from app.services.import_service import ImportService
from app.services.report_service import ReportService
from app.storage.app_state import clear_imported_state, load_import_batches, load_transactions
from app.storage.json_store import transaction_to_dict
from app.storage.settings import get_paths, load_settings, save_settings
from app.ui_web.charts import build_line_chart


paths = get_paths()
settings = load_settings()
templates = Jinja2Templates(directory=str(paths.root / "app" / "ui_web" / "pages"))

app = FastAPI(
    title="Crypto Tax App",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings["allowed_origins"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(paths.root / "app" / "ui_web" / "assets")), name="static")

app.include_router(import_router)
app.include_router(calc_router)
app.include_router(reports_router)
app.include_router(analysis_router)
app.include_router(api_connections_router)
app.include_router(settings_router)


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=400,
        content={
            "error": str(exc),
            "path": str(request.url.path),
        },
    )


@app.exception_handler(Exception)
async def generic_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_error",
            "detail": str(exc),
            "path": str(request.url.path),
        },
    )


def _available_years(transactions) -> list[int]:
    years = {
        tx.timestamp_jst.year
        for tx in transactions
        if tx.timestamp_jst is not None
    }
    return sorted(years, reverse=True)


def _as_number(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _line_chart_from_rows(rows, value_key: str, title: str, color: str = "#0f766e") -> str:
    values = [_as_number(row.get(value_key)) for row in rows]
    return build_line_chart(
        [{"label": value_key, "values": values, "color": color}],
        title=title,
    )


def _multi_line_chart(rows, series_defs, title: str) -> str:
    series = []
    for label, value_key, color in series_defs:
        values = [_as_number(row.get(value_key)) for row in rows]
        series.append({"label": label, "values": values, "color": color})
    return build_line_chart(series, title=title)


def _analysis_summary(analysis_run: dict | None) -> dict | None:
    if not analysis_run:
        return None
    snapshots = analysis_run.get("portfolio_snapshots", [])
    latest = snapshots[-1] if snapshots else None
    return {
        "latest_snapshot": latest,
        "asset_summary_table": analysis_run.get("asset_summary_table", []),
        "review_notes": analysis_run.get("review_notes", []),
        "edge_report": analysis_run.get("edge_report", {}),
    }


def _build_analysis_view_data(analysis_run: dict | None) -> dict:
    snapshots = analysis_run.get("portfolio_snapshots", []) if analysis_run else []
    asset_quantity_rows = analysis_run.get("asset_quantity_history", []) if analysis_run else []
    top_symbols = []
    if asset_quantity_rows:
        totals = {}
        for row in asset_quantity_rows:
            value = _as_number(row.get("market_value_jpy")) or 0
            totals[row["symbol"]] = max(value, totals.get(row["symbol"], 0))
        top_symbols = sorted(totals, key=totals.get, reverse=True)[:5]
    asset_series_rows = []
    if top_symbols and analysis_run:
        by_timestamp = {}
        for row in asset_quantity_rows:
            if row["symbol"] not in top_symbols:
                continue
            by_timestamp.setdefault(row["timestamp"], {})[row["symbol"]] = row["quantity"]
        asset_series_rows = [
            {symbol: by_timestamp[timestamp].get(symbol) for symbol in top_symbols}
            for timestamp in sorted(by_timestamp)
        ]

    return {
        "chart_equity_jpy": _line_chart_from_rows(snapshots, "total_equity_jpy", "総資産 JPY 推移"),
        "chart_equity_usd": _line_chart_from_rows(
            snapshots,
            "total_equity_usd",
            "総資産 USD 推移",
            "#0b5cab",
        ),
        "chart_benchmark": _multi_line_chart(
            snapshots,
            [
                ("actual", "total_equity_jpy", "#0f766e"),
                ("benchmark", "benchmark_total_equity_jpy", "#92400e"),
            ],
            "actual vs benchmark (JPY)",
        ),
        "chart_pnl": _multi_line_chart(
            snapshots,
            [
                ("realized", "realized_pnl_jpy", "#0f766e"),
                ("unrealized", "unrealized_pnl_jpy", "#0b5cab"),
                ("fees", "fees_jpy", "#991b1b"),
            ],
            "realized / unrealized / fees",
        ),
        "chart_asset": _multi_line_chart(
            asset_series_rows,
            [
                (symbol, symbol, color)
                for symbol, color in zip(
                    top_symbols,
                    ["#0f766e", "#0b5cab", "#92400e", "#7c3aed", "#be185d"],
                    strict=False,
                )
            ],
            "asset quantity history",
        ),
    }


def _base_context(request: Request, title: str, **extra):
    txs = load_transactions()
    review_count = sum(1 for tx in txs if tx.review_flag)
    latest_batches = load_import_batches()[-10:]
    report_service = ReportService()
    analysis_service = AnalysisService()
    latest_run = report_service.latest_run()
    latest_analysis = analysis_service.latest_run()
    context = {
        "request": request,
        "title": title,
        "settings": load_settings(),
        "transactions_count": len(txs),
        "review_required_count": review_count,
        "latest_batches": list(reversed(latest_batches)),
        "latest_run": latest_run,
        "latest_analysis": latest_analysis,
        "latest_analysis_summary": _analysis_summary(latest_analysis),
        "available_years": _available_years(txs),
        "sync_status": ExchangeSyncService().connection_state(),
        "message": request.query_params.get("message"),
        "error": request.query_params.get("error"),
        "notice_tax": "本ソフトは日本の暗号資産損益計算の補助を目的とするローカル専用ツールです。最終的な税務判断は利用者または税理士等が行ってください。",
        "notice_review": "不明取引・JPY未評価取引・対応付け不能な取引は要確認として残します。",
    }
    context.update(extra)
    return context


@app.get("/", response_class=HTMLResponse)
def root():
    return RedirectResponse("/dashboard", status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    report_service = ReportService()
    analysis_service = AnalysisService()
    latest_run = report_service.latest_run()
    latest_analysis = analysis_service.latest_run()
    yearly = latest_run.get("yearly_summary") if latest_run else None
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        _base_context(
            request,
            "ダッシュボード",
            yearly_summary=yearly,
            analysis_summary=_analysis_summary(latest_analysis),
        ),
    )


@app.get("/import", response_class=HTMLResponse)
def import_page(request: Request):
    return templates.TemplateResponse(
        request,
        "import.html",
        _base_context(request, "ファイル取込"),
    )


@app.get("/integrations", response_class=HTMLResponse)
def integrations_page(request: Request):
    return templates.TemplateResponse(
        request,
        "integrations.html",
        _base_context(request, "API連携"),
    )


@app.get("/transactions", response_class=HTMLResponse)
def transactions_page(
    request: Request,
    year: int | None = None,
    asset: str | None = None,
    tx_type: str | None = None,
    review_required: bool | None = None,
):
    txs = load_transactions()
    filtered = []
    for tx in txs:
        if year and (tx.timestamp_jst is None or tx.timestamp_jst.year != year):
            continue
        if asset and asset.upper() not in {tx.base_asset or "", tx.quote_asset or "", tx.fee_asset or ""}:
            continue
        if tx_type and tx.tx_type.value != tx_type:
            continue
        if review_required is not None and tx.review_flag != review_required:
            continue
        filtered.append(transaction_to_dict(tx))
    return templates.TemplateResponse(
        request,
        "transactions.html",
        _base_context(
            request,
            "取引一覧",
            transactions=filtered[:500],
            filter_year=year,
            filter_asset=asset,
            filter_tx_type=tx_type,
            filter_review_required=review_required,
        ),
    )


@app.get("/calc", response_class=HTMLResponse)
def calc_page(
    request: Request,
    year: int | None = None,
    method: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
):
    report_service = ReportService()
    calc_service = CalcService()
    settings = load_settings()
    selected_method = CalculationMethod(method) if method else None
    latest_run = report_service.latest_run(method=selected_method, year=year)
    selected_start_year = (
        start_year if start_year is not None else settings["calc_window"].get("default_start_year")
    )
    selected_end_year = end_year if end_year is not None else settings["calc_window"].get("default_end_year")
    latest_window_run = calc_service.latest_window_run(
        method=selected_method,
        start_year=selected_start_year,
        end_year=selected_end_year,
    )
    return templates.TemplateResponse(
        request,
        "calc.html",
        _base_context(
            request,
            "計算結果",
            run_data=latest_run,
            selected_year=year,
            selected_method=method,
            window_run_data=latest_window_run,
            selected_start_year=selected_start_year,
            selected_end_year=selected_end_year,
        ),
    )


@app.get("/analysis", response_class=HTMLResponse)
def analysis_page(
    request: Request,
    year: int | None = None,
    method_reference: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
):
    settings = load_settings()
    selected_method = CalculationMethod(method_reference) if method_reference else None
    service = AnalysisService()
    analysis_run = service.latest_run(year=year, method_reference=selected_method)
    selected_start_year = (
        start_year if start_year is not None else settings["analysis_window"].get("default_start_year")
    )
    selected_end_year = (
        end_year if end_year is not None else settings["analysis_window"].get("default_end_year")
    )
    analysis_window_run = service.latest_window_run(
        start_year=selected_start_year,
        end_year=selected_end_year,
        method_reference=selected_method,
    )
    single_view = _build_analysis_view_data(analysis_run)
    window_view = _build_analysis_view_data(analysis_window_run)
    return templates.TemplateResponse(
        request,
        "analysis.html",
        _base_context(
            request,
            "分析",
            analysis_run=analysis_run,
            analysis_window_run=analysis_window_run,
            selected_year=year,
            selected_method_reference=method_reference,
            selected_start_year=selected_start_year,
            selected_end_year=selected_end_year,
            chart_equity_jpy=single_view["chart_equity_jpy"],
            chart_equity_usd=single_view["chart_equity_usd"],
            chart_benchmark=single_view["chart_benchmark"],
            chart_pnl=single_view["chart_pnl"],
            chart_asset=single_view["chart_asset"],
            window_chart_equity_jpy=window_view["chart_equity_jpy"],
            window_chart_equity_usd=window_view["chart_equity_usd"],
            window_chart_benchmark=window_view["chart_benchmark"],
            window_chart_pnl=window_view["chart_pnl"],
            window_chart_asset=window_view["chart_asset"],
        ),
    )


@app.get("/review", response_class=HTMLResponse)
def review_page(request: Request, year: int | None = None):
    txs = load_transactions()
    filtered = []
    for tx in txs:
        if not tx.review_flag:
            continue
        if year and (tx.timestamp_jst is None or tx.timestamp_jst.year != year):
            continue
        filtered.append(transaction_to_dict(tx))
    return templates.TemplateResponse(
        request,
        "review.html",
        _base_context(request, "要確認取引", review_rows=filtered[:500], filter_year=year),
    )


@app.get("/exports", response_class=HTMLResponse)
def exports_page(
    request: Request,
    year: int | None = None,
    method: str | None = None,
    method_reference: str | None = None,
):
    report_service = ReportService()
    selected_method = CalculationMethod(method) if method else None
    latest_run = report_service.latest_run(method=selected_method, year=year)
    export_files = sorted(paths.exports.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return templates.TemplateResponse(
        request,
        "exports.html",
        _base_context(
            request,
            "エクスポート",
            run_data=latest_run,
            export_files=[p.name for p in export_files if p.is_file()],
            selected_year=year,
            selected_method=method,
            selected_method_reference=method_reference,
        ),
    )


@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    return templates.TemplateResponse(
        request,
        "settings.html",
        _base_context(request, "設定"),
    )


@app.post("/ui/import/csv")
def ui_import_csv(file: UploadFile = File(...)):
    service = ImportService()
    temp_path = _save_upload_to_temp(file)
    try:
        service.import_file(temp_path)
    except Exception as exc:
        return RedirectResponse(f"/import?error={str(exc)}", status_code=303)
    finally:
        temp_path.unlink(missing_ok=True)
    return RedirectResponse("/import?message=CSVを取り込みました", status_code=303)


@app.post("/ui/import/manual-adjustments")
def ui_import_manual_adjustments(file: UploadFile = File(...)):
    service = ImportService()
    temp_path = _save_upload_to_temp(file)
    try:
        service.import_file(temp_path, import_kind="manual_adjustment")
    except Exception as exc:
        return RedirectResponse(f"/import?error={str(exc)}", status_code=303)
    finally:
        temp_path.unlink(missing_ok=True)
    return RedirectResponse("/import?message=手動補正CSVを取り込みました", status_code=303)


@app.post("/ui/import/manual-rates")
def ui_import_manual_rates(file: UploadFile = File(...)):
    service = ImportService()
    temp_path = _save_upload_to_temp(file)
    try:
        service.import_manual_rate_file(temp_path)
    except Exception as exc:
        return RedirectResponse(f"/import?error={str(exc)}", status_code=303)
    finally:
        temp_path.unlink(missing_ok=True)
    return RedirectResponse("/import?message=JPY補完レートCSVを取り込みました", status_code=303)


@app.post("/ui/import/reset")
def ui_import_reset():
    removed = clear_imported_state()
    params = urlencode(
        {
            "message": (
                "取引データをリセットしました。"
                f" transactions={removed['transactions']}, import_batches={removed['import_batches']}"
            )
        }
    )
    return RedirectResponse(f"/import?{params}", status_code=303)


@app.post("/ui/calc/run")
def ui_calc_run(
    year: int = Form(...),
    method: str = Form(...),
):
    service = CalcService()
    try:
        service.run(year=year, method=CalculationMethod(method))
    except Exception as exc:
        return RedirectResponse(f"/calc?year={year}&method={method}&error={str(exc)}", status_code=303)
    return RedirectResponse(f"/calc?year={year}&method={method}&message=計算を実行しました", status_code=303)


@app.post("/ui/calc/run-window")
def ui_calc_run_window(
    start_year: str = Form(default=""),
    end_year: str = Form(default=""),
    method: str = Form(...),
):
    service = CalcService()
    parsed_start_year = int(start_year) if start_year else None
    parsed_end_year = int(end_year) if end_year else None
    try:
        service.run_window(
            start_year=parsed_start_year,
            end_year=parsed_end_year,
            method=CalculationMethod(method),
        )
    except Exception as exc:
        params = {"method": method, "error": str(exc)}
        if parsed_start_year is not None:
            params["start_year"] = parsed_start_year
        if parsed_end_year is not None:
            params["end_year"] = parsed_end_year
        return RedirectResponse(f"/calc?{urlencode(params)}", status_code=303)

    params = {"method": method, "message": "期間集計を実行しました"}
    if parsed_start_year is not None:
        params["start_year"] = parsed_start_year
    if parsed_end_year is not None:
        params["end_year"] = parsed_end_year
    return RedirectResponse(f"/calc?{urlencode(params)}", status_code=303)


@app.post("/ui/analysis/run")
def ui_analysis_run(
    year: int = Form(...),
    method_reference: str = Form(...),
):
    service = AnalysisService()
    try:
        service.run(year=year, method_reference=CalculationMethod(method_reference))
    except Exception as exc:
        return RedirectResponse(
            f"/analysis?year={year}&method_reference={method_reference}&error={str(exc)}",
            status_code=303,
        )
    return RedirectResponse(
        f"/analysis?year={year}&method_reference={method_reference}&message=分析を実行しました",
        status_code=303,
    )


@app.post("/ui/analysis/run-window")
def ui_analysis_run_window(
    start_year: str = Form(default=""),
    end_year: str = Form(default=""),
    method_reference: str = Form(...),
):
    service = AnalysisService()
    parsed_start_year = int(start_year) if start_year else None
    parsed_end_year = int(end_year) if end_year else None
    try:
        service.run_window(
            start_year=parsed_start_year,
            end_year=parsed_end_year,
            method_reference=CalculationMethod(method_reference),
        )
    except Exception as exc:
        params = {"method_reference": method_reference, "error": str(exc)}
        if parsed_start_year is not None:
            params["start_year"] = parsed_start_year
        if parsed_end_year is not None:
            params["end_year"] = parsed_end_year
        return RedirectResponse(f"/analysis?{urlencode(params)}", status_code=303)

    params = {"method_reference": method_reference, "message": "期間分析を実行しました"}
    if parsed_start_year is not None:
        params["start_year"] = parsed_start_year
    if parsed_end_year is not None:
        params["end_year"] = parsed_end_year
    return RedirectResponse(f"/analysis?{urlencode(params)}", status_code=303)


@app.post("/ui/integrations/connect")
def ui_connect_binance_japan(
    api_key: str = Form(default=""),
    api_secret: str = Form(default=""),
    base_url: str = Form(default=""),
):
    service = ExchangeSyncService()
    try:
        service.save_connection(api_key=api_key, api_secret=api_secret, base_url=base_url or None)
    except Exception as exc:
        return RedirectResponse(f"/integrations?error={str(exc)}", status_code=303)
    return RedirectResponse("/integrations?message=API接続を保存しました", status_code=303)


@app.post("/ui/integrations/sync")
def ui_sync_binance_japan(
    symbols: str = Form(default=""),
    start_time_ms: str = Form(default=""),
    end_time_ms: str = Form(default=""),
):
    service = ExchangeSyncService()
    try:
        service.sync(
            symbols=[token.strip().upper() for token in symbols.split(",") if token.strip()],
            start_time_ms=int(start_time_ms) if start_time_ms else None,
            end_time_ms=int(end_time_ms) if end_time_ms else None,
        )
    except Exception as exc:
        return RedirectResponse(f"/integrations?error={str(exc)}", status_code=303)
    return RedirectResponse("/integrations?message=API同期を実行しました", status_code=303)


@app.post("/ui/integrations/disconnect")
def ui_disconnect_binance_japan():
    ExchangeSyncService().disconnect()
    return RedirectResponse("/integrations?message=API接続を解除しました", status_code=303)


@app.post("/ui/settings/save")
def ui_save_settings(
    default_year: str = Form(default=""),
    default_method: str = Form(...),
    disclaimer_acknowledged: str | None = Form(default=None),
):
    current = load_settings()
    current["default_year"] = int(default_year) if default_year else None
    current["default_method"] = default_method
    current["disclaimer_acknowledged"] = disclaimer_acknowledged == "on"
    save_settings(current)
    return RedirectResponse("/settings?message=設定を保存しました", status_code=303)


@app.post("/ui/exports/nta")
def ui_export_nta(
    year: int = Form(...),
    method: str = Form(...),
):
    try:
        ReportService().nta_export(method=CalculationMethod(method), year=year)
    except Exception as exc:
        return RedirectResponse(f"/exports?year={year}&method={method}&error={str(exc)}", status_code=303)
    return RedirectResponse(f"/exports?year={year}&method={method}&message=国税庁補助出力を生成しました", status_code=303)


@app.post("/ui/exports/analysis")
def ui_export_analysis(
    year: int = Form(...),
    method_reference: str = Form(...),
):
    try:
        AnalysisService().export_analysis(
            year=year,
            method_reference=CalculationMethod(method_reference),
        )
    except Exception as exc:
        return RedirectResponse(
            f"/exports?year={year}&method_reference={method_reference}&error={str(exc)}",
            status_code=303,
        )
    return RedirectResponse(
        f"/exports?year={year}&method_reference={method_reference}&message=分析エクスポートを生成しました",
        status_code=303,
    )
