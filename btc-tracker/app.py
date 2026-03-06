"""
app.py — Flask application factory and route definitions.
"""

import json
import logging
import os
import threading

from flask import Flask, redirect, render_template, request, url_for, flash

import db
import prices
import scanner
import balances
import scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")

    # Initialise DB schema
    db.init_db()

    # Start background scheduler
    scheduler.start()

    # On first start, backfill prices in the background so the UI loads fast
    def _initial_sync():
        log.info("Running initial price backfill…")
        prices.backfill_prices()

    t = threading.Thread(target=_initial_sync, daemon=True)
    t.start()

    # ------------------------------------------------------------------ #
    #  Routes                                                              #
    # ------------------------------------------------------------------ #

    @app.route("/")
    def dashboard():
        with db.get_conn() as conn:
            wallets = db.get_wallets(conn)
            latest_price = db.get_latest_price(conn)
            wallet_data = []
            total_sats = 0
            for w in wallets:
                bal_sats = db.get_current_balance_sats(conn, w["id"])
                total_sats += bal_sats
                bal_btc = bal_sats / 1e8
                bal_usd = round(bal_btc * latest_price, 2) if latest_price else None
                wallet_data.append({
                    "id": w["id"],
                    "label": w["label"],
                    "xpub_short": w["xpub"][:20] + "…",
                    "balance_sats": bal_sats,
                    "balance_btc": round(bal_btc, 8),
                    "balance_usd": bal_usd,
                    "last_scanned_at": w["last_scanned_at"],
                })
            total_btc = round(total_sats / 1e8, 8)
            total_usd = round(total_btc * latest_price, 2) if latest_price else None
            chart_data = db.get_portfolio_chart_data(conn)

        return render_template(
            "dashboard.html",
            wallets=wallet_data,
            total_btc=total_btc,
            total_usd=total_usd,
            latest_price=latest_price,
            chart_labels=json.dumps([r["date"] for r in chart_data]),
            chart_values=json.dumps([r["total_usd"] for r in chart_data]),
        )

    @app.route("/wallets")
    def wallet_list():
        with db.get_conn() as conn:
            wallets = db.get_wallets(conn)
        return render_template("wallets.html", wallets=wallets)

    @app.route("/wallets/add", methods=["POST"])
    def wallet_add():
        label = request.form.get("label", "").strip()
        xpub = request.form.get("xpub", "").strip()
        if not label or not xpub:
            flash("Both label and xpub are required.", "error")
            return redirect(url_for("wallet_list"))
        # Basic sanity check — zpub starts with 'zpub'
        if not (xpub.startswith("zpub") or xpub.startswith("xpub")):
            flash("XPub must start with 'zpub' (or 'xpub' for some wallets).", "error")
            return redirect(url_for("wallet_list"))
        try:
            with db.get_conn() as conn:
                db.add_wallet(conn, label, xpub)
        except Exception as exc:
            flash(f"Could not add wallet: {exc}", "error")
            return redirect(url_for("wallet_list"))
        flash(f"Wallet '{label}' added. Click Sync Now to load transaction history.", "success")
        return redirect(url_for("wallet_list"))

    @app.route("/wallets/<int:wallet_id>/delete", methods=["POST"])
    def wallet_delete(wallet_id):
        with db.get_conn() as conn:
            w = db.get_wallet(conn, wallet_id)
            if w:
                db.delete_wallet(conn, wallet_id)
                flash(f"Wallet '{w['label']}' removed.", "success")
        return redirect(url_for("wallet_list"))

    @app.route("/sync", methods=["POST"])
    def sync():
        """Trigger a full sync in the background and redirect back."""
        def _bg():
            try:
                scanner.scan_all_wallets()
                balances.rebuild_all_balances()
                prices.backfill_prices()
            except Exception:
                log.exception("Background sync failed")

        t = threading.Thread(target=_bg, daemon=True)
        t.start()
        flash("Sync started in the background. Refresh in a minute to see updated data.", "info")
        return redirect(url_for("dashboard"))

    @app.route("/api/chart-data")
    def api_chart_data():
        """JSON endpoint for the portfolio chart."""
        from flask import jsonify
        with db.get_conn() as conn:
            data = db.get_portfolio_chart_data(conn)
        return jsonify(data)

    @app.teardown_appcontext
    def _shutdown_scheduler(exc=None):
        pass  # Scheduler is a daemon; it shuts down with the process.

    return app


if __name__ == "__main__":
    # Local development only — gunicorn is used in production
    application = create_app()
    application.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
