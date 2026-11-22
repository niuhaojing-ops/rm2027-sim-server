"""Built-in Web Console & REST API for RM2027 simulation server."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from typing import Any

from .diagnostics import build_full_diagnostics, run_mqtt_probe
from .match_engine import MatchSimulator
from .mqtt_broker import MqttBrokerServer
from .video_streamer import HevcVideoStreamer


@dataclass
class SimControlContext:
    loop: asyncio.AbstractEventLoop
    broker: MqttBrokerServer
    simulator: MatchSimulator
    video: HevcVideoStreamer
    listen_host: str
    control_port: int
    terminal_config_path: str | None = None

    def run_coro(self, coro: Any, timeout: float = 5.0) -> Any:
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return fut.result(timeout=timeout)

    def snapshot(self) -> dict[str, Any]:
        return {
            "ok": True,
            "mqtt": {
                "host": self.listen_host,
                "port": self.broker.actual_port,
                "total_connections": self.broker.total_connections,
                "total_rejected": self.broker.total_rejected,
                "total_rx_publishes": self.broker.total_rx_publishes,
                "total_tx_publishes": self.broker.total_tx_publishes,
            },
            "auth": self.broker.auth.to_dict(),
            "clients": [s.to_dict() for s in self.broker.active_sessions()],
            "audit": [r.to_dict() for r in reversed(self.broker.audit_log)][:30],
            "simulator": self.simulator.to_dict(),
            "video": self.video.to_dict(),
        }

    def diagnostics(self) -> dict[str, Any]:
        return build_full_diagnostics(
            listen_host=self.listen_host,
            mqtt_port=self.broker.actual_port,
            control_port=self.control_port,
            auth=self.broker.auth,
            video_cfg=self.video.config,
            audit_records=[r.to_dict() for r in self.broker.audit_log],
            active_sessions=[s.to_dict() for s in self.broker.active_sessions()],
            terminal_config_path=self.terminal_config_path,
        )


WEB_HTML = """<!doctype html>
<html lang="zh-CN" data-theme="light">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RM2027 模拟赛事控制中心</title>
<style>
  :root, [data-theme="light"] {
    --canvas: #f2f5fa;
    --mesh-1: rgba(37, 99, 235, 0.14);
    --mesh-2: rgba(99, 102, 241, 0.11);
    --mesh-3: rgba(16, 185, 129, 0.09);
    --glass: rgba(255, 255, 255, 0.78);
    --glass-strong: rgba(255, 255, 255, 0.90);
    --glass-subtle: rgba(255, 255, 255, 0.56);
    --header-bg: rgba(247, 249, 253, 0.78);
    --pill-bg: rgba(15, 23, 42, 0.055);
    --pill-hover: rgba(15, 23, 42, 0.09);
    --input-bg: rgba(255, 255, 255, 0.85);
    --bd: rgba(15, 23, 42, 0.08);
    --bd-highlight: rgba(255, 255, 255, 0.92);
    --shadow-card: 0 12px 32px -12px rgba(15, 23, 42, 0.08), 0 2px 8px -2px rgba(15, 23, 42, 0.04), inset 0 1px 0 rgba(255, 255, 255, 0.92);
    --shadow-pill: 0 4px 12px -4px rgba(37, 99, 235, 0.25);
    --ink: #0f172a;
    --ink-2: #334155;
    --muted: #64748b;
    --accent: #2563eb;
    --accent-soft: rgba(37, 99, 235, 0.12);
    --ok: #16a34a;
    --ok-soft: rgba(22, 163, 74, 0.12);
    --warn: #d97706;
    --warn-soft: rgba(217, 119, 6, 0.13);
    --bad: #dc2626;
    --bad-soft: rgba(220, 38, 38, 0.12);
    --red: #e11d48;
    --red-soft: rgba(225, 29, 72, 0.10);
    --blue: #2563eb;
    --blue-soft: rgba(37, 99, 235, 0.10);
    --log-bg: rgba(248, 250, 252, 0.85);
    --toast-bg: rgba(15, 23, 42, 0.88);
    --toast-ink: #ffffff;
  }
  [data-theme="dark"] {
    --canvas: #0b101b;
    --mesh-1: rgba(59, 130, 246, 0.18);
    --mesh-2: rgba(139, 92, 246, 0.14);
    --mesh-3: rgba(16, 185, 129, 0.10);
    --glass: rgba(22, 29, 44, 0.76);
    --glass-strong: rgba(28, 37, 56, 0.88);
    --glass-subtle: rgba(16, 22, 34, 0.65);
    --header-bg: rgba(13, 19, 31, 0.78);
    --pill-bg: rgba(255, 255, 255, 0.07);
    --pill-hover: rgba(255, 255, 255, 0.12);
    --input-bg: rgba(12, 18, 29, 0.78);
    --bd: rgba(255, 255, 255, 0.09);
    --bd-highlight: rgba(255, 255, 255, 0.11);
    --shadow-card: 0 18px 40px -16px rgba(0, 0, 0, 0.55), inset 0 1px 0 rgba(255, 255, 255, 0.09);
    --shadow-pill: 0 6px 16px -4px rgba(59, 130, 246, 0.42);
    --ink: #f1f5f9;
    --ink-2: #cbd5e1;
    --muted: #94a3b8;
    --accent: #3b82f6;
    --accent-soft: rgba(59, 130, 246, 0.18);
    --ok: #22c55e;
    --ok-soft: rgba(34, 197, 94, 0.16);
    --warn: #f59e0b;
    --warn-soft: rgba(245, 158, 11, 0.16);
    --bad: #ef4444;
    --bad-soft: rgba(239, 68, 68, 0.17);
    --red: #fb7185;
    --red-soft: rgba(244, 63, 94, 0.16);
    --blue: #60a5fa;
    --blue-soft: rgba(59, 130, 246, 0.16);
    --log-bg: rgba(10, 15, 26, 0.78);
    --toast-bg: rgba(241, 245, 249, 0.92);
    --toast-ink: #0f172a;
  }

  * { box-sizing: border-box; }
  html, body { min-height: 100%; }
  body {
    margin: 0;
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "PingFang SC", "Noto Sans SC", "Segoe UI", Roboto, sans-serif;
    font-size: 13.5px;
    line-height: 1.5;
    background-color: var(--canvas);
    background-image:
      radial-gradient(60% 45% at 10% 0%, var(--mesh-1), transparent 72%),
      radial-gradient(50% 45% at 92% 8%, var(--mesh-2), transparent 70%),
      radial-gradient(45% 40% at 50% 96%, var(--mesh-3), transparent 72%);
    background-attachment: fixed;
    -webkit-font-smoothing: antialiased;
    transition: background-color 0.35s cubic-bezier(0.22, 1, 0.36, 1), color 0.25s ease;
  }

  /* Header & Navigation */
  .topbar-wrap {
    position: sticky;
    top: 0;
    z-index: 40;
    padding: 12px 18px 4px;
  }
  header.topbar {
    max-width: 1400px;
    margin: 0 auto;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 10px 16px;
    background: var(--header-bg);
    backdrop-filter: blur(26px) saturate(180%);
    -webkit-backdrop-filter: blur(26px) saturate(180%);
    border: 1px solid var(--bd);
    border-radius: 24px;
    box-shadow: var(--shadow-card);
  }
  .brand {
    display: flex;
    align-items: center;
    gap: 11px;
  }
  .emblem {
    width: 36px;
    height: 36px;
    border-radius: 12px;
    background: linear-gradient(135deg, #2563eb 0%, #4f46e5 100%);
    display: grid;
    place-items: center;
    color: #fff;
    box-shadow: 0 6px 16px -4px rgba(37, 99, 235, 0.45);
    flex-shrink: 0;
  }
  .brand-title h1 {
    margin: 0;
    font-size: 15.5px;
    font-weight: 700;
    letter-spacing: -0.015em;
  }
  .brand-title .sub {
    font-size: 11px;
    color: var(--muted);
    font-weight: 500;
  }
  .status-pills {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 7px;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 11px;
    border-radius: 999px;
    background: var(--pill-bg);
    border: 1px solid var(--bd);
    font-size: 12px;
    font-weight: 600;
    color: var(--ink-2);
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
    transition: all 0.25s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .pill .dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--muted);
  }
  .pill.ok { background: var(--ok-soft); border-color: rgba(22, 163, 74, 0.25); color: var(--ok); }
  .pill.ok .dot { background: var(--ok); box-shadow: 0 0 0 3px rgba(22, 163, 74, 0.20); }
  .pill.warn { background: var(--warn-soft); border-color: rgba(217, 119, 6, 0.25); color: var(--warn); }
  .pill.warn .dot { background: var(--warn); }
  .pill.bad { background: var(--bad-soft); border-color: rgba(220, 38, 38, 0.25); color: var(--bad); }
  .pill.bad .dot { background: var(--bad); }
  .pill.info { background: var(--accent-soft); border-color: rgba(37, 99, 235, 0.22); color: var(--accent); }
  .pill.info .dot { background: var(--accent); }

  .nav-group {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  nav.seg-nav {
    display: inline-flex;
    padding: 4px;
    border-radius: 999px;
    background: var(--pill-bg);
    border: 1px solid var(--bd);
    gap: 3px;
  }
  .tab {
    border: 0;
    background: transparent;
    color: var(--muted);
    padding: 6px 14px;
    border-radius: 999px;
    font-size: 12.5px;
    font-weight: 600;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    transition: all 0.24s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .tab:hover { color: var(--ink); }
  .tab.active {
    background: var(--glass-strong);
    color: var(--accent);
    box-shadow: 0 3px 10px -2px rgba(15, 23, 42, 0.12);
  }
  [data-theme="dark"] .tab.active {
    background: var(--accent);
    color: #fff;
    box-shadow: var(--shadow-pill);
  }
  .icon-btn {
    width: 34px;
    height: 34px;
    border-radius: 999px;
    border: 1px solid var(--bd);
    background: var(--pill-bg);
    color: var(--ink-2);
    display: grid;
    place-items: center;
    cursor: pointer;
    transition: all 0.22s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .icon-btn:hover { background: var(--pill-hover); transform: translateY(-1px); }

  /* Layout & Bento Cards */
  main {
    max-width: 1400px;
    margin: 0 auto;
    padding: 12px 18px 48px;
  }
  .panel { display: none; animation: fadeIn 0.26s cubic-bezier(0.22, 1, 0.36, 1); }
  .panel.active { display: block; }
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .bento-grid-2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; margin-bottom: 14px; }
  .bento-grid-4 { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 14px; }
  @media (max-width: 1040px) {
    .bento-grid-2 { grid-template-columns: 1fr; }
    .bento-grid-4 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
  @media (max-width: 600px) {
    .bento-grid-4 { grid-template-columns: 1fr; }
  }

  .card {
    background: var(--glass);
    backdrop-filter: blur(24px) saturate(165%);
    -webkit-backdrop-filter: blur(24px) saturate(165%);
    border: 1px solid var(--bd);
    border-radius: 26px;
    padding: 18px 20px;
    margin-bottom: 14px;
    box-shadow: var(--shadow-card);
  }
  .bento-grid-2 > .card, .bento-grid-4 > .card { margin-bottom: 0; }

  .card-head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 14px;
  }
  .card-title {
    margin: 0;
    font-size: 15px;
    font-weight: 700;
    letter-spacing: -0.01em;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .muted { color: var(--muted); font-size: 12px; }

  /* Hero Match Card */
  .hero-bento {
    display: grid;
    grid-template-columns: minmax(320px, 1.05fr) minmax(340px, 1.25fr);
    gap: 18px;
    align-items: stretch;
  }
  @media (max-width: 980px) {
    .hero-bento { grid-template-columns: 1fr; }
  }
  .hero-clock-box {
    background: var(--glass-subtle);
    border: 1px solid var(--bd);
    border-radius: 22px;
    padding: 18px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    gap: 14px;
  }
  .stage-badge {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    padding: 5px 12px;
    border-radius: 999px;
    font-size: 12.5px;
    font-weight: 700;
    background: var(--pill-bg);
    color: var(--ink-2);
  }
  .stage-badge.live { background: var(--ok-soft); color: var(--ok); }
  .stage-badge.standby { background: var(--accent-soft); color: var(--accent); }
  .stage-badge.paused { background: var(--warn-soft); color: var(--warn); }
  .stage-badge.done { background: var(--pill-bg); color: var(--muted); }

  .clock-digits {
    font-size: 46px;
    font-weight: 800;
    letter-spacing: -0.03em;
    line-height: 1.05;
    font-variant-numeric: tabular-nums;
    margin: 6px 0 4px;
  }
  .progress-track {
    width: 100%;
    height: 7px;
    border-radius: 999px;
    background: var(--pill-bg);
    overflow: hidden;
  }
  .progress-fill {
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, #2563eb, #06b6d4);
    width: 0%;
    transition: width 0.35s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .progress-fill.red { background: linear-gradient(90deg, #e11d48, #fb7185); }
  .progress-fill.blue { background: linear-gradient(90deg, #2563eb, #60a5fa); }
  .progress-fill.hp { background: linear-gradient(90deg, #16a34a, #4ade80); }
  .progress-fill.heat { background: linear-gradient(90deg, #f59e0b, #ef4444); }

  .hero-controls-box {
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    gap: 12px;
  }
  .ctrl-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
  }
  .ctrl-label {
    font-size: 12px;
    font-weight: 600;
    color: var(--muted);
    min-width: 68px;
  }

  /* Segmented Pills & Buttons */
  .seg-group {
    display: inline-flex;
    flex-wrap: wrap;
    padding: 3px;
    border-radius: 999px;
    background: var(--pill-bg);
    border: 1px solid var(--bd);
    gap: 2px;
  }
  .seg-btn {
    border: 0;
    background: transparent;
    color: var(--muted);
    padding: 5px 12px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .seg-btn:hover { color: var(--ink); }
  .seg-btn.active {
    background: var(--glass-strong);
    color: var(--accent);
    box-shadow: 0 2px 8px -2px rgba(15, 23, 42, 0.14);
  }
  [data-theme="dark"] .seg-btn.active {
    background: var(--accent);
    color: #fff;
  }

  .btn {
    border: 1px solid var(--bd);
    background: var(--glass-subtle);
    color: var(--ink);
    padding: 7px 13px;
    border-radius: 13px;
    font-size: 12.5px;
    font-weight: 600;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    transition: all 0.2s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .btn:hover {
    background: var(--glass-strong);
    border-color: rgba(37, 99, 235, 0.35);
    transform: translateY(-1px);
  }
  .btn:active { transform: translateY(0) scale(0.985); }
  .btn.sm { padding: 5px 10px; border-radius: 10px; font-size: 12px; }
  .btn.pri { background: linear-gradient(135deg, #2563eb, #3b82f6); border-color: transparent; color: #fff; box-shadow: var(--shadow-pill); }
  .btn.pri:hover { filter: brightness(1.06); }
  .btn.ok { background: linear-gradient(135deg, #16a34a, #22c55e); border-color: transparent; color: #fff; box-shadow: 0 4px 12px -4px rgba(22, 163, 74, 0.40); }
  .btn.warn { background: var(--warn-soft); border-color: rgba(217, 119, 6, 0.28); color: var(--warn); }
  .btn.bad { background: var(--bad-soft); border-color: rgba(220, 38, 38, 0.28); color: var(--bad); }
  .btn.solid-bad { background: linear-gradient(135deg, #dc2626, #ef4444); border-color: transparent; color: #fff; }

  /* Tactile Switch */
  .switch-label {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 12.5px;
    font-weight: 600;
    color: var(--ink-2);
    cursor: pointer;
    user-select: none;
  }
  .switch {
    appearance: none;
    -webkit-appearance: none;
    width: 38px;
    height: 22px;
    border-radius: 999px;
    background: var(--pill-hover);
    border: 1px solid var(--bd);
    position: relative;
    cursor: pointer;
    outline: none;
    transition: background 0.22s ease;
    flex-shrink: 0;
  }
  .switch::after {
    content: "";
    position: absolute;
    top: 2px;
    left: 2px;
    width: 16px;
    height: 16px;
    border-radius: 50%;
    background: #fff;
    box-shadow: 0 2px 4px rgba(15, 23, 42, 0.22);
    transition: transform 0.22s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .switch:checked { background: var(--accent); border-color: transparent; }
  .switch:checked::after { transform: translateX(16px); }

  /* Inputs & Form Controls */
  input[type="text"], input[type="number"], input[type="password"], input:not([type]), select {
    padding: 7px 11px;
    border-radius: 12px;
    border: 1px solid var(--bd);
    background: var(--input-bg);
    color: var(--ink);
    font-size: 12.5px;
    outline: none;
    transition: border-color 0.2s ease, box-shadow 0.2s ease;
  }
  input:focus, select:focus {
    border-color: var(--accent);
    box-shadow: 0 0 0 3px var(--accent-soft);
  }
  .field {
    display: flex;
    flex-direction: column;
    gap: 5px;
    flex: 1;
    min-width: 130px;
  }
  .field span {
    font-size: 11.5px;
    font-weight: 600;
    color: var(--muted);
  }
  .form-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 10px;
    margin-bottom: 12px;
  }

  /* Stat Tiles & Team Cards */
  .tile {
    background: var(--glass);
    backdrop-filter: blur(22px) saturate(160%);
    -webkit-backdrop-filter: blur(22px) saturate(160%);
    border: 1px solid var(--bd);
    border-radius: 22px;
    padding: 14px 16px;
    box-shadow: var(--shadow-card);
  }
  .tile-label { font-size: 11.5px; font-weight: 600; color: var(--muted); margin-bottom: 4px; }
  .tile-val { font-size: 20px; font-weight: 800; letter-spacing: -0.02em; font-variant-numeric: tabular-nums; }
  .tile-sub { font-size: 11.5px; color: var(--muted); margin-top: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

  .team-card.red-tint { background: linear-gradient(155deg, var(--red-soft), var(--glass) 48%); }
  .team-card.blue-tint { background: linear-gradient(155deg, var(--blue-soft), var(--glass) 48%); }
  .team-tag {
    display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px; border-radius: 999px; font-size: 11.5px; font-weight: 700;
  }
  .team-tag.red { background: var(--red-soft); color: var(--red); }
  .team-tag.blue { background: var(--blue-soft); color: var(--blue); }

  .kv-mini-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 8px;
    margin-top: 12px;
  }
  @media (max-width: 680px) { .kv-mini-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  .kv-mini {
    background: var(--glass-subtle);
    border: 1px solid var(--bd);
    border-radius: 15px;
    padding: 9px 11px;
  }
  .kv-mini span { display: block; font-size: 11px; color: var(--muted); font-weight: 600; }
  .kv-mini strong { font-size: 15px; font-weight: 700; font-variant-numeric: tabular-nums; }

  /* Trigger Deck */
  .trig-group { margin-bottom: 12px; }
  .trig-group:last-child { margin-bottom: 0; }
  .trig-group-title { font-size: 11.5px; font-weight: 700; color: var(--muted); margin-bottom: 7px; text-transform: uppercase; letter-spacing: 0.03em; }
  .trig-wrap { display: flex; flex-wrap: wrap; gap: 7px; }

  /* Tables & Logs */
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
  th, td { text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--bd); white-space: nowrap; }
  th { color: var(--muted); font-weight: 700; font-size: 11.5px; }
  tr:last-child td { border-bottom: 0; }
  .mini-bar-row { display: flex; align-items: center; gap: 8px; font-variant-numeric: tabular-nums; }
  .mini-bar { width: 62px; height: 6px; border-radius: 999px; background: var(--pill-bg); overflow: hidden; }

  .log-box {
    max-height: 310px;
    overflow: auto;
    background: var(--log-bg);
    border: 1px solid var(--bd);
    border-radius: 18px;
    padding: 8px 12px;
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    font-size: 12px;
  }
  .log-item {
    padding: 7px 2px;
    border-bottom: 1px solid var(--bd);
    display: flex;
    align-items: flex-start;
    gap: 8px;
    line-height: 1.4;
  }
  .log-item:last-child { border-bottom: 0; }
  .tag {
    display: inline-flex;
    align-items: center;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 700;
    background: var(--pill-bg);
    color: var(--ink-2);
    flex-shrink: 0;
  }
  .tag.ok { background: var(--ok-soft); color: var(--ok); }
  .tag.warn { background: var(--warn-soft); color: var(--warn); }
  .tag.bad { background: var(--bad-soft); color: var(--bad); }
  .tag.info { background: var(--accent-soft); color: var(--accent); }

  /* Video Playlist */
  .dir-banner {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    padding: 10px 13px;
    border-radius: 16px;
    background: var(--glass-subtle);
    border: 1px solid var(--bd);
    margin-bottom: 12px;
    font-size: 12px;
  }
  .playlist {
    display: flex;
    flex-direction: column;
    gap: 7px;
    max-height: 320px;
    overflow: auto;
    padding-right: 2px;
  }
  .pl-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    padding: 10px 13px;
    border-radius: 16px;
    border: 1px solid var(--bd);
    background: var(--glass-subtle);
    cursor: pointer;
    text-align: left;
    color: var(--ink);
    transition: all 0.2s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .pl-item:hover { background: var(--glass-strong); border-color: rgba(37, 99, 235, 0.35); transform: translateX(2px); }
  .pl-item.active { background: var(--accent-soft); border-color: rgba(37, 99, 235, 0.45); }
  .pl-meta { display: flex; align-items: center; gap: 10px; min-width: 0; }
  .pl-name { font-weight: 600; font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .empty-drop {
    padding: 26px 18px;
    border-radius: 20px;
    border: 1.5px dashed var(--bd);
    background: var(--glass-subtle);
    text-align: center;
    color: var(--muted);
  }
  .alert-box {
    margin-top: 12px;
    padding: 10px 13px;
    border-radius: 15px;
    background: var(--warn-soft);
    border: 1px solid rgba(217, 119, 6, 0.25);
    color: var(--warn);
    font-size: 12px;
    font-weight: 600;
    display: none;
  }
  .conn-strip {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 13px 18px;
    border-radius: 22px;
    background: linear-gradient(135deg, var(--accent-soft), var(--glass));
    border: 1px solid rgba(37, 99, 235, 0.22);
    margin-bottom: 14px;
    box-shadow: var(--shadow-card);
  }
  code {
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    font-size: 12px;
    padding: 2px 7px;
    border-radius: 8px;
    background: var(--pill-bg);
    border: 1px solid var(--bd);
  }
  .toast {
    position: fixed;
    bottom: 22px;
    left: 50%;
    transform: translateX(-50%) translateY(8px);
    background: var(--toast-bg);
    color: var(--toast-ink);
    backdrop-filter: blur(18px);
    padding: 9px 18px;
    border-radius: 999px;
    font-size: 12.5px;
    font-weight: 600;
    box-shadow: 0 12px 30px rgba(0, 0, 0, 0.22);
    opacity: 0;
    pointer-events: none;
    transition: all 0.25s cubic-bezier(0.22, 1, 0.36, 1);
    z-index: 99;
  }
  .toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
</style>
</head>
<body>
<div class="topbar-wrap">
  <header class="topbar">
    <div class="brand">
      <div class="emblem" aria-hidden="true">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 2L4 6v6c0 5.5 3.4 9.7 8 10.9 4.6-1.2 8-5.4 8-10.9V6l-8-4z"/>
          <circle cx="12" cy="12" r="2.8"/>
        </svg>
      </div>
      <div class="brand-title">
        <h1>RM2027 模拟赛事控制中心</h1>
        <div class="sub">MQTT 3.1.1 协议引擎 · 目录 HEVC 图传流 · 链路诊断</div>
      </div>
    </div>

    <div class="status-pills">
      <span class="pill" id="pMqtt"><span class="dot"></span>MQTT —</span>
      <span class="pill" id="pStage"><span class="dot"></span>阶段 —</span>
      <span class="pill" id="pVideo"><span class="dot"></span>图传 —</span>
      <span class="pill" id="pAuth"><span class="dot"></span>认证 —</span>
    </div>

    <div class="nav-group">
      <nav class="seg-nav" aria-label="主功能导航">
        <button class="tab active" data-tab="match">赛程总览</button>
        <button class="tab" data-tab="video">图传媒体库</button>
        <button class="tab" data-tab="auth">登录与客户端</button>
        <button class="tab" data-tab="diag">连接诊断</button>
      </nav>
      <button class="icon-btn" id="themeToggle" type="button" title="切换浅色/深色玻璃主题" aria-label="切换主题">
        <svg id="themeIcon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
        </svg>
      </button>
    </div>
  </header>
</div>

<main>
  <!-- TAB 1: MATCH -->
  <section class="panel active" id="tab-match">
    <div class="card">
      <div class="hero-bento">
        <div class="hero-clock-box">
          <div>
            <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap">
              <span class="stage-badge standby" id="stageBadge">待机 · 未开始</span>
              <span class="pill" id="roundBadge">第 1/3 局</span>
            </div>
            <div class="clock-digits" id="clockLabel">07:00</div>
            <div class="muted" id="elapsedLabel">比赛尚未启动 · 请点击下方按钮开始模拟赛程</div>
          </div>
          <div>
            <div class="progress-track" style="margin-bottom:14px">
              <div class="progress-fill" id="clockBar"></div>
            </div>
            <div class="ctrl-row">
              <button class="btn pri" type="button" onclick="trig('start_full_match')">开始完整赛程</button>
              <button class="btn ok" type="button" onclick="trig('start_battle')">直接开始 7 分钟</button>
              <button class="btn" type="button" id="btnPauseResume">暂停时钟</button>
              <button class="btn bad" type="button" onclick="trig('reset',{stage:0})">停止并重置</button>
            </div>
          </div>
        </div>

        <div class="hero-controls-box">
          <div class="ctrl-row">
            <span class="ctrl-label">推进模式</span>
            <div class="seg-group">
              <button class="seg-btn" type="button" data-mode="auto">常规自动</button>
              <button class="seg-btn" type="button" data-mode="manual">纯手动</button>
              <button class="seg-btn" type="button" data-mode="paused">暂停</button>
            </div>
          </div>
          <div class="ctrl-row">
            <span class="ctrl-label">规则随机</span>
            <div class="seg-group">
              <button class="seg-btn" type="button" data-rand="off">关闭</button>
              <button class="seg-btn" type="button" data-rand="low">低频</button>
              <button class="seg-btn" type="button" data-rand="normal">标准</button>
              <button class="seg-btn" type="button" data-rand="high">高频对抗</button>
            </div>
          </div>
          <div class="ctrl-row">
            <span class="ctrl-label">时间倍速</span>
            <div class="seg-group">
              <button class="seg-btn" type="button" data-speed="0.5">0.5×</button>
              <button class="seg-btn" type="button" data-speed="1">1×</button>
              <button class="seg-btn" type="button" data-speed="2">2×</button>
              <button class="seg-btn" type="button" data-speed="5">5×</button>
              <button class="seg-btn" type="button" data-speed="10">10×</button>
            </div>
          </div>
          <div class="ctrl-row" style="gap:18px;padding:4px 0">
            <label class="switch-label"><input class="switch" type="checkbox" id="chkVideoRules" checked> 规则联动断图传</label>
            <label class="switch-label"><input class="switch" type="checkbox" id="chkAutoLoop"> 赛后自动重开</label>
          </div>
          <div class="ctrl-row" style="padding-top:4px;border-top:1px solid var(--bd)">
            <span class="ctrl-label">阶段直达</span>
            <button class="btn sm" type="button" onclick="trig('set_stage',{stage:0})">未开始</button>
            <button class="btn sm" type="button" onclick="trig('set_stage',{stage:1,countdown_s:15})">准备阶段</button>
            <button class="btn sm" type="button" onclick="trig('set_stage',{stage:2,countdown_s:5})">15s 自检</button>
            <button class="btn sm warn" type="button" onclick="trig('set_stage',{stage:3,countdown_s:5})">5s 倒计时</button>
            <button class="btn sm" type="button" onclick="trig('set_stage',{stage:5,countdown_s:15})">结算</button>
            <input id="jumpSec" type="number" value="180" min="0" max="420" style="width:74px" aria-label="跳转比赛秒数">
            <button class="btn sm" type="button" onclick="trig('jump_time',{elapsed_s:Number($('jumpSec').value)})">跳转(s)</button>
          </div>
        </div>
      </div>
    </div>

    <!-- Red vs Blue Scoreboard -->
    <div class="bento-grid-2">
      <div class="card team-card red-tint">
        <div class="card-head" style="margin-bottom:10px">
          <span class="team-tag red">红方阵营 RED</span>
          <span class="muted" id="redScoreTag">基地护盾 —</span>
        </div>
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:6px">
          <strong style="font-size:22px;font-variant-numeric:tabular-nums" id="redBaseHp">10000 / 10000</strong>
          <span class="pill" id="redLambda">λ = 1.000</span>
        </div>
        <div class="progress-track"><div class="progress-fill red" id="redBaseBar" style="width:100%"></div></div>
        <div class="kv-mini-grid" id="redTeamKvs"></div>
      </div>

      <div class="card team-card blue-tint">
        <div class="card-head" style="margin-bottom:10px">
          <span class="team-tag blue">蓝方阵营 BLUE</span>
          <span class="muted" id="blueScoreTag">基地护盾 —</span>
        </div>
        <div style="display:flex;align-items:baseline;justify-content:space-between;margin-bottom:6px">
          <strong style="font-size:22px;font-variant-numeric:tabular-nums" id="blueBaseHp">10000 / 10000</strong>
          <span class="pill" id="blueLambda">λ = 1.000</span>
        </div>
        <div class="progress-track"><div class="progress-fill blue" id="blueBaseBar" style="width:100%"></div></div>
        <div class="kv-mini-grid" id="blueTeamKvs"></div>
      </div>
    </div>

    <div class="bento-grid-2">
      <div class="card">
        <div class="card-head">
          <h2 class="card-title">规则事件与合规手动触发</h2>
          <span class="muted">随时可点按注入</span>
        </div>
        <div class="trig-group">
          <div class="trig-group-title">合规遮挡与断图传</div>
          <div class="trig-wrap">
            <button class="btn sm warn" type="button" onclick="trig('dart_gate_open')">敌方飞镖开闸 (37s)</button>
            <button class="btn sm bad" type="button" onclick="trig('dart_hit',{target_id:3})">飞镖命中致盲 (10s)</button>
            <button class="btn sm warn" type="button" onclick="trig('yellow_card',{seconds:5})">黄牌遮挡 (5s)</button>
            <button class="btn sm warn" type="button" onclick="trig('second_yellow_card',{seconds:10})">二次黄牌 (10s)</button>
            <button class="btn sm solid-bad" type="button" onclick="trig('red_card')">红牌锁定</button>
            <button class="btn sm bad" type="button" onclick="trig('overheat',{excess:45})">枪口超热断图传</button>
            <button class="btn sm ok" type="button" onclick="trig('clear_compliance')">解除全部遮挡</button>
          </div>
        </div>
        <div class="trig-group">
          <div class="trig-group-title">阵地与机关科技</div>
          <div class="trig-wrap">
            <button class="btn sm" type="button" onclick="trig('flip_control',{kind:'sentinel',home:'blue'})">翻转蓝哨占领</button>
            <button class="btn sm" type="button" onclick="trig('flip_control',{kind:'sentinel',home:'red'})">翻转红哨占领</button>
            <button class="btn sm" type="button" onclick="trig('mechanism_progress',{mechanism_id:4,seconds:5})">机制进度条</button>
            <button class="btn sm" type="button" onclick="trig('rune_activate')">激活能量机关</button>
            <button class="btn sm" type="button" onclick="trig('tech_core_step')">重装装配 (+1级)</button>
            <button class="btn sm" type="button" onclick="trig('install_42mm')">安装 42mm</button>
          </div>
        </div>
        <div class="trig-group">
          <div class="trig-group-title">对抗与机器人状态</div>
          <div class="trig-wrap">
            <button class="btn sm" type="button" onclick="trig('armor_hit',{count:1})">RMH1 +1</button>
            <button class="btn sm" type="button" onclick="trig('armor_hit',{count:10})">RMH1 +10</button>
            <button class="btn sm" type="button" onclick="trig('kill_robot',{victim_id:103,killer_id:3})">击杀蓝 3</button>
            <button class="btn sm" type="button" onclick="trig('respawn_finish')">己方立即复活</button>
            <button class="btn sm" type="button" onclick="trig('set_base_hp',{team:'red',hp:5000})">红基地=5000</button>
            <button class="btn sm" type="button" onclick="trig('base_armor_open',{team:'blue'})">敌基地开护甲</button>
            <button class="btn sm" type="button" onclick="trig('module_fault',{module:'video_transmission',state:0})">图传模块掉线</button>
            <button class="btn sm" type="button" onclick="trig('grant_buff',{buff_type:2,duration:30})">授予防御 Buff</button>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2 class="card-title">实时事件流 & 上行指令</h2>
          <span class="muted">最新 30 条</span>
        </div>
        <div class="log-box" id="eventLog">暂无事件</div>
      </div>
    </div>

    <div class="card">
      <div class="card-head">
        <h2 class="card-title">8 辆机器人实时遥测</h2>
        <span class="muted">1Hz / 5Hz 下行数据源状态</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>ID</th><th>阵营</th><th>血量</th><th>热量</th><th>弹丸</th><th>等级</th><th>坐标 (m)</th><th>状态</th></tr></thead>
          <tbody id="robotsBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- TAB 2: VIDEO LIBRARY -->
  <section class="panel" id="tab-video">
    <div class="bento-grid-4" id="videoKvs"></div>

    <div class="bento-grid-2">
      <div class="card">
        <div class="card-head">
          <h2 class="card-title">本地视频目录播放列表</h2>
          <div style="display:flex;gap:7px">
            <button class="btn sm" type="button" onclick="videoAction('prev')">上一视频</button>
            <button class="btn sm" type="button" onclick="videoAction('next')">下一视频</button>
            <button class="btn sm pri" type="button" onclick="videoAction('rescan')">刷新列表</button>
          </div>
        </div>

        <div class="ctrl-row" style="margin-bottom:10px">
          <label class="field" style="flex:1">
            <span>视频目录路径（相对工作目录或绝对路径，修改后自动扫描全部视频）</span>
            <input id="vDir" value="videos" placeholder="例如 videos 或 /path/to/videos">
          </label>
          <button class="btn pri" type="button" style="align-self:flex-end" onclick="applyVideoDir()">应用目录</button>
        </div>

        <div class="dir-banner">
          <div style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            <span class="muted">扫描路径：</span><code id="vDirResolved">—</code>
          </div>
          <div class="seg-group" style="flex-shrink:0">
            <button class="seg-btn" type="button" data-loop="all">目录轮播</button>
            <button class="seg-btn" type="button" data-loop="single">单文件循环</button>
          </div>
        </div>

        <div id="videoPlaylistWrap"></div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2 class="card-title">UDP :3334 推流参数与传输控制</h2>
          <span class="muted">无需开赛，启动即推流</span>
        </div>

        <div class="ctrl-row" style="gap:18px;margin-bottom:14px">
          <label class="switch-label"><input class="switch" type="checkbox" id="vEnabled" checked> 启用图传推流</label>
          <label class="switch-label"><input class="switch" type="checkbox" id="vMirror" checked> 自动镜像推给远端已连客户端 IP</label>
        </div>

        <div class="form-grid">
          <label class="field"><span>目标 IP</span><input id="vHost" value="127.0.0.1"></label>
          <label class="field"><span>UDP 端口</span><input id="vPort" type="number" value="3334"></label>
          <label class="field"><span>编码后端</span>
            <select id="vBackend">
              <option value="auto">自动匹配 (GStreamer/FFmpeg/裸流)</option>
              <option value="gstreamer">GStreamer (x265enc)</option>
              <option value="ffmpeg">FFmpeg (libx265)</option>
              <option value="file">裸流直读 (.hevc/.h265)</option>
              <option value="synthetic">合成测试帧 (兜底)</option>
            </select>
          </label>
          <label class="field"><span>输出分辨率</span>
            <select id="vRes">
              <option value="1280x720" selected>1280×720</option>
              <option value="1920x1080">1920×1080</option>
              <option value="960x540">960×540</option>
            </select>
          </label>
          <label class="field"><span>目标帧率 (FPS)</span><input id="vFps" type="number" value="30" min="1" max="60"></label>
          <label class="field"><span>编码码率 (kbps)</span><input id="vBitrate" type="number" value="2500" min="200" max="20000"></label>
          <label class="field"><span>模拟丢包率 (%)</span><input id="vLoss" type="number" value="0" min="0" max="80" step="0.5"></label>
          <label class="field"><span>右上角叠加文字 (留空无水印)</span><input id="vOverlay" placeholder="留空保持原始画面"></label>
        </div>

        <div class="ctrl-row" style="justify-content:space-between">
          <span class="muted">支持 <code>.mp4 .mkv .mov .avi .ts .webm .hevc .h265</code></span>
          <button class="btn pri" type="button" onclick="applyVideo()">应用推流设置</button>
        </div>
        <div class="alert-box" id="videoErr"></div>
      </div>
    </div>
  </section>

  <!-- TAB 3: AUTH & CLIENTS -->
  <section class="panel" id="tab-auth">
    <div class="conn-strip">
      <div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px">
        <span class="tag info">客户端默认接入地址</span>
        <code id="connAddr">127.0.0.1:3333</code>
        <span class="muted">UDP 图传目标：<code id="connVideoAddr">127.0.0.1:3334</code></span>
      </div>
      <button class="btn sm pri" type="button" id="btnCopyConn">复制 MQTT 地址</button>
    </div>

    <div class="bento-grid-2">
      <div class="card">
        <div class="card-head">
          <h2 class="card-title">MQTT 登录认证策略与故障注入</h2>
          <span class="muted">实时生效</span>
        </div>
        <div class="ctrl-row" style="gap:16px;margin-bottom:12px">
          <label class="field"><span>认证模式</span>
            <select id="aMode">
              <option value="open">开放接入（允许匿名）</option>
              <option value="required">强制用户名密码登录</option>
            </select>
          </label>
          <label class="switch-label" style="margin-top:16px"><input class="switch" type="checkbox" id="aStrictId"> 严格校验 RM2027 Robot ID</label>
        </div>
        <div class="form-grid">
          <label class="field"><span>登录用户名</span><input id="aUser" placeholder="例如 rm2027"></label>
          <label class="field"><span>登录密码</span><input id="aPass" type="text" placeholder="设置登录密码"></label>
        </div>
        <div class="ctrl-row">
          <label class="field"><span>故障注入 CONNACK</span>
            <select id="aFault">
              <option value="">无（正常判定）</option>
              <option value="1">0x01 协议版本不兼容</option>
              <option value="2">0x02 Client ID 被拒绝</option>
              <option value="3">0x03 服务端不可用</option>
              <option value="4">0x04 用户名或密码错误</option>
              <option value="5">0x05 未授权</option>
            </select>
          </label>
          <label class="switch-label" style="margin-top:18px"><input class="switch" type="checkbox" id="aFaultOnce" checked> 仅触发一次</label>
          <button class="btn pri" type="button" style="margin-top:18px" onclick="applyAuth()">保存认证策略</button>
        </div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2 class="card-title">已连接客户端 (<span id="clientCnt">0</span>)</h2>
          <span class="muted">支持红/蓝双视角同时接入</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>ClientID</th><th>阵营</th><th>来源</th><th>订阅覆盖</th><th>收/发包</th><th>操作</th></tr></thead>
            <tbody id="clientsBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-head">
        <h2 class="card-title">最近连接与登录审计日志</h2>
        <span class="muted">用于定位握手失败或断线原因</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>状态</th><th>来源</th><th>ClientID</th><th>用户名</th><th>详情与排错建议</th></tr></thead>
          <tbody id="auditBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- TAB 4: DIAGNOSTICS -->
  <section class="panel" id="tab-diag">
    <div class="bento-grid-2">
      <div class="card">
        <div class="card-head">
          <h2 class="card-title">环境、端口与客户端配置排查</h2>
          <button class="btn sm pri" type="button" onclick="loadDiagnostics()">刷新诊断</button>
        </div>
        <div id="diagSummary" class="muted">点击「刷新诊断」扫描本机网卡、监听端口与客户端配置...</div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2 class="card-title">端到端 MQTT 登录主动探测</h2>
          <button class="btn sm ok" type="button" onclick="runProbe()">执行握手探测</button>
        </div>
        <div class="form-grid" style="margin-bottom:10px">
          <label class="field"><span>目标 Host</span><input id="prHost" value="127.0.0.1"></label>
          <label class="field"><span>端口</span><input id="prPort" type="number" value="3333"></label>
          <label class="field"><span>Robot ID</span><input id="prId" value="3"></label>
          <label class="field"><span>用户名 (可选)</span><input id="prUser" placeholder="匿名留空"></label>
          <label class="field"><span>密码 (可选)</span><input id="prPass" type="password" placeholder="匿名留空"></label>
        </div>
        <div class="log-box" id="probeOut">等待探测...</div>
      </div>
    </div>
  </section>
</main>
<div class="toast" id="toast"></div>

<script>
const $ = id => document.getElementById(id);
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const isFocused = id => document.activeElement && document.activeElement.id === id;

function fmtBytes(n) {
  const b = Number(n || 0);
  if (b >= 1024 * 1024) return (b / (1024 * 1024)).toFixed(1) + ' MB';
  if (b >= 1024) return Math.round(b / 1024) + ' KB';
  return b + ' B';
}

function toast(msg) {
  const el = $('toast');
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(window._tt);
  window._tt = setTimeout(() => el.classList.remove('show'), 2500);
}

async function api(path, body) {
  const opts = body !== undefined
    ? { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
    : {};
  const r = await fetch(path, opts);
  return await r.json();
}

async function trig(action, params = {}) {
  const res = await api('/api/trigger', { action, params });
  toast(res.message || res.error || action);
  refreshState();
}

// Theme toggle (light/dark frosted glass persisted in localStorage)
function applyTheme(theme) {
  const t = theme === 'dark' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', t);
  localStorage.setItem('rm2027_sim_theme', t);
  $('themeIcon').innerHTML = t === 'dark'
    ? '<circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>'
    : '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>';
}
applyTheme(localStorage.getItem('rm2027_sim_theme') || 'light');
$('themeToggle').addEventListener('click', () => {
  const cur = document.documentElement.getAttribute('data-theme');
  applyTheme(cur === 'dark' ? 'light' : 'dark');
});

// Tab switching
document.querySelectorAll('nav .tab').forEach(b => b.addEventListener('click', () => {
  document.querySelectorAll('nav .tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
  b.classList.add('active');
  $('tab-' + b.dataset.tab).classList.add('active');
  if (b.dataset.tab === 'diag') loadDiagnostics();
}));

// Match controls
document.querySelectorAll('[data-mode]').forEach(b => b.addEventListener('click', async () => {
  await api('/api/mode', { mode: b.dataset.mode });
  toast('推进模式已切换: ' + b.dataset.mode);
  refreshState();
}));
document.querySelectorAll('[data-rand]').forEach(b => b.addEventListener('click', async () => {
  await api('/api/mode', { random_events: b.dataset.rand });
  toast('随机事件频率: ' + b.dataset.rand);
  refreshState();
}));
document.querySelectorAll('[data-speed]').forEach(b => b.addEventListener('click', async () => {
  await api('/api/mode', { speed: Number(b.dataset.speed) });
  toast('倍速已设为 ' + b.dataset.speed + '×');
  refreshState();
}));
$('btnPauseResume').addEventListener('click', async () => {
  const cur = window._latestState?.simulator?.mode || 'auto';
  const next = cur === 'paused' ? 'auto' : 'paused';
  await api('/api/mode', { mode: next });
  toast(next === 'paused' ? '已暂停比赛时钟' : '已恢复常规自动模拟');
  refreshState();
});
$('chkVideoRules').addEventListener('change', async () => {
  await api('/api/mode', { respect_video_rules: $('chkVideoRules').checked });
  refreshState();
});
$('chkAutoLoop').addEventListener('change', async () => {
  await api('/api/mode', { auto_loop: $('chkAutoLoop').checked });
  refreshState();
});

// Video controls & playlist
document.querySelectorAll('[data-loop]').forEach(b => b.addEventListener('click', async () => {
  await api('/api/video', { loop_mode: b.dataset.loop });
  toast(b.dataset.loop === 'single' ? '已切换为单文件循环' : '已切换为目录轮播');
  refreshState();
}));

async function videoAction(action) {
  const res = await api('/api/video', { action });
  if (action === 'rescan') toast('目录已重新扫描 (' + (res.video?.available_files?.length ?? 0) + ' 个视频)');
  else if (action === 'next') toast('切换到下一视频: ' + (res.video?.current_file || '无'));
  else if (action === 'prev') toast('切换到上一视频: ' + (res.video?.current_file || '无'));
  refreshState();
}

async function applyVideoDir() {
  const dir = $('vDir').value.trim() || 'videos';
  const res = await api('/api/video', { video_dir: dir, video_file: '' });
  toast('已应用视频目录，发现 ' + (res.video?.available_files?.length ?? 0) + ' 个视频');
  refreshState();
}

async function playSpecificFile(name) {
  const res = await api('/api/video', { action: 'play_file', video_file: name });
  toast('正在播放: ' + (res.video?.current_file || name));
  refreshState();
}

async function applyVideo() {
  const [w, h] = $('vRes').value.split('x').map(Number);
  const res = await api('/api/video', {
    enabled: $('vEnabled').checked,
    auto_mirror_clients: $('vMirror').checked,
    target_host: $('vHost').value.trim(),
    target_port: Number($('vPort').value),
    backend: $('vBackend').value,
    width: w, height: h,
    fps: Number($('vFps').value),
    bitrate_kbps: Number($('vBitrate').value),
    loss_percent: Number($('vLoss').value),
    video_dir: $('vDir').value.trim() || 'videos',
    overlay_text: $('vOverlay').value.trim(),
  });
  toast('图传推流设置已更新 (' + (res.video?.active_backend || '') + ')');
  refreshState();
}

// Auth & clients
async function applyAuth() {
  const faultVal = $('aFault').value;
  const res = await api('/api/auth', {
    auth_mode: $('aMode').value,
    strict_client_id: $('aStrictId').checked,
    username: $('aUser').value.trim(),
    password: $('aPass').value,
    fault_connack: faultVal === '' ? null : Number(faultVal),
    fault_once: $('aFaultOnce').checked,
  });
  toast('MQTT 认证策略已更新: ' + res.auth.auth_mode);
  refreshState();
}

async function kickClient(cid) {
  await api('/api/auth', { kick_client_id: cid });
  toast('已断开客户端 ' + cid);
  refreshState();
}

$('clientsBody').addEventListener('click', e => {
  const btn = e.target.closest('[data-kick]');
  if (btn) kickClient(btn.dataset.kick);
});

$('videoPlaylistWrap').addEventListener('click', e => {
  const item = e.target.closest('[data-file]');
  if (item) playSpecificFile(item.dataset.file);
});

$('btnCopyConn').addEventListener('click', async () => {
  const txt = $('connAddr').textContent.trim();
  try {
    await navigator.clipboard.writeText(txt);
    toast('已复制接入地址: ' + txt);
  } catch {
    toast('接入地址: ' + txt);
  }
});

// Diagnostics & Probe
async function loadDiagnostics() {
  const d = await api('/api/diagnostics');
  const issuesHtml = (d.issues || []).length
    ? d.issues.map(i => {
        const cls = i.level === 'error' ? 'bad' : i.level === 'warn' ? 'warn' : 'info';
        return `<div class="log-item"><span class="tag ${cls}">${esc(i.level.toUpperCase())}</span><div><strong>${esc(i.title)}</strong><div class="muted">${esc(i.detail)}</div></div></div>`;
      }).join('')
    : '<div class="log-item"><span class="tag ok">OK</span><div>未检测到端口冲突或客户端配置差异。</div></div>';
  const termsHtml = (d.running_terminals || []).length
    ? d.running_terminals.map(t => `<div style="margin-top:4px"><code>API :${esc(t.api_port)}</code> · connected=<b>${esc(t.mqtt_connected)}</b> (${esc(t.mqtt_host)}:${esc(t.mqtt_port)}) · topics=${esc(t.active_topics)}/23 · video=${esc(t.video_fps)}fps ${t.mqtt_last_error ? '· err=' + esc(t.mqtt_last_error) : ''}</div>`).join('')
    : '<span class="muted">本机暂未检测到运行中的自定义终端（可使用右侧主动探测工具）。</span>';
  const ifsHtml = (d.network_interfaces || []).map(x => `<code>${esc(x.iface)}: ${esc(x.ip)}/${esc(x.prefix)}</code>`).join(' ');
  const cci = d.client_connect_info || {};
  $('diagSummary').innerHTML = `
    <div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px">
      <span class="tag info">MQTT ${esc(cci.mqtt_host)}:${esc(cci.mqtt_port)}</span>
      <span class="tag">UDP图传 :${esc(cci.video_udp_port)}</span>
      <span class="tag">认证: ${esc(cci.auth_mode)}</span>
    </div>
    <div style="margin-bottom:8px"><span class="muted">本机 IPv4 网卡：</span>${ifsHtml}</div>
    <div style="margin-bottom:8px"><span class="muted">CLI 主动探测：</span><code>${esc(d.recommended_command)}</code></div>
    <div style="margin-bottom:10px"><span class="muted">本机活跃终端：</span>${termsHtml}</div>
    <div class="log-box">${issuesHtml}</div>
  `;
}

async function runProbe() {
  $('probeOut').textContent = '正在执行 TCP -> MQTT 3.1.1 握手探测...';
  const res = await api('/api/probe', {
    host: $('prHost').value.trim(),
    port: Number($('prPort').value),
    client_id: $('prId').value.trim(),
    username: $('prUser').value.trim(),
    password: $('prPass').value,
  });
  const html = (res.steps || []).map(s => {
    const cls = s.status === 'ok' ? 'ok' : s.status === 'warn' ? 'warn' : 'bad';
    return `<div class="log-item"><span class="tag ${cls}">${esc(s.status.toUpperCase())}</span><div><strong>${esc(s.name)}</strong>: ${esc(s.detail)}${s.hint ? `<div style="color:var(--warn)">建议: ${esc(s.hint)}</div>` : ''}</div></div>`;
  }).join('');
  $('probeOut').innerHTML = html || esc(JSON.stringify(res));
}

function teamMetricsHtml(t) {
  return [
    ['前哨血量', t.outpost_hp],
    ['控制得分', t.control_score],
    ['金币 / 总计', `${t.economy} / ${t.total_economy}`],
    ['科技 / 42mm', `Lv${t.tech_level} ${t.launcher_unlocked ? '✓' : '·'}`],
  ].map(([k, v]) => `<div class="kv-mini"><span>${esc(k)}</span><strong>${esc(v)}</strong></div>`).join('');
}

let firstSync = true;
async function refreshState() {
  try {
    const s = await api('/api/state');
    window._latestState = s;
    const sim = s.simulator, mq = s.mqtt, vid = s.video, auth = s.auth;
    const connectHost = mq.host === '0.0.0.0' ? '127.0.0.1' : mq.host;

    // Top Status Capsules
    $('pMqtt').innerHTML = `<span class="dot"></span>MQTT ${esc(connectHost)}:${esc(mq.port)} · ${s.clients.length} 在线`;
    $('pMqtt').className = 'pill ' + (s.clients.length ? 'ok' : 'info');
    $('pStage').innerHTML = `<span class="dot"></span>${esc(sim.stage_name)}`;
    $('pStage').className = 'pill ' + (sim.stage === 4 ? (sim.mode === 'paused' ? 'warn' : 'ok') : (sim.stage === 0 ? '' : 'info'));

    if (vid.cut_active) {
      $('pVideo').innerHTML = `<span class="dot"></span>图传断流: ${esc(vid.cut_reason)}`;
      $('pVideo').className = 'pill bad';
    } else if (vid.active_backend === 'waiting_files') {
      $('pVideo').innerHTML = `<span class="dot"></span>图传待放入视频`;
      $('pVideo').className = 'pill warn';
    } else if (!vid.config.enabled) {
      $('pVideo').innerHTML = `<span class="dot"></span>图传已关闭`;
      $('pVideo').className = 'pill';
    } else {
      $('pVideo').innerHTML = `<span class="dot"></span>${esc(vid.current_file || vid.active_backend)} · ${esc(vid.current_fps)}fps`;
      $('pVideo').className = 'pill ' + (vid.current_fps > 0 ? 'ok' : 'info');
    }

    $('pAuth').innerHTML = `<span class="dot"></span>${auth.auth_mode === 'required' ? '认证: 账密必选' : '认证: 开放'}`;
    $('pAuth').className = 'pill ' + (auth.auth_mode === 'required' ? 'warn' : 'ok');

    // Hero Match Card
    const badge = $('stageBadge');
    if (sim.stage === 0) {
      badge.textContent = '待机 · 未开始';
      badge.className = 'stage-badge standby';
    } else if (sim.mode === 'paused') {
      badge.textContent = `${sim.stage_name} · 已暂停`;
      badge.className = 'stage-badge paused';
    } else if (sim.stage === 4) {
      badge.textContent = `${sim.stage_name} (${sim.mode}/${sim.random_events})`;
      badge.className = 'stage-badge live';
    } else if (sim.stage === 5) {
      badge.textContent = sim.stage_name;
      badge.className = 'stage-badge done';
    } else {
      badge.textContent = sim.stage_name;
      badge.className = 'stage-badge standby';
    }

    $('roundBadge').textContent = `第 ${sim.round} 局 · 红 ${sim.score.red} : ${sim.score.blue} 蓝`;
    const rem = sim.stage === 4 ? Math.max(0, Math.round(sim.remaining_s)) : Math.max(0, Math.round(sim.stage_countdown_s));
    $('clockLabel').textContent = `${String(Math.floor(rem / 60)).padStart(2, '0')}:${String(rem % 60).padStart(2, '0')}`;
    if (sim.stage === 0) {
      $('elapsedLabel').textContent = '待机就绪 · 点击「开始完整赛程」或「直接开始 7 分钟」启动模拟';
      $('clockBar').style.width = '0%';
    } else if (sim.stage === 4) {
      $('elapsedLabel').textContent = `已进行 ${Math.round(sim.elapsed_s)}s / 420s · 倍速 ${sim.speed}×`;
      $('clockBar').style.width = Math.min(100, (sim.elapsed_s / 420.0) * 100).toFixed(1) + '%';
    } else if (sim.stage === 5 && sim.mode === 'paused') {
      $('elapsedLabel').textContent = `本局已结束并自动暂停 · 比分 红 ${sim.score.red} : ${sim.score.blue} 蓝（点击开赛或重置可开启下一局）`;
      $('clockBar').style.width = '100%';
    } else {
      $('elapsedLabel').textContent = `${sim.stage_name} · 倒计时 ${Math.round(sim.stage_countdown_s)}s`;
      $('clockBar').style.width = '100%';
    }
    $('btnPauseResume').textContent = sim.mode === 'paused' ? '恢复时钟' : '暂停时钟';

    document.querySelectorAll('[data-mode]').forEach(b => b.classList.toggle('active', b.dataset.mode === sim.mode));
    document.querySelectorAll('[data-rand]').forEach(b => b.classList.toggle('active', b.dataset.rand === sim.random_events));
    document.querySelectorAll('[data-speed]').forEach(b => b.classList.toggle('active', Number(b.dataset.speed) === Number(sim.speed)));
    $('chkVideoRules').checked = Boolean(sim.respect_video_rules);
    $('chkAutoLoop').checked = Boolean(sim.auto_loop);

    // Team Scoreboard Cards
    const r = sim.teams.red, b = sim.teams.blue;
    $('redBaseHp').textContent = `${r.base_hp} / 10000`;
    $('redBaseBar').style.width = Math.max(0, Math.min(100, r.base_hp / 100)).toFixed(1) + '%';
    $('redLambda').textContent = `λ = ${r.lambda}`;
    $('redScoreTag').textContent = `飞镖剩余 ${r.darts_remaining} · 阵地 ${sim.fields.sentinel_red.owner === 'red' ? '红占' : '蓝占'}`;
    $('redTeamKvs').innerHTML = teamMetricsHtml(r);

    $('blueBaseHp').textContent = `${b.base_hp} / 10000`;
    $('blueBaseBar').style.width = Math.max(0, Math.min(100, b.base_hp / 100)).toFixed(1) + '%';
    $('blueLambda').textContent = `λ = ${b.lambda}`;
    $('blueScoreTag').textContent = `飞镖剩余 ${b.darts_remaining} · 阵地 ${sim.fields.sentinel_blue.owner === 'blue' ? '蓝占' : '红占'}`;
    $('blueTeamKvs').innerHTML = teamMetricsHtml(b);

    // Robots Telemetry Table
    $('robotsBody').innerHTML = Object.values(sim.robots).map(rb => {
      const hpPct = rb.max_hp > 0 ? Math.max(0, Math.min(100, (rb.hp / rb.max_hp) * 100)) : 0;
      const heatPct = rb.max_heat > 0 ? Math.max(0, Math.min(100, (rb.heat / rb.max_heat) * 100)) : 0;
      let stCls = 'ok', stText = '正常';
      if (rb.red_card) { stCls = 'bad'; stText = '红牌'; }
      else if (rb.penalty_blind_s > 0) { stCls = 'warn'; stText = `黄牌 ${rb.penalty_blind_s}s`; }
      else if (rb.overheat_locked) { stCls = 'bad'; stText = '超热断流'; }
      else if (rb.respawn_pending) { stCls = 'warn'; stText = '复活中'; }
      return `<tr>
        <td><strong>#${esc(rb.robot_id)}</strong></td>
        <td><span class="team-tag ${rb.team === 'red' ? 'red' : 'blue'}">${rb.team === 'red' ? '红方' : '蓝方'}</span></td>
        <td><div class="mini-bar-row"><div class="mini-bar"><div class="progress-fill hp" style="width:${hpPct}%"></div></div><span>${esc(rb.hp)}/${esc(rb.max_hp)}</span></div></td>
        <td><div class="mini-bar-row"><div class="mini-bar"><div class="progress-fill heat" style="width:${heatPct}%"></div></div><span>${esc(rb.heat)}/${esc(rb.max_heat)}</span></div></td>
        <td>${esc(rb.remaining_ammo)}</td>
        <td>L${esc(rb.level)}</td>
        <td><code>${esc(rb.x)}, ${esc(rb.y)}</code></td>
        <td><span class="tag ${stCls}">${esc(stText)}</span></td>
      </tr>`;
    }).join('');

    // Event Feed
    const evLogs = (sim.events || []).map(e => ({ ts: e.elapsed_s, kind: 'EVENT', badge: `E${e.event_id}`, msg: e.detail }));
    const cmdLogs = (sim.commands || []).map(c => ({ ts: c.elapsed_s, kind: 'CMD', badge: `${c.topic} #${c.robot_id}`, msg: c.summary }));
    const merged = [...evLogs, ...cmdLogs].slice(-30).reverse();
    $('eventLog').innerHTML = merged.length
      ? merged.map(l => `<div class="log-item"><span class="tag ${l.kind === 'CMD' ? 'info' : ''}">${esc(l.badge)}</span><div><span>${esc(l.msg)}</span> <span class="muted">T+${esc(l.ts)}s</span></div></div>`).join('')
      : '<div class="muted" style="padding:10px">暂无事件记录</div>';

    // Video KPIs & Playlist
    const videoTiles = [
      ['当前播放状态', vid.current_file || (vid.active_backend === 'waiting_files' ? '等待放入视频' : vid.active_backend), vid.status_message || '目录自动监测中'],
      ['实时输出速率', `${vid.current_fps} FPS`, `${vid.current_bitrate_kbps} kbps · 丢包 ${vid.config.loss_percent}%`],
      ['推流目标与后端', `${vid.config.target_host}:${vid.config.target_port}`, `后端: ${vid.active_backend} · 镜像 ${vid.config.auto_mirror_clients ? '开启' : '关闭'}`],
      ['推流帧与发包统计', `${vid.frames_sent} 帧`, `关键帧 ${vid.keyframes_sent} · UDP包 ${vid.packets_sent}`],
    ];
    $('videoKvs').innerHTML = videoTiles.map(([k, v, sub]) =>
      `<div class="tile"><div class="tile-label">${esc(k)}</div><div class="tile-val">${esc(v)}</div><div class="tile-sub">${esc(sub)}</div></div>`
    ).join('');

    $('vDirResolved').textContent = vid.video_dir_resolved || vid.config.video_dir;
    document.querySelectorAll('[data-loop]').forEach(b => b.classList.toggle('active', b.dataset.loop === (vid.config.loop_mode || 'all')));

    const files = vid.available_files || [];
    if (!files.length) {
      $('videoPlaylistWrap').innerHTML = `
        <div class="empty-drop">
          <div style="font-weight:700;color:var(--ink);margin-bottom:6px">目录中暂无可播放视频</div>
          <div style="margin-bottom:10px">将 <code>.mp4</code> <code>.mkv</code> <code>.mov</code> <code>.avi</code> <code>.ts</code> <code>.hevc</code> 文件放入下方目录，模拟服务器将在 1.5 秒内自动识别并循环推流：</div>
          <code>${esc(vid.video_dir_resolved || vid.config.video_dir)}</code>
        </div>`;
    } else {
      $('videoPlaylistWrap').innerHTML = '<div class="playlist">' + files.map(f => {
        const isActive = f.name === vid.current_file || f.path === vid.current_file_path;
        return `<button class="pl-item ${isActive ? 'active' : ''}" type="button" data-file="${esc(f.name)}">
          <div class="pl-meta">
            <span class="tag ${isActive ? 'info' : ''}">${esc(f.ext)}</span>
            <span class="pl-name">${esc(f.name)}</span>
          </div>
          <div style="display:flex;align-items:center;gap:8px;flex-shrink:0">
            <span class="muted">${esc(fmtBytes(f.size_bytes))}</span>
            ${isActive ? '<span class="tag ok">正在推流</span>' : ''}
          </div>
        </button>`;
      }).join('') + '</div>';
    }

    if (vid.last_error) {
      $('videoErr').textContent = '推流提示: ' + vid.last_error;
      $('videoErr').style.display = 'block';
    } else {
      $('videoErr').style.display = 'none';
    }

    // Auth & Clients
    $('connAddr').textContent = `${connectHost}:${mq.port}`;
    $('connVideoAddr').textContent = `${vid.config.target_host}:${vid.config.target_port}`;
    $('clientCnt').textContent = s.clients.length;
    $('clientsBody').innerHTML = s.clients.length
      ? s.clients.map(c =>
          `<tr><td><strong>${esc(c.client_id)}</strong></td><td><span class="team-tag ${c.team === 'red' ? 'red' : 'blue'}">${esc(c.team)}</span></td><td><code>${esc(c.peer)}</code></td><td>${esc(c.inbound_coverage)}/${esc(c.inbound_total)}</td><td>${esc(c.rx_packets)} / ${esc(c.tx_packets)}</td><td><button class="btn sm bad" type="button" data-kick="${esc(c.client_id)}">断开</button></td></tr>`
        ).join('')
      : '<tr><td colspan="6" class="muted">暂无客户端连接 · 可让终端连接 ' + esc(connectHost) + ':' + esc(mq.port) + '</td></tr>';

    $('auditBody').innerHTML = (s.audit || []).length
      ? (s.audit || []).map(a => {
          const cls = a.status === 'accepted' ? 'ok' : (a.status === 'replaced' ? 'warn' : 'bad');
          return `<tr><td><span class="tag ${cls}">${esc(a.status)}</span>${a.connack_code != null ? ` <code>0x${a.connack_code.toString(16).padStart(2, '0')}</code>` : ''}</td><td><code>${esc(a.peer)}</code></td><td>${esc(a.client_id || '—')}</td><td>${esc(a.username || '—')}</td><td>${esc(a.reason)}${a.hint ? ` — <span class="muted">${esc(a.hint)}</span>` : ''}</td></tr>`;
        }).join('')
      : '<tr><td colspan="5" class="muted">暂无握手日志</td></tr>';

    if (firstSync) {
      firstSync = false;
      $('prHost').value = connectHost;
      $('prPort').value = mq.port;
      $('vDir').value = vid.config.video_dir || 'videos';
      $('vEnabled').checked = Boolean(vid.config.enabled);
      $('vMirror').checked = Boolean(vid.config.auto_mirror_clients);
      $('vHost').value = vid.config.target_host;
      $('vPort').value = vid.config.target_port;
      $('vBackend').value = vid.config.backend;
      $('vRes').value = `${vid.config.width}x${vid.config.height}`;
      $('vFps').value = vid.config.fps;
      $('vBitrate').value = vid.config.bitrate_kbps;
      $('vLoss').value = vid.config.loss_percent;
      $('vOverlay').value = vid.config.overlay_text || '';
      $('aMode').value = auth.auth_mode;
      $('aStrictId').checked = Boolean(auth.strict_client_id);
    } else {
      if (!isFocused('vEnabled')) $('vEnabled').checked = Boolean(vid.config.enabled);
      if (!isFocused('vMirror')) $('vMirror').checked = Boolean(vid.config.auto_mirror_clients);
      if (!isFocused('vDir') && $('vDir').value !== (vid.config.video_dir || 'videos')) $('vDir').value = vid.config.video_dir || 'videos';
    }
  } catch (e) {}
}

refreshState();
setInterval(refreshState, 1000);
</script>
</body>
</html>
"""


def _make_handler(ctx: SimControlContext) -> type[BaseHTTPRequestHandler]:
    class SimHttpHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: object) -> None:
            return

        def _send_json(self, status: int, data: Any) -> None:
            raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def _send_html(self, html: str) -> None:
            raw = html.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(min(length, 1024 * 1024))
            return json.loads(raw.decode("utf-8")) if raw else {}

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path in ("/", "/index.html"):
                self._send_html(WEB_HTML)
            elif path == "/api/state":
                self._send_json(HTTPStatus.OK, ctx.snapshot())
            elif path == "/api/diagnostics":
                self._send_json(HTTPStatus.OK, ctx.diagnostics())
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})

        def do_POST(self) -> None:
            path = self.path.split("?", 1)[0]
            try:
                body = self._read_json()
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": f"invalid JSON: {exc}"})
                return

            if path == "/api/mode":
                sim = ctx.simulator
                if "mode" in body and body["mode"] in ("auto", "manual", "paused"):
                    sim.mode = body["mode"]
                    sim.is_paused = sim.mode == "paused"
                if "random_events" in body and body["random_events"] in ("off", "low", "normal", "high"):
                    sim.random_events = body["random_events"]
                if "speed" in body:
                    sim.speed = max(0.25, min(10.0, float(body["speed"])))
                if "auto_loop" in body:
                    sim.auto_loop = bool(body["auto_loop"])
                if "respect_video_rules" in body:
                    sim.respect_video_rules = bool(body["respect_video_rules"])
                ctx.run_coro(sim.broadcast_1hz())
                self._send_json(HTTPStatus.OK, {"ok": True, "simulator": sim.to_dict()})
            elif path == "/api/trigger":
                action = str(body.get("action", "")).strip()
                params = body.get("params") if isinstance(body.get("params"), dict) else {}
                res = ctx.run_coro(ctx.simulator.trigger_manual(action, params))
                status = HTTPStatus.OK if res.get("ok") else HTTPStatus.BAD_REQUEST
                self._send_json(status, res)
            elif path == "/api/auth":
                auth = ctx.broker.auth
                if "kick_client_id" in body and body["kick_client_id"]:
                    kicked = ctx.run_coro(ctx.broker.kick_client(str(body["kick_client_id"])))
                    self._send_json(HTTPStatus.OK, {"ok": kicked, "auth": auth.to_dict()})
                    return
                if "auth_mode" in body and body["auth_mode"] in ("open", "required"):
                    auth.auth_mode = body["auth_mode"]
                if "strict_client_id" in body:
                    auth.strict_client_id = bool(body["strict_client_id"])
                if "username" in body:
                    uname = str(body["username"]).strip()
                    pw = str(body.get("password", ""))
                    if uname:
                        auth.users = {uname: pw}
                    elif auth.auth_mode == "open":
                        auth.users = {}
                if "denied_robot_ids" in body and isinstance(body["denied_robot_ids"], list):
                    auth.denied_robot_ids = {int(x) for x in body["denied_robot_ids"]}
                if "fault_connack" in body:
                    fc = body["fault_connack"]
                    auth.fault_connack = int(fc) if fc is not None else None
                if "fault_once" in body:
                    auth.fault_once = bool(body["fault_once"])
                self._send_json(HTTPStatus.OK, {"ok": True, "auth": auth.to_dict()})
            elif path == "/api/video":
                try:
                    vid_state = ctx.video.update_config(body)
                except Exception as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                    return
                self._send_json(HTTPStatus.OK, {"ok": True, "video": vid_state})
            elif path == "/api/probe":
                host = str(body.get("host", "127.0.0.1")).strip() or "127.0.0.1"
                port = int(body.get("port", ctx.broker.actual_port))
                cid = str(body.get("client_id", "3")).strip() or "3"
                user = str(body.get("username", ""))
                pw = str(body.get("password", ""))
                res = run_mqtt_probe(host, port, client_id=cid, username=user, password=pw)
                self._send_json(HTTPStatus.OK, res)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})

    return SimHttpHandler


class SimWebServer:
    def __init__(self, host: str, port: int, ctx: SimControlContext) -> None:
        self.host = host
        self.port = port
        self.ctx = ctx
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def actual_port(self) -> int:
        if self._httpd is not None:
            return int(self._httpd.server_address[1])
        return self.port

    def start(self) -> None:
        handler = _make_handler(self.ctx)
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        self._httpd.daemon_threads = True
        self.ctx.control_port = self.actual_port
        self._thread = threading.Thread(target=self._httpd.serve_forever, name="rm2027-sim-web", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
