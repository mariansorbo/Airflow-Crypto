"""
Test script: Historical data fetch for polygon-ecosystem-token via CoinGecko

Objetivo:
    Probar el endpoint /coins/{id}/market_chart/range para obtener datos
    históricos de polygon-ecosystem-token en 5 timestamps específicos.

    Para cada timestamp objetivo:
        1. Pedimos un rango de +-1 hora a CoinGecko
        2. Encontramos el punto más cercano en la respuesta
        3. Calculamos la diferencia entre lo pedido y lo devuelto
        4. Mostramos precio, market_cap, volumen y el delta de tiempo

Uso:
    python scripts/test_historical_polygon.py

    Opcional: setear variable de entorno con tu API key de CoinGecko
        set COINGECKO_API_KEY=tu_key   (Windows CMD)
        $env:COINGECKO_API_KEY="tu_key" (PowerShell)
"""

import os
import time
from datetime import datetime, timedelta, timezone

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

COIN_ID    = "polygon-ecosystem-token"
VS_CURRENCY = "usd"
BASE_URL   = "https://api.coingecko.com/api/v3"
WINDOW_HOURS = 1          # cuántas horas de ventana pedir a cada lado del target
DELAY_BETWEEN_CALLS = 2.5 # segundos entre llamadas (respetar rate limit free tier)

# 5 timestamps históricos a testear — modificá estos según quieras
TARGET_TIMESTAMPS = [
    datetime(2026, 3, 20, 10,  0, tzinfo=timezone.utc),
    datetime(2026, 3, 21, 15, 30, tzinfo=timezone.utc),
    datetime(2026, 3, 22,  8,  0, tzinfo=timezone.utc),
    datetime(2026, 3, 23, 20,  0, tzinfo=timezone.utc),
    datetime(2026, 3, 24, 12,  0, tzinfo=timezone.utc),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    api_key = os.getenv("COINGECKO_API_KEY", "")
    return {"x-cg-demo-api-key": api_key} if api_key else {}


def fetch_market_chart_range(coin_id: str, from_ts: datetime, to_ts: datetime) -> dict:
    """
    GET /coins/{id}/market_chart/range
    Devuelve precios, market_caps y volúmenes en el rango [from_ts, to_ts].

    Granularidad según el plan de CoinGecko:
        - Rango <= 1 día  --> datos cada 5 minutos (plan gratuito)
        - Rango 1-90 días --> datos cada hora
        - Rango > 90 días --> datos diarios
    """
    url = f"{BASE_URL}/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": VS_CURRENCY,
        "from": int(from_ts.timestamp()),
        "to":   int(to_ts.timestamp()),
    }
    resp = requests.get(url, params=params, headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def find_closest_point(data_points: list, target_ts: datetime) -> tuple:
    """
    Dado un listado de [unix_ms, value], devuelve el punto más cercano
    al target_ts y la diferencia en segundos.

    Retorna: (punto_datetime, value, delta_segundos)
    """
    target_ms = target_ts.timestamp() * 1000  # CoinGecko devuelve ms

    closest = min(data_points, key=lambda p: abs(p[0] - target_ms))
    point_ts = datetime.fromtimestamp(closest[0] / 1000, tz=timezone.utc)
    delta_sec = (point_ts - target_ts).total_seconds()

    return point_ts, closest[1], delta_sec


def analyze_timestamp(target_ts: datetime) -> dict:
    """
    Para un timestamp objetivo:
        1. Pide rango +-WINDOW_HOURS a CoinGecko
        2. Encuentra precio, market_cap, volumen más cercanos
        3. Calcula el delta de tiempo entre lo pedido y lo devuelto
    """
    from_ts = target_ts - timedelta(hours=WINDOW_HOURS)
    to_ts   = target_ts + timedelta(hours=WINDOW_HOURS)

    print(f"\n  Consultando rango: {from_ts.strftime('%Y-%m-%d %H:%M')} --> {to_ts.strftime('%Y-%m-%d %H:%M')} UTC")

    data = fetch_market_chart_range(COIN_ID, from_ts, to_ts)

    prices      = data.get("prices", [])
    market_caps = data.get("market_caps", [])
    volumes     = data.get("total_volumes", [])

    if not prices:
        return {"error": "No hay datos en el rango solicitado"}

    price_ts,  price,      price_delta  = find_closest_point(prices,      target_ts)
    mcap_ts,   market_cap, mcap_delta   = find_closest_point(market_caps, target_ts)
    vol_ts,    volume,     vol_delta    = find_closest_point(volumes,      target_ts)

    return {
        "target_ts":   target_ts,
        "points_available": len(prices),
        "price": {
            "value":     round(price, 6),
            "point_ts":  price_ts,
            "delta_sec": price_delta,
            "delta_min": round(price_delta / 60, 1),
        },
        "market_cap": {
            "value":     round(market_cap, 2),
            "point_ts":  mcap_ts,
            "delta_sec": mcap_delta,
            "delta_min": round(mcap_delta / 60, 1),
        },
        "volume": {
            "value":     round(volume, 2),
            "point_ts":  vol_ts,
            "delta_sec": vol_delta,
            "delta_min": round(vol_delta / 60, 1),
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print(f"  Test histórico: {COIN_ID}")
    print(f"  Ventana por request: +-{WINDOW_HOURS}h alrededor del target")
    print(f"  Timestamps a testear: {len(TARGET_TIMESTAMPS)}")
    print("=" * 70)

    results = []

    for i, target_ts in enumerate(TARGET_TIMESTAMPS, start=1):
        print(f"\n[{i}/{len(TARGET_TIMESTAMPS)}] Target: {target_ts.strftime('%Y-%m-%d %H:%M UTC')}")

        try:
            result = analyze_timestamp(target_ts)
            results.append(result)

            if "error" in result:
                print(f"  ERROR: {result['error']}")
                continue

            print(f"  Puntos disponibles en el rango: {result['points_available']}")
            print(f"  Precio:      ${result['price']['value']:>12,.6f}  "
                  f"| punto real: {result['price']['point_ts'].strftime('%H:%M:%S')} UTC  "
                  f"(delta: {result['price']['delta_min']:+.1f} min)")
            print(f"  Market Cap:  ${result['market_cap']['value']:>15,.2f}  "
                  f"| punto real: {result['market_cap']['point_ts'].strftime('%H:%M:%S')} UTC  "
                  f"(delta: {result['market_cap']['delta_min']:+.1f} min)")
            print(f"  Volumen 24h: ${result['volume']['value']:>15,.2f}  "
                  f"| punto real: {result['volume']['point_ts'].strftime('%H:%M:%S')} UTC  "
                  f"(delta: {result['volume']['delta_min']:+.1f} min)")

        except requests.HTTPError as exc:
            print(f"  HTTP ERROR: {exc}")
            results.append({"target_ts": target_ts, "error": str(exc)})
        except Exception as exc:
            print(f"  ERROR inesperado: {exc}")
            results.append({"target_ts": target_ts, "error": str(exc)})

        if i < len(TARGET_TIMESTAMPS):
            print(f"  Esperando {DELAY_BETWEEN_CALLS}s para respetar rate limit...")
            time.sleep(DELAY_BETWEEN_CALLS)

    # -----------------------------------------------------------------------
    # Resumen final
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("  RESUMEN — Variación de timestamp (precio)")
    print("=" * 70)
    print(f"  {'Target (UTC)':<22} {'Punto devuelto':<20} {'Delta (min)':>12}  {'Precio USD':>14}")
    print(f"  {'-'*22} {'-'*20} {'-'*12}  {'-'*14}")

    deltas = []
    for r in results:
        if "error" in r:
            target_str = r["target_ts"].strftime("%Y-%m-%d %H:%M")
            print(f"  {target_str:<22} {'ERROR':<20} {'N/A':>12}  {'N/A':>14}")
            continue

        target_str = r["target_ts"].strftime("%Y-%m-%d %H:%M")
        point_str  = r["price"]["point_ts"].strftime("%Y-%m-%d %H:%M")
        delta_min  = r["price"]["delta_min"]
        price      = r["price"]["value"]
        deltas.append(abs(delta_min))

        print(f"  {target_str:<22} {point_str:<20} {delta_min:>+11.1f}m  ${price:>13,.6f}")

    if deltas:
        print(f"\n  Promedio de variación absoluta : {sum(deltas)/len(deltas):.1f} min")
        print(f"  Variación máxima               : {max(deltas):.1f} min")
        print(f"  Variación mínima               : {min(deltas):.1f} min")

    print("=" * 70)
    print("\nConclusión:")
    if deltas and max(deltas) <= 30:
        print("  La granularidad de CoinGecko es suficiente para backfills de 30 min.")
        print("  El punto devuelto siempre va a estar dentro del slot buscado.")
    elif deltas and max(deltas) <= 60:
        print("  La granularidad es horaria — puede haber desfasaje de hasta 60 min.")
        print("  Para backfills de 30 min, dos slots consecutivos pueden tener el mismo precio.")
    else:
        print("  La granularidad es mayor a 60 min — considerar si el backfill vale la pena.")


if __name__ == "__main__":
    main()
