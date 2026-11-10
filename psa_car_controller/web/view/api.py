import logging

from flask import jsonify, request, Response as FlaskResponse
from pydantic import BaseModel

from psa_car_controller.common.utils import RateLimitException
from psa_car_controller.psacc.application.car_controller import PSACarController
from psa_car_controller.psacc.repository.db import Database
from psa_car_controller.web.app import app

from psa_car_controller.psacc.model.car import Cars
from psa_car_controller.psacc.repository.trips import Trips

from psa_car_controller.psacc.application.charging import Charging

import json

from psa_car_controller.web.tools.utils import convert_to_number_if_number_else_return_str

logger = logging.getLogger(__name__)

STYLE_CACHE = None
APP = PSACarController()


def json_response(json: str, status=200):
    return app.response_class(
        response=json,
        status=status,
        mimetype='application/json'
    )


@app.route('/get_vehicles')
def get_vehicules():
    response = app.response_class(
        response=json.dumps(APP.myp.get_vehicles(), default=lambda car: car.to_dict()),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/get_vehicleinfo/<string:vin>')
def get_vehicle_info(vin):
    from_cache = int(request.args.get('from_cache', 0)) == 1
    response = app.response_class(
        response=json.dumps(APP.myp.get_vehicle_info(vin, from_cache).to_dict(), default=str),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route("/style.json")
def get_style():
    global STYLE_CACHE
    if not STYLE_CACHE:
        with open(app.root_path + "/assets/style.json", "r", encoding="utf-8") as f:
            res = json.loads(f.read())
            STYLE_CACHE = res
    url_root = request.url_root
    STYLE_CACHE["sprite"] = url_root + "assets/sprites/osm-liberty"
    return jsonify(STYLE_CACHE)


@app.route('/charge_now/<string:vin>/<int:charge>')
def charge_now(vin, charge):
    return jsonify(APP.myp.remote_client.charge_now(vin, charge != 0))


@app.route('/charge_hour')
def change_charge_hour():
    return jsonify(APP.myp.remote_client.change_charge_hour(request.args['vin'],
                                                            request.args['hour'],
                                                            request.args['minute']))


@app.route('/wakeup/<string:vin>')
def wakeup(vin):
    try:
        return jsonify(APP.myp.remote_client.wakeup(vin))
    except RateLimitException:
        return jsonify({"error": "Wakeup rate limit exceeded"})


@app.route('/preconditioning/<string:vin>/<int:activate>')
def preconditioning(vin, activate):
    return jsonify(APP.myp.remote_client.preconditioning(vin, activate))


@app.route('/position/<string:vin>')
def get_position(vin):
    res = APP.myp.get_vehicle_info(vin)
    try:
        coordinates = res.last_position.geometry.coordinates
    except AttributeError:
        return jsonify({'error': 'last_position not available from api'})
    longitude, latitude = coordinates[:2]
    if len(coordinates) == 3:  # altitude is not always available
        altitude = coordinates[2]
    else:
        altitude = None
    return jsonify(
        {"longitude": longitude, "latitude": latitude, "altitude": altitude,
         "url": f"https://maps.google.com/maps?q={latitude},{longitude}"})


# Set a battery threshold and schedule an hour to stop the charge
@app.route('/charge_control')
def get_charge_control():
    logger.info(request)
    vin = request.args['vin']
    if APP.chc:
        charge_control = APP.chc.get(vin)
        if charge_control is None:
            return jsonify({"error": "VIN not in list"})
        if 'hour' in request.args and 'minute' in request.args:
            charge_control.set_stop_hour([int(request.args["hour"]), int(request.args["minute"])])
        if 'percentage' in request.args:
            charge_control.percentage_threshold = int(request.args['percentage'])
        APP.chc.save_config()
        return jsonify(charge_control.get_dict())
    error = "Charge control not setup check your PSACC configuration and logs"
    logger.error(error)
    return jsonify({"error": error})


@app.route('/positions')
def get_recorded_position():
    return FlaskResponse(Database.get_recorded_position(), mimetype='application/json')


@app.route('/abrp')
def abrp():
    vin = request.args.get('vin', None)
    enable = request.args.get('enable', None)
    token = request.args.get('token', None)
    if vin is not None and enable is not None:
        if enable == '1':
            APP.myp.abrp.abrp_enable_vin.add(vin)
        else:
            APP.myp.abrp.abrp_enable_vin.discard(vin)
    if token is not None:
        APP.myp.abrp.token = token
    return jsonify(dict(APP.myp.abrp))


@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/horn/<string:vin>/<int:count>')
def horn(vin, count):
    try:
        return jsonify(APP.myp.remote_client.horn(vin, count))
    except RateLimitException:
        return jsonify({"error": "Horn rate limit exceeded"})


@app.route('/lights/<string:vin>/<int:duration>')
def lights(vin, duration):
    try:
        return jsonify(APP.myp.remote_client.lights(vin, duration))
    except RateLimitException:
        return jsonify({"error": "Lights rate limit exceeded"})


@app.route('/lock_door/<string:vin>/<int:lock>')
def lock_door(vin, lock):
    try:
        return jsonify(APP.myp.remote_client.lock_door(vin, lock))
    except RateLimitException:
        return jsonify({"error": "Locks rate limit exceeded"})


@app.route('/settings/<string:section>')
def settings_section(section: str):
    config_section: BaseModel = getattr(APP.config, section.capitalize())
    for key, value in request.args.items():
        typed_value = convert_to_number_if_number_else_return_str(value)
        setattr(config_section, key, typed_value)
        APP.config.write_config()
    return json_response(config_section.json())


@app.route('/vehicles/trips')
def get_trips():
    try:
        if not APP.myp.vehicles_list:
            return jsonify({})
        car = APP.myp.vehicles_list[0]
        trips_by_vin = Trips.get_trips(Cars([car]))
        trips = trips_by_vin[car.vin]
        trips_as_dict = trips.get_trips_as_dict()
        return jsonify(trips_as_dict)
    except (IndexError, TypeError, KeyError):
        logger.debug("Failed to get trips, there is probably not enough data yet:", exc_info=True)
        return jsonify([])


def _bin_avg(rows, key, width):
    """Average `key` per bin of `width`, bin label = lower edge. Skips rows without value."""
    bins = {}
    for r in rows:
        v, x = r.get(key), r.get("_x")
        if v is None or x is None:
            continue
        b = int(x // width) * width
        acc = bins.setdefault(b, [0.0, 0])
        acc[0] += v
        acc[1] += 1
    return [{"from": b, "to": b + width, "avg": round(acc[0] / acc[1], 2), "n": acc[1]}
            for b, acc in sorted(bins.items())]


def _charging_summary():
    """Totals + per-month kWh/price/sessions from Charging.get_chargings()."""
    months = {}
    total_kwh = total_price = 0.0
    sessions = 0
    for c in Charging.get_chargings():
        sessions += 1
        start = c.get("start_at")
        if hasattr(start, "strftime"):
            ym = start.strftime("%Y-%m")
        else:
            ym = str(start)[:7]
        m = months.setdefault(ym, {"month": ym, "kwh": 0.0, "price": 0.0, "sessions": 0})
        m["sessions"] += 1
        if c.get("kw") is not None:
            m["kwh"] += c["kw"]
            total_kwh += c["kw"]
        if c.get("price") is not None:
            m["price"] += c["price"]
            total_price += c["price"]
    by_month = [dict(m, kwh=round(m["kwh"], 2), price=round(m["price"], 2)) for _, m in sorted(months.items())]
    return {"sessions": sessions, "kwh_total": round(total_kwh, 2), "price_total": round(total_price, 2),
            "by_month": by_month}


@app.route('/vehicles/trips_summary')
def get_trips_summary():
    """Compact summary for HA (stays well under the 16 KB HA attribute limit)."""
    empty = {"count": 0, "trips": [], "by_temp": [], "by_speed": []}
    try:
        if not APP.myp.vehicles_list:
            return jsonify(empty)
        car = APP.myp.vehicles_list[0]
        trips = Trips.get_trips(Cars([car]))[car.vin]
        rows = []
        for t in trips.get_trips_as_dict():
            start = t.get("start_at")
            rows.append({
                "start_at": start.isoformat() if hasattr(start, "isoformat") else start,
                "temp": t.get("consumption_by_temp"),
                "speed": t.get("speed_average"),
                "consumption_km": t.get("consumption_km"),
                "distance": t.get("distance"),
                # ruwe duur uit get_info staat al in minuten
                "duration_min": round(t["duration"], 1) if t.get("duration") is not None else None,
            })
        # drop trips without a measured consumption (0/None)
        # ritten < 5 km: accu meet in stappen van 1% (~0,5 kWh), dat geeft bij korte ritten absurde kWh/100km
        # bovengrens 40 kWh/100 km: meetfout-uitschieters, ook bij ritten >= 5 km
        rows = [r for r in rows if r["consumption_km"] and (r["distance"] or 0) >= 5
                and r["consumption_km"] <= 40]
        # _x: axis value per row for binning (temp in °C, speed in km/h)
        by_temp_rows = [dict(r, _x=r["temp"]) for r in rows]
        by_speed_rows = [dict(r, _x=r["speed"]) for r in rows]
        # ?trips=0 leaves out the per-trip list: HA attributes are capped at 16 KB
        include_trips = request.args.get("trips", "1") != "0"
        return jsonify({
            "count": len(rows),
            "trips": rows if include_trips else [],
            "by_temp": _bin_avg(by_temp_rows, "consumption_km", 5),
            "by_speed": _bin_avg(by_speed_rows, "consumption_km", 5),
            "charging": _charging_summary(),
            # compact per-trip series for time charts, fits HA attribute cap even with trips=0
            # [start_at, distance_km, duration_min, speed_avg, temp_c, consumption_km]
            "series": [[r["start_at"], r["distance"], r["duration_min"], r["speed"], r["temp"],
                        r["consumption_km"]] for r in rows],
        })
    except (IndexError, TypeError, KeyError):
        logger.debug("Failed to build trips summary, probably not enough data yet:", exc_info=True)
        return jsonify(empty)


@app.route('/vehicles/chargings')
def get_chargings():
    try:
        chargings = Charging.get_chargings()
        return jsonify(chargings)
    except (IndexError, TypeError):
        logger.debug("Failed to get chargings, there is probably not enough data yet:", exc_info=True)
        return jsonify([])


@app.route('/settings')
def settings():
    return json_response(APP.config.json())


@app.route('/battery/soh/<string:vin>')
def db(vin: str):
    soh = Database.get_last_soh_by_vin(vin)
    return jsonify({"soh": soh})
