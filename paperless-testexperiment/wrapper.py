# paperless/pyjoules_wrapper.py
import logging, os

try:
    from pyJoules.energy_meter import EnergyMeter
    from pyJoules.device.rapl_device import RaplPackageDomain, RaplDramDomain
    from pyJoules.device import DeviceFactory
    from pyJoules.handler.csv_handler import CSVHandler
    _PYJOULES_OK = True
    _IMPORT_ERR = None
except Exception as e:
    _PYJOULES_OK = False
    _IMPORT_ERR = e


class EnergyWSGIWrapper:
    """
    WSGI wrapper that measures energy for /static/* requests.
    Compatible with pyJoules 0.5.x (no handler arg on EnergyMeter).
    """
    def __init__(self, app, csv_path="/tmp/energy_static_requests.csv"):
        self.app = app
        self.csv_path = csv_path
        if not _PYJOULES_OK:
            logging.warning("EnergyWSGIWrapper: pyJoules unavailable: %r. "
                            "Proceeding without measurement.", _IMPORT_ERR)

    def __call__(self, environ, start_response):
        path = environ.get("PATH_INFO", "") or ""
        if not _PYJOULES_OK or not path.startswith("/static/"):
            return self.app(environ, start_response)

        # Ensure parent directory exists
        parent = os.path.dirname(self.csv_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        # Build devices (package; DRAM optional)
        domains = [RaplPackageDomain(0)]
        try:
            domains.append(RaplDramDomain(0))
        except Exception:
            pass
        try:
            devices = DeviceFactory.create_devices(domains)
        except Exception as e:
            logging.warning("EnergyWSGIWrapper: cannot create RAPL devices: %r", e)
            return self.app(environ, start_response)

        meter = EnergyMeter(devices)

        # Start measuring before handing to Django/WhiteNoise
        meter.start(tag=f"{environ.get('REQUEST_METHOD','GET')} {path}")
        inner_iterable = self.app(environ, start_response)

        def _gen():
            try:
                for chunk in inner_iterable:
                    yield chunk
            finally:
                # Stop when the iterable is exhausted (i.e., after last byte sent)
                try:
                    meter.stop()
                    trace = meter.get_trace()     # <-- collect measurements
                    CSVHandler(self.csv_path).save_data(trace)  # <-- write CSV
                except Exception as e:
                    logging.warning("EnergyWSGIWrapper: stop/save failed: %r", e)

        class _WrappedIterable:
            def __iter__(self): return _gen()
            def close(self):
                if hasattr(inner_iterable, "close"):
                    inner_iterable.close()

        return _WrappedIterable()
