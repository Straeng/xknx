"""

Connects to KNX platform.

For more details about this platform, please refer to the documentation at
https://home-assistant.io/components/knx/

"""
import asyncio
import logging

import voluptuous as vol

from homeassistant.const import CONF_HOST, CONF_PORT, EVENT_HOMEASSISTANT_STOP
from homeassistant.helpers import discovery
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.event import (
    async_track_state_change, async_track_utc_time_change)
from homeassistant.helpers.script import Script

DOMAIN = "xknx"
DATA_XKNX = "data_knx"
CONF_XKNX_CONFIG = "config_file"

CONF_XKNX_ROUTING = "routing"
CONF_XKNX_TUNNELING = "tunneling"
CONF_XKNX_LOCAL_IP = "local_ip"
CONF_XKNX_FIRE_EVENT = "fire_event"
CONF_XKNX_FIRE_EVENT_FILTER = "fire_event_filter"
CONF_XKNX_STATE_UPDATER = "state_updater"
CONF_XKNX_EXPOSE = "expose"
CONF_XKNX_EXPOSE_TYPE = "type"
CONF_XKNX_EXPOSE_ENTITY_ID = "entity_id"
CONF_XKNX_EXPOSE_ADDRESS = "address"

SERVICE_XKNX_SEND = "send"
SERVICE_XKNX_ATTR_ADDRESS = "address"
SERVICE_XKNX_ATTR_PAYLOAD = "payload"

ATTR_DISCOVER_DEVICES = 'devices'

_LOGGER = logging.getLogger(__name__)

# REQUIREMENTS = ['xknx==0.7.18']

TUNNELING_SCHEMA = vol.Schema({
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_PORT): cv.port,
    vol.Required(CONF_XKNX_LOCAL_IP): cv.string,
})

ROUTING_SCHEMA = vol.Schema({
    vol.Required(CONF_XKNX_LOCAL_IP): cv.string,
})

EXPOSE_SCHEMA = vol.Schema({
    vol.Required(CONF_XKNX_EXPOSE_TYPE): cv.string,
    vol.Optional(CONF_XKNX_EXPOSE_ENTITY_ID): cv.string,
    vol.Required(CONF_XKNX_EXPOSE_ADDRESS): cv.string,
})

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Optional(CONF_XKNX_CONFIG): cv.string,
        vol.Exclusive(CONF_XKNX_ROUTING, 'connection_type'): ROUTING_SCHEMA,
        vol.Exclusive(CONF_XKNX_TUNNELING, 'connection_type'):
            TUNNELING_SCHEMA,
        vol.Inclusive(CONF_XKNX_FIRE_EVENT, 'fire_ev'):
            cv.boolean,
        vol.Inclusive(CONF_XKNX_FIRE_EVENT_FILTER, 'fire_ev'):
            vol.All(
                cv.ensure_list,
                [cv.string]),
        vol.Optional(CONF_XKNX_STATE_UPDATER, default=True): cv.boolean,
        vol.Optional(CONF_XKNX_EXPOSE):
            vol.All(
                cv.ensure_list,
                [EXPOSE_SCHEMA]),
    })
}, extra=vol.ALLOW_EXTRA)

SERVICE_XKNX_SEND_SCHEMA = vol.Schema({
    vol.Required(SERVICE_XKNX_ATTR_ADDRESS): cv.string,
    vol.Required(SERVICE_XKNX_ATTR_PAYLOAD): vol.Any(
        cv.positive_int, [cv.positive_int]),
})


@asyncio.coroutine
def async_setup(hass, config):
    """Set up knx component."""
    from xknx.exceptions import XKNXException
    try:
        hass.data[DATA_XKNX] = KNXModule(hass, config)
        yield from hass.data[DATA_XKNX].start()

    except XKNXException as ex:
        _LOGGER.exception("Can't connect to KNX interface: %s", ex)
        return False

    for component, discovery_type in (
            ('switch', 'Switch'),
            ('climate', 'Climate'),
            ('cover', 'Cover'),
            ('light', 'Light'),
            ('sensor', 'Sensor'),
            ('binary_sensor', 'BinarySensor'),
            ('notify', 'Notification')):
        found_devices = _get_devices(hass, discovery_type)
        hass.async_add_job(
            discovery.async_load_platform(hass, component, DOMAIN, {
                ATTR_DISCOVER_DEVICES: found_devices
            }, config))

    hass.services.async_register(
        DOMAIN, SERVICE_XKNX_SEND,
        hass.data[DATA_XKNX].service_send_to_knx_bus,
        schema=SERVICE_XKNX_SEND_SCHEMA)

    return True


def _get_devices(hass, discovery_type):
    return list(
        map(lambda device: device.name,
            filter(
                lambda device: type(device).__name__ == discovery_type,
                hass.data[DATA_XKNX].xknx.devices)))


class KNXModule(object):
    """Representation of KNX Object."""

    def __init__(self, hass, config):
        """Initialize KNXModule."""
        self.hass = hass
        self.config = config
        self.initialized = False
        self.init_xknx()
        self.register_callbacks()
        self.exposures = self.create_exposures()

    def init_xknx(self):
        """Initialize KNX object."""
        from xknx import XKNX
        self.xknx = XKNX(
            config=self.config_file(),
            loop=self.hass.loop)

    @asyncio.coroutine
    def start(self):
        """Start KNX object. Connect to tunneling or Routing device."""
        connection_config = self.connection_config()
        yield from self.xknx.start(
            state_updater=self.config[DOMAIN][CONF_XKNX_STATE_UPDATER],
            connection_config=connection_config)
        self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self.stop)
        self.initialized = True

    @asyncio.coroutine
    def stop(self, event):
        """Stop KNX object. Disconnect from tunneling or Routing device."""
        yield from self.xknx.stop()

    def config_file(self):
        """Resolve and return the full path of xknx.yaml if configured."""
        config_file = self.config[DOMAIN].get(CONF_XKNX_CONFIG)
        if not config_file:
            return None
        if not config_file.startswith("/"):
            return self.hass.config.path(config_file)
        return config_file

    def connection_config(self):
        """Return the connection_config."""
        if CONF_XKNX_TUNNELING in self.config[DOMAIN]:
            return self.connection_config_tunneling()
        elif CONF_XKNX_ROUTING in self.config[DOMAIN]:
            return self.connection_config_routing()
        return self.connection_config_auto()

    def connection_config_routing(self):
        """Return the connection_config if routing is configured."""
        from xknx.io import ConnectionConfig, ConnectionType
        local_ip = \
            self.config[DOMAIN][CONF_XKNX_ROUTING].get(CONF_XKNX_LOCAL_IP)
        return ConnectionConfig(
            connection_type=ConnectionType.ROUTING,
            local_ip=local_ip)

    def connection_config_tunneling(self):
        """Return the connection_config if tunneling is configured."""
        from xknx.io import ConnectionConfig, ConnectionType, \
            DEFAULT_MCAST_PORT
        gateway_ip = \
            self.config[DOMAIN][CONF_XKNX_TUNNELING].get(CONF_HOST)
        gateway_port = \
            self.config[DOMAIN][CONF_XKNX_TUNNELING].get(CONF_PORT)
        local_ip = \
            self.config[DOMAIN][CONF_XKNX_TUNNELING].get(CONF_XKNX_LOCAL_IP)
        if gateway_port is None:
            gateway_port = DEFAULT_MCAST_PORT
        return ConnectionConfig(
            connection_type=ConnectionType.TUNNELING,
            gateway_ip=gateway_ip,
            gateway_port=gateway_port,
            local_ip=local_ip)

    def connection_config_auto(self):
        """Return the connection_config if auto is configured."""
        # pylint: disable=no-self-use
        from xknx.io import ConnectionConfig
        return ConnectionConfig()

    def register_callbacks(self):
        """Register callbacks within XKNX object."""
        if CONF_XKNX_FIRE_EVENT in self.config[DOMAIN] and \
                self.config[DOMAIN][CONF_XKNX_FIRE_EVENT]:
            from xknx.knx import AddressFilter
            address_filters = list(map(
                AddressFilter,
                self.config[DOMAIN][CONF_XKNX_FIRE_EVENT_FILTER]))
            self.xknx.telegram_queue.register_telegram_received_cb(
                self.telegram_received_cb, address_filters)

    def create_exposures(self):
        """Create exposures."""
        exposures = []
        if CONF_XKNX_EXPOSE in self.config[DOMAIN]:
            for to_expose in self.config[DOMAIN][CONF_XKNX_EXPOSE]:
                expose_type = to_expose.get(CONF_XKNX_EXPOSE_TYPE)
                expose_entity_id = to_expose.get(CONF_XKNX_EXPOSE_ENTITY_ID)
                expose_address = to_expose.get(CONF_XKNX_EXPOSE_ADDRESS)
                exposure = KNXExpose(self.hass, expose_type,
                                     expose_entity_id, expose_address)
                exposures.append(exposure)
        return exposures

    @asyncio.coroutine
    def telegram_received_cb(self, telegram):
        """Callback invoked after a KNX telegram was received."""
        self.hass.bus.fire('knx_event', {
            'address': telegram.group_address.str(),
            'data': telegram.payload.value
        })
        # False signals XKNX to proceed with processing telegrams.
        return False

    @asyncio.coroutine
    def service_send_to_knx_bus(self, call):
        """Service for sending an arbitrary KNX message to the KNX bus."""
        from xknx.knx import Telegram, GroupAddress, DPTBinary, DPTArray
        attr_payload = call.data.get(SERVICE_XKNX_ATTR_PAYLOAD)
        attr_address = call.data.get(SERVICE_XKNX_ATTR_ADDRESS)

        def calculate_payload(attr_payload):
            """Calculate payload depending on type of attribute."""
            if isinstance(attr_payload, int):
                return DPTBinary(attr_payload)
            return DPTArray(attr_payload)
        payload = calculate_payload(attr_payload)
        address = GroupAddress(attr_address)

        telegram = Telegram()
        telegram.payload = payload
        telegram.group_address = address
        yield from self.xknx.telegrams.put(telegram)


class KNXAutomation():
    """Wrapper around xknx.devices.ActionCallback object.."""

    def __init__(self, hass, device, hook, action, counter=1):
        """Initialize Automation class."""
        self.hass = hass
        self.device = device
        script_name = "{} turn ON script".format(device.get_name())
        self.script = Script(hass, action, script_name)

        import xknx
        self.action = xknx.devices.ActionCallback(
            hass.data[DATA_XKNX].xknx,
            self.script.async_run,
            hook=hook,
            counter=counter)
        device.actions.append(self.action)


class KNXExpose(object):
    """Object to Expose HASS entity to KNX bus."""
    def __init__(self, hass, expose_type, expose_entity_id, expose_address):
        """Initialize of Expose class."""
        self.hass = hass
        self.type = expose_type
        self.entity_id = expose_entity_id
        self.address = expose_address
        self.register()

    def register(self):
        """Register event listener."""
        listening_to_entity = not (self.type in ['time', 'datetime'])

        if listening_to_entity:
            if self.entity_id is None:
                _LOGGER.warning('Can\'t expose %s w/o entity', self.type)
                return
            async_track_state_change(
                self.hass, self.entity_id, self._async_entity_changed)
        else:
            async_track_utc_time_change(
                self.hass, self._async_time_changed, second=0)

    @asyncio.coroutine
    def _async_entity_changed(self, entity_id, old_state, new_state):
        """Callback after entity changed."""
        from xknx.knx import DPTArray, DPTTemperature, DPTHumidity, DPTLux, \
            DPTWsp, DPTUElCurrentmA
        if new_state is None:
            return
        if self.type == 'temperature':
            yield from self._async_send_to_knx_bus(
                DPTArray(DPTTemperature().to_knx(float(new_state.state))))
        elif self.type == 'humidity':
            yield from self._async_send_to_knx_bus(
                DPTArray(DPTHumidity().to_knx(float(new_state.state))))
        elif self.type == 'illuminance':
            yield from self._async_send_to_knx_bus(
                DPTArray(DPTLux().to_knx(float(new_state.state))))
        elif self.type == 'speed_ms':
            yield from self._async_send_to_knx_bus(
                DPTArray(DPTWsp().to_knx(float(new_state.state))))
        elif self.type == 'current':
            yield from self._async_send_to_knx_bus(
                DPTArray(DPTUElCurrentmA().to_knx(float(new_state.state))))
        else:
            _LOGGER.warning('Can\'t expose, unknown type %s', self.type)

    @asyncio.coroutine
    def _async_time_changed(self, *_):
        """Callback after time has changed."""
        from xknx.knx import DPTArray, DPTTime
        if self.type == 'time':
            yield from self._async_send_to_knx_bus(
                DPTArray(DPTTime.current_time_as_knx()))
        elif self.type == 'datetime':
            print("XXX EXPOSE DATETIME")
        else:
            _LOGGER.warning('Can\'t expose, unknown type %s', self.type)

    @asyncio.coroutine
    def _async_send_to_knx_bus(self, payload):
        """Send given payload to KNX bus."""
        from xknx.knx import Telegram, GroupAddress
        telegram = Telegram()
        telegram.group_address = GroupAddress(self.address)
        telegram.payload = payload
        yield from self.hass.data[DATA_XKNX].xknx.telegrams.put(telegram)
