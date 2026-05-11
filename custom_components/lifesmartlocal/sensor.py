"""Platform for LifeSmart sensor integration."""
import logging
import random
from typing import Any, Dict

from homeassistant.components.sensor import (
    SensorEntity,
    SensorDeviceClass,
    SensorStateClass,
)
from homeassistant.const import (
    UnitOfTemperature,
    PERCENTAGE,
    LIGHT_LUX,
    UnitOfPower,
    UnitOfEnergy,
    CONCENTRATION_PARTS_PER_MILLION,
    CONCENTRATION_MILLIGRAMS_PER_CUBIC_METER,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.entity import DeviceInfo
from .const import DOMAIN, CMD_GET, MANUFACTURER
from . import generate_entity_id

_LOGGER = logging.getLogger(__name__)

SENSOR_TYPES = {
    "T": {"device_class": SensorDeviceClass.TEMPERATURE, "state_class": SensorStateClass.MEASUREMENT, "unit": UnitOfTemperature.CELSIUS, "name": "Temperature"},
    "H": {"device_class": SensorDeviceClass.HUMIDITY, "state_class": SensorStateClass.MEASUREMENT, "unit": PERCENTAGE, "name": "Humidity"},
    "Z": {"device_class": SensorDeviceClass.ILLUMINANCE, "state_class": SensorStateClass.MEASUREMENT, "unit": LIGHT_LUX, "name": "Illuminance"},
    "V": {"device_class": SensorDeviceClass.BATTERY, "state_class": SensorStateClass.MEASUREMENT, "unit": PERCENTAGE, "name": "Battery"},
    "P8": {"device_class": SensorDeviceClass.BATTERY, "state_class": SensorStateClass.MEASUREMENT, "unit": PERCENTAGE, "name": "Battery"},
    "CO2": {"device_class": SensorDeviceClass.CO2, "state_class": SensorStateClass.MEASUREMENT, "unit": CONCENTRATION_PARTS_PER_MILLION, "name": "CO2"},
    "TVOC": {"device_class": SensorDeviceClass.VOLATILE_ORGANIC_COMPOUNDS, "state_class": SensorStateClass.MEASUREMENT, "unit": CONCENTRATION_MILLIGRAMS_PER_CUBIC_METER, "name": "TVOC"},
    "CH2O": {"device_class": SensorDeviceClass.VOLATILE_ORGANIC_COMPOUNDS, "state_class": SensorStateClass.MEASUREMENT, "unit": CONCENTRATION_MILLIGRAMS_PER_CUBIC_METER, "name": "Formaldehyde"},
    "EP": {"device_class": SensorDeviceClass.POWER, "state_class": SensorStateClass.MEASUREMENT, "unit": UnitOfPower.WATT, "name": "Power"},
    "EE": {"device_class": SensorDeviceClass.ENERGY, "state_class": SensorStateClass.TOTAL_INCREASING, "unit": UnitOfEnergy.KILO_WATT_HOUR, "name": "Energy"},
}

async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    entry_data = hass.data[DOMAIN]["entries"][config_entry.entry_id]
    api = entry_data["api"]
    devices = entry_data.get("devices") or []
    
    sensors = []
    if isinstance(devices, list):
        for device in devices:
            data = device.get("data", {})
            for idx, config in SENSOR_TYPES.items():
                if idx in data:
                    sensors.append(LifeSmartSensor(api, device, idx, config))
                    
    async_add_entities(sensors)

class LifeSmartSensor(SensorEntity):
    _attr_should_poll = False
    _attr_has_entity_name = True

    def __init__(self, api, device, idx, config):
        self._api = api
        self._device = device
        self._idx = idx
        self._available = True
        self._unsub_report = None
        self._unsub_stat = None
        self._unsub_gw = None
        
        self._attr_name = config.get("name")
        self._attr_device_class = config.get("device_class")
        self._attr_state_class = config.get("state_class")
        self._attr_native_unit_of_measurement = config.get("unit")
        
        device_type = device.get('devtype')
        hub_id = device.get('agt', '')
        device_id = device['me']
        
        self._attr_unique_id = f"lifesmartlocal_sensor_{device_id}_{idx}"
        self.entity_id = f"sensor.{generate_entity_id(device_type, hub_id, device_id, idx)}"
        
        initial_val = device.get("data", {}).get(idx, {}).get("v")
        self._update_native_value(initial_val)

    def _update_native_value(self, val):
        if val is None:
            return
        try:
            if self._idx in ["T", "H"] and isinstance(val, int):
                self._attr_native_value = float(val) / 10.0
            else:
                self._attr_native_value = float(val) if isinstance(val, (int, float)) else val
        except (ValueError, TypeError):
            self._attr_native_value = val

    async def async_added_to_hass(self):
        self._unsub_report = self._api.register_state_listener(self._device["me"], self._idx, self._handle_state_value)
        self._unsub_stat = self._api.register_state_listener(self._device["me"], "stat", self._handle_device_stat)
        self._unsub_gw = self._api.register_gw_listener(self._handle_gw_status)
        self.async_write_ha_state()

    async def async_will_remove_from_hass(self):
        if self._unsub_report:
            self._unsub_report()
        if self._unsub_stat:
            self._unsub_stat()
        if self._unsub_gw:
            self._unsub_gw()

    # 🚀 補上缺少的處理函數
    def _handle_device_stat(self, stat_val):
        self._available = bool(stat_val)
        if self.hass:
            #self.hass.async_create_task(self._async_write_state())
            self.hass.async_create_background_task(self._async_write_state(), "lifesmart_update_task")
    
    def _handle_gw_status(self, gw_available: bool):
        """處理網關整體死機/復活廣播"""
        was_available = getattr(self, "_available", False)
        self._available = gw_available
        
        # 🚀 終極修補：當網關從「斷線」恢復為「上線」時，強制抓取最新狀態
        if gw_available and not was_available:
            if self.hass:
                async def _delayed_update():
                    # 隨機延遲 0.1 到 3.0 秒，完美錯開全家設備的併發請求，保護網關晶片
                    await asyncio.sleep(random.uniform(0.1, 3.0))
                    await self._async_update_state()
                #self.hass.async_create_task(_delayed_update())
                self.hass.async_create_background_task(_delayed_update(), "lifesmart_update_task")
        elif self.hass:
            #self.hass.async_create_task(self._async_write_state())
            self.hass.async_create_background_task(self._async_write_state(), "lifesmart_update_task")

    def _handle_state_value(self, val):
        self._update_native_value(val)
        self._available = True
        if self.hass:
            #self.hass.async_create_task(self._async_write_state())
            self.hass.async_create_background_task(self._async_write_state(), "lifesmart_update_task")

    async def _async_update_state(self, *_):
        try:
            args = {"me": self._device["me"]}
            response = await self._api.send_command("ep", args, CMD_GET)
            if response.get("code") == 0 and "msg" in response:
                msg_data = response["msg"]
                if msg_data.get("stat") == 0:
                    self._available = False
                    self._failures = 0 # 🚀 成功時歸零
                else:
                    self._available = True
                    data = msg_data.get("data", {})
                    if self._idx in data:
                        self._update_native_value(data[self._idx].get("v"))
            self.async_write_ha_state()
        except Exception:
            # 修補：容忍偶發的 UDP 掉包，連續失敗 3 次才宣告設備離線
            self._failures += 1
            if self._failures >= 3:
                self._available = False
            self.async_write_ha_state()

    async def _async_write_state(self):
        self.async_write_ha_state()

    @property
    def available(self):
        return self._available

    
    @property
    def device_info(self) -> DeviceInfo:
        """Return device info to Home Assistant."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._device['me'])},
            name=self._device.get('name', 'LifeSmart Device'),
            manufacturer=MANUFACTURER,
            model=self._device.get('devtype'),
            # 將版本號轉為字串以符合 HA 規範
            sw_version=str(self._device.get('epver', 'Unknown')),
            # 新增這行：將 API 的 'me' 欄位作為序號顯示
            serial_number=self._device.get('me'),
        )