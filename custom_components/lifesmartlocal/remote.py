"""Platform for LifeSmart Remote integration."""
import logging
import random
import asyncio
from typing import Any, Iterable
from homeassistant.components.remote import RemoteEntity, RemoteEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.entity import DeviceInfo
from .const import DOMAIN, MANUFACTURER
from . import generate_entity_id

_LOGGER = logging.getLogger(__name__)

SUPPORTED_REMOTE_TYPES = ["SL_P_IR", "MSL_IRCTL", "OD_WE_IRCTL"]

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up LifeSmart Remote devices."""
    _LOGGER.debug("Setting up LifeSmart remotes")
    entry_data = hass.data[DOMAIN]["entries"][config_entry.entry_id]
    api = entry_data["api"]
    devices = entry_data.get("devices") or []
    
    remotes = []
    # 根據 Advanced 規範 2.4，抓取紅外線遙控器清單
    remote_list = await api.get_remote_list()
    
    if isinstance(devices, list):
        for device in devices:
            if device.get("devtype") in SUPPORTED_REMOTE_TYPES:
                device_keys = []
                if remote_list:
                    for rm in remote_list:
                        if rm.get("remote", {}).get("agt") == device.get("agt"):
                            device_keys = rm.get("keys", [])
                            break
                remotes.append(LifeSmartRemote(api, device, device_keys))

    async_add_entities(remotes)

class LifeSmartRemote(RemoteEntity):
    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_name = None 
    _attr_supported_features = RemoteEntityFeature.ACTIVITY

    def __init__(self, api, device, remote_keys):
        """Initialize the remote."""
        self._api = api
        self._device = device
        self._available = True
        self._remote_keys = remote_keys
        self._unsub_stat = None
        self._unsub_gw = None
        
        device_type = device.get('devtype')
        hub_id = device.get('agt', '')
        device_id = device['me']
        
        self._attr_unique_id = f"lifesmartlocal_remote_{device_id}"
        self.entity_id = f"remote.{generate_entity_id(device_type, hub_id, device_id, 'remote')}"
        
        # 建立指令對應字典
        self._key_map = {key.get("name"): key.get("id") for key in remote_keys if "name" in key and "id" in key}

        # 初始狀態判定：如果設備快取資料顯示離線，預設標記為不可用
        if device.get("stat") == 0:
            self._available = False

    async def async_added_to_hass(self):
        """When entity is added to hass."""
        # 補齊防禦機制：監聽紅外線發射器本身的存活狀態，以及網關整體的存活狀態
        self._unsub_stat = self._api.register_state_listener(self._device["me"], "stat", self._handle_device_stat)
        self._unsub_gw = self._api.register_gw_listener(self._handle_gw_status)
        self.async_write_ha_state()

    async def async_will_remove_from_hass(self):
        """When entity is removed from hass."""
        if self._unsub_stat:
            self._unsub_stat()
        if self._unsub_gw:
            self._unsub_gw()

    def _handle_device_stat(self, stat_val):
        """處理設備上線/離線推播"""
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

    async def _async_write_state(self):
        self.async_write_ha_state()

    @property
    def available(self):
        return self._available

    @property
    def is_on(self) -> bool:
        """Return true if remote is on. Always true for IR blasters."""
        return True

    
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

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the remote on."""
        pass

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the remote off."""
        pass

    
    async def async_send_command(self, command: Iterable[str], **kwargs: Any) -> None:
        """Send a command to a device."""
        #import asyncio # 確保頂部有引入

        for idx, cmd in enumerate(command):
            # 物理防禦：多重紅外線指令連發時，強制加入 1 秒間隔，避免網關當機與漏發
            if idx > 0:
                await asyncio.sleep(1)
                
            key_id = self._key_map.get(cmd)
            if not key_id:
                _LOGGER.warning("Command %s not found in remote %s", cmd, self._attr_unique_id)
                continue
                
            try:
                await self._api.send_remote_key(key_id, cmd)
            except Exception as ex:
                _LOGGER.error("Failed to send command %s: %s", cmd, ex)