"""Platform for LifeSmart cover integration."""
import logging
import asyncio
import random  #引入 random 用於重連時的 Jitter 抖動延遲
from homeassistant.components.cover import CoverEntity, CoverEntityFeature, CoverDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.entity import DeviceInfo
from .const import DOMAIN, CMD_GET, CMD_SET, MANUFACTURER
from . import generate_entity_id

_LOGGER = logging.getLogger(__name__)

SUPPORTED_COVER_TYPES = ["SL_SW_WIN", "SL_CN_IF", "SL_CN_FE", "SL_DOOYA", "SL_P_V2", "SL_P"]

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up LifeSmart covers."""
    entry_data = hass.data[DOMAIN]["entries"][config_entry.entry_id]
    api = entry_data["api"]
    devices = entry_data.get("devices") or []
    
    covers = []
    for device in devices:
        if device.get("devtype") in SUPPORTED_COVER_TYPES:
            covers.append(LifeSmartCover(api, device))
            
    async_add_entities(covers)

class LifeSmartCover(CoverEntity):
    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_name = None  
    _attr_device_class = CoverDeviceClass.CURTAIN
    _attr_supported_features = CoverEntityFeature.OPEN | CoverEntityFeature.CLOSE | CoverEntityFeature.STOP

    def __init__(self, api, device):
        """Initialize the cover."""
        self._api = api
        self._device = device
        self._available = True
        self._failures = 0  # 🚀 引入 UDP 掉包容錯計數器
        self._unsub_report = None
        self._unsub_stat = None
        self._unsub_gw = None
        self._is_closed = None
        
        device_type = device.get('devtype')
        hub_id = device.get('agt', '')
        device_id = device['me']
        
        self._attr_unique_id = f"lifesmartlocal_cover_{device_id}"
        self.entity_id = f"cover.{generate_entity_id(device_type, hub_id, device_id, 'cover')}"

        # 啟動優化：直接從 __init__ 快取資料取得初始狀態
        initial_data = device.get("data", {})
        if "P1" in initial_data:
            val = initial_data["P1"].get("v", 0)
            try:
                self._is_closed = (float(val) > 0)
            except (ValueError, TypeError):
                pass

    async def async_added_to_hass(self):
        """When entity is added to hass."""
        self._unsub_report = self._api.register_state_listener(self._device["me"], "P1", self._handle_state_value)
        self._unsub_stat = self._api.register_state_listener(self._device["me"], "stat", self._handle_device_stat)
        self._unsub_gw = self._api.register_gw_listener(self._handle_gw_status)
        self.async_write_ha_state()

    async def async_will_remove_from_hass(self):
        """When entity is removed from hass."""
        if self._unsub_report:
            self._unsub_report()
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
        """處理網關整體死機/復活廣播 (包含 Jitter 防風暴與重連主動同步)"""
        was_available = getattr(self, "_available", False)
        self._available = gw_available
        
        # 當網關從斷線恢復為上線時，強制抓取最新狀態，並隨機延遲保護網關
        if gw_available and not was_available:
            if self.hass:
                async def _delayed_update():
                    await asyncio.sleep(random.uniform(0.1, 3.0))
                    await self._async_update_state()
                #self.hass.async_create_task(_delayed_update())
                self.hass.async_create_background_task(_delayed_update(), "lifesmart_update_task")
        elif self.hass:
            #self.hass.async_create_task(self._async_write_state())
            self.hass.async_create_background_task(self._async_write_state(), "lifesmart_update_task")

    def _handle_state_value(self, val):
        """處理 UDP 推播過來的 P1 數值更新"""
        self._available = True
        self._failures = 0
        try:
            if isinstance(val, (int, float, str)):
                self._is_closed = (float(val) > 0)
        except (ValueError, TypeError):
            pass
            
        if self.hass:
            #self.hass.async_create_task(self._async_write_state())
            self.hass.async_create_background_task(self._async_write_state(), "lifesmart_update_task")

    async def _async_update_state(self, *_):
        """主動查詢 (加入 3 次失敗容忍機制)"""
        try:
            args = {"me": self._device["me"]}
            response = await self._api.send_command("ep", args, CMD_GET)
            if response.get("code") == 0 and "msg" in response:
                msg_data = response["msg"]
                if msg_data.get("stat") == 0:
                    self._available = False
                else:
                    self._available = True
                    self._failures = 0  # 🚀 成功時歸零
                    data = msg_data.get("data", {})
                    if "P1" in data:
                        val = data["P1"].get("v", 0)
                        try:
                            self._is_closed = (float(val) > 0)
                        except (ValueError, TypeError):
                            pass
            self.async_write_ha_state()
        except Exception:
            # 容忍偶發的 UDP 掉包，連續失敗 3 次才宣告設備離線
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
    def is_closed(self) -> bool | None:
        return self._is_closed

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device['me'])},
            name=self._device.get('name', 'LifeSmart Curtain'),
            manufacturer=MANUFACTURER,
            model=self._device.get('devtype'),
            sw_version=str(self._device.get('epver', 'Unknown')),
            serial_number=self._device.get('me'),  # 🚀 新增設備序號顯示
        )

    async def async_open_cover(self, **kwargs):
        """開啟窗簾 (包含樂觀預測與錯誤回滾)"""
        devtype = self._device.get('devtype')
        if devtype == "SL_DOOYA":
            args = {"tag": "m", "me": self._device["me"], "idx": "P2", "type": 0xCF, "val": 100}
        elif devtype in ["SL_SW_WIN", "SL_CN_IF", "SL_CN_FE"]:
            args = {"tag": "m", "me": self._device["me"], "idx": "OP", "type": 0x81, "val": 1}
        else: 
            args = {"tag": "m", "me": self._device["me"], "idx": "P2", "type": 0x81, "val": 1}
        
        # 紀錄舊狀態以備回滾
        old_state = self._is_closed
        # 樂觀狀態預測：立刻更新 UI 為開啟
        self._is_closed = False
        self.async_write_ha_state()
        
        try:
            # 執行指令並加入超時設定
            response = await self._api.send_command("ep", args, CMD_SET, 1.0)
            if response.get("code") != 0:
                # 網關回報錯誤，回滾狀態
                self._is_closed = old_state
        except Exception as e:
            _LOGGER.debug("Cover open command failed, rolling back: %s", e)
            # UDP 掉包或超時，回滾狀態
            self._is_closed = old_state
        finally:
            self.async_write_ha_state()

    async def async_close_cover(self, **kwargs):
        """關閉窗簾 (包含樂觀預測與錯誤回滾)"""
        devtype = self._device.get('devtype')
        if devtype == "SL_DOOYA":
            args = {"tag": "m", "me": self._device["me"], "idx": "P2", "type": 0xCF, "val": 0}
        elif devtype in ["SL_SW_WIN", "SL_CN_IF", "SL_CN_FE"]:
            args = {"tag": "m", "me": self._device["me"], "idx": "CL", "type": 0x81, "val": 1}
        else:
            args = {"tag": "m", "me": self._device["me"], "idx": "P3", "type": 0x81, "val": 1}
            
        # 紀錄舊狀態以備回滾
        old_state = self._is_closed
        # 樂觀狀態預測：立刻更新 UI 為關閉
        self._is_closed = True
        self.async_write_ha_state()
        
        try:
            # 執行指令並加入超時設定
            response = await self._api.send_command("ep", args, CMD_SET, 1.0)
            if response.get("code") != 0:
                # 網關回報錯誤，回滾狀態
                self._is_closed = old_state
        except Exception as e:
            _LOGGER.debug("Cover close command failed, rolling back: %s", e)
            # UDP 掉包或超時，回滾狀態
            self._is_closed = old_state
        finally:
            self.async_write_ha_state()

    async def async_stop_cover(self, **kwargs):
        """停止窗簾 (包含基礎錯誤捕獲)"""
        devtype = self._device.get('devtype')
        if devtype == "SL_DOOYA":
            args = {"tag": "m", "me": self._device["me"], "idx": "P2", "type": 0xCE, "val": 0x80}
        elif devtype in ["SL_SW_WIN", "SL_CN_IF", "SL_CN_FE"]:
            args = {"tag": "m", "me": self._device["me"], "idx": "ST", "type": 0x81, "val": 1}
        else:
            args = {"tag": "m", "me": self._device["me"], "idx": "P4", "type": 0x81, "val": 1}
            
        try:
            await self._api.send_command("ep", args, CMD_SET, 1.0)
        except Exception as e:
            _LOGGER.debug("Cover stop command failed: %s", e)