"""Platform for LifeSmart switch integration."""
import asyncio
import logging
import random
from homeassistant.components.switch import SwitchEntity, SwitchDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.core import callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.entity import DeviceInfo
from .const import DOMAIN, CMD_GET, CMD_SET, MANUFACTURER
from . import generate_entity_id

_LOGGER = logging.getLogger(__name__)

VAL_TYPE_ON = 0x81
VAL_TYPE_OFF = 0x80

SUPPORTED_SWITCH_TYPES = [
    "SL_SW_NS1", "SL_SW_NS2", "SL_SW_NS3", "SL_NATURE", 
    "SL_SW_ND1", "SL_SW_ND2", "SL_SW_ND3",
    "SL_SW_IF1", "SL_SW_IF2", "SL_SW_IF3",
    "SL_SW_RC", "SL_SF_RC", "SL_SW_RC1", "SL_SW_RC2", "SL_SW_RC3",
    "SL_SW_BS1", "SL_SW_BS2", "SL_SW_BS3",
    "SL_SW_MJ1", "SL_SW_MJ2",
    "SL_SW_CP1", "SL_SW_CP2", "SL_SW_CP3",
    "SL_SW_FE1", "SL_SW_FE2",
    "SL_OL_3C", "SL_OL_DE", "SL_OL_UK", "SL_OL_UL", "OD_WE_OT1", "SL_OL_W",
    "SL_OE_W", "SL_OE_3C", "SL_OE_DE"
]

async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    entry_data = hass.data[DOMAIN]["entries"][config_entry.entry_id]
    api = entry_data["api"]
    devices = entry_data.get("devices") or []
    
    switches = []
    if isinstance(devices, list):
        for device in devices:
            if device.get("devtype") in SUPPORTED_SWITCH_TYPES:
                data = device.get("data", {})
                for channel in ["L1", "L2", "L3", "O"]:
                    if channel in data:
                        channel_data = data[channel]
                        channel_name = channel_data.get('name', channel).replace('{$EPN}', '').strip()
                        switches.append(
                            LifeSmartSwitch(api=api, device=device, idx=channel, channel_name=channel_name)
                        )
    async_add_entities(switches)

class LifeSmartSwitch(SwitchEntity):
    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_device_class = SwitchDeviceClass.SWITCH
    
    def __init__(self, api, device, idx, channel_name):
        self._api = api
        self._device = device
        self._idx = idx
        self._attr_name = channel_name if channel_name else f"Switch {idx}"
        self._available = True
        self._unsub_report = None
        self._unsub_stat = None
        self._unsub_gw = None
        self._expected_state = None
        self._confirm_event = None
        self._failures = 0
        self._send_lock = asyncio.Lock()
        self._send_task: asyncio.Task | None = None
        self._pending_value: int | None = None
        self._pending_waiters: list[asyncio.Future] = []
        
        device_type = device.get('devtype')
        hub_id = device.get('agt', '')
        device_id = device['me']
        
        self._attr_unique_id = f"lifesmartlocal_switch_{device_id}_{idx}"
        self.entity_id = f"switch.{generate_entity_id(device_type, hub_id, device_id, idx)}"
        
        initial_state = device.get("data", {}).get(idx, {}).get("v", 0)
        self._state = bool(initial_state)

    async def async_added_to_hass(self):
        self._unsub_report = self._api.register_state_listener(self._device["me"], self._idx, self._handle_state_value)
        # 綁定設備斷線與網關斷線的監聽器
        self._unsub_stat = self._api.register_state_listener(self._device["me"], "stat", self._handle_device_stat)
        self._unsub_gw = self._api.register_gw_listener(self._handle_gw_status)
        # 取消啟動時的 GET 請求，直接更新介面
        self.async_write_ha_state()

    async def async_will_remove_from_hass(self):
        if self._unsub_report:
            self._unsub_report()
        if self._unsub_stat:
            self._unsub_stat()
        if self._unsub_gw:
            self._unsub_gw()
        if self._send_task:
            self._send_task.cancel()

    # 補上缺少的處理函數
    def _handle_device_stat(self, stat_val):
        self._available = bool(stat_val)
        if self.hass:
            #self.hass.async_create_task(self._async_write_state())
            self.hass.async_create_background_task(self._async_write_state(), "lifesmart_update_task")

    
    def _handle_gw_status(self, gw_available: bool):
        """處理網關整體死機/復活廣播"""
        was_available = getattr(self, "_available", False)
        self._available = gw_available
        
        # 修補：當網關從「斷線」恢復為「上線」時，強制抓取最新狀態
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
    
    @callback
    def _handle_state_value(self, val) -> None:
        """處理 UDP 推播過來的數值更新"""
        try:
            # 容錯修復：強制將數值轉為 float 再轉 bool，完美吃下整數、浮點數與字串 "1"/"0"
            self._state = bool(float(val))
            self._available = True
            self._failures = 0
            
            # 若是在發送控制指令後的確認事件，則觸發解鎖
            if self._confirm_event and self._expected_state is not None and self._state == self._expected_state:
                self._confirm_event.set()
                
            # 直接更新 HA 介面，消滅 Context Switch 負載
            self.async_write_ha_state()
        except (ValueError, TypeError):
            pass

    async def _async_update_state(self, *_):
        try:
            args = {"me": self._device["me"]}
            response = await self._api.send_command("ep", args, CMD_GET)
            if response.get("code") == 0 and "msg" in response:
                msg_data = response["msg"]
                if msg_data.get("stat") == 0:
                    self._available = False
                else:
                    self._available = True
                    data = msg_data.get("data", {})
                    if self._idx in data:
                        self._state = bool(data[self._idx].get("v"))
            self.async_write_ha_state()
        except Exception:
            self._failures += 1
            if self._failures >= 3:
                self._available = False
            self.async_write_ha_state()

    async def _async_write_state(self) -> None:
        self.async_write_ha_state()

    @property
    def available(self):
        return self._available

    @property
    def is_on(self):
        return self._state

    
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
            # 新增：將 API 的 'me' 欄位作為序號顯示
            serial_number=self._device.get('me'),
        )

    async def async_turn_on(self, **kwargs):
        await self._enqueue_command(1)

    async def async_turn_off(self, **kwargs):
        await self._enqueue_command(0)

    async def _enqueue_command(self, value: int) -> None:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_value = value
        self._pending_waiters.append(fut)
        if self._send_task is None or self._send_task.done():
            #self._send_task = self.hass.async_create_task(self._drain_pending_commands())
            self.hass.async_create_background_task(self._drain_pending_commands(), "lifesmart_update_task")
        await fut

    async def _drain_pending_commands(self) -> None:
        async with self._send_lock:
            while self._pending_value is not None:
                value = self._pending_value
                waiters = self._pending_waiters
                self._pending_value = None
                self._pending_waiters = []
                try:
                    await self._send_command(value)
                finally:
                    for w in waiters:
                        if not w.done():
                            w.set_result(None)

    async def _send_command(self, value: int) -> None:
        args = {
            "tag": "m",
            "me": self._device["me"],
            "idx": self._idx,
            "type": VAL_TYPE_ON if value == 1 else VAL_TYPE_OFF,
            "val": value
        }
        try:
            self._expected_state = bool(value)
            self._confirm_event = asyncio.Event()
            try:
                response = await self._api.send_command("ep", args, CMD_SET, 0.7)
            except asyncio.TimeoutError:
                response = {"code": 0}

            if response.get("code") == 0:
                self._available = True
                self._failures = 0
                try:
                    await asyncio.wait_for(self._confirm_event.wait(), timeout=1.5)
                except asyncio.TimeoutError:
                    await self._async_update_state()
            else:
                await self._async_update_state()
        except Exception:
            self._failures += 1
            if self._failures >= 3:
                self._available = False
            self.async_write_ha_state()
        finally:
            self._expected_state = None
            self._confirm_event = None