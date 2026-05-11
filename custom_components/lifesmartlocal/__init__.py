"""The LifeSmart Local integration."""
import asyncio
import logging
import re
import socket
import voluptuous as vol
from typing import Any
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.components import network

from .const import DOMAIN, PLATFORMS, API_TIMEOUT
from .api import LifeSmartAPI

_LOGGER = logging.getLogger(__name__)

def _get_local_ip_for_target(target_ip: str) -> str:
    """透過 OS 路由表動態偵測與網關通訊的正確本地 IP，拒絕寫死。"""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # UDP connect 不會實際發送封包，但會強制作業系統查路由表並綁定正確的網卡 IP
        s.connect((target_ip, 12348))
        local_ip = s.getsockname()[0]
    except Exception:
        local_ip = "0.0.0.0"
    finally:
        s.close()
    return local_ip


def generate_entity_id(device_type, hub_id, device_id, idx=None):
    """Generate a valid, predictable entity ID."""
    if idx:
        raw_id = f"{device_type}_{hub_id}_{device_id}_{idx}"
    else:
        raw_id = f"{device_type}_{hub_id}_{device_id}"
    return re.sub(r"[^a-zA-Z0-9_]+", "_", raw_id).lower()


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up LifeSmart Local from a config entry."""
    if "host" not in entry.data:
        _LOGGER.error("找不到 host 設定，請刪除此整合並重新加入")
        return False

    api = LifeSmartAPI(
        host=entry.data["host"],
        model=entry.data.get("model", "OD_ALI_TECH"),
        token=entry.data.get("token", ""),
        timeout=API_TIMEOUT,
        local_port=0, # 強制動態 Port，徹底避開 Address in use 報錯
    )

    try:
        await api.async_start()
        devices_data = await api.discover_devices()
    except Exception as err:
        await api.async_stop()
        raise ConfigEntryNotReady(f"Failed to connect or discover devices: {err}") from err

    # 取得 HA 的正確對內網路 IP，用來接收網關的 UDP 狀態推播
    local_ip = _get_local_ip_for_target(entry.data["host"])
    if local_ip == "0.0.0.0":
        _LOGGER.warning("Could not determine local IP via socket, falling back to network adapters")
        adapters = await network.async_get_adapters(hass)
        for adapter in adapters:
            if adapter["ipv4"]:
                local_ip = adapter["ipv4"][0]["address"]
                break

    # 初次向網關註冊 Event Server (事件接收伺服器)
    try:
        await api.configure_event_service(local_ip, api.local_port)
        _LOGGER.debug("Successfully registered Event Server at %s:%s", local_ip, api.local_port)
    except Exception as err:
        _LOGGER.error("Failed to register Event Server: %s", err)

    async def _async_keep_alive():
        while True:
            await asyncio.sleep(240)
            try:
                # 🚀 終極網路容錯：每次心跳前動態抓取最新 IP，無懼 DHCP 變動或網卡重啟
                current_local_ip = _get_local_ip_for_target(entry.data["host"])
                if current_local_ip == "0.0.0.0":
                    continue # 網路斷線中，等待下一輪
                    
                response = await api.configure_event_service(current_local_ip, api.local_port)
                if response and response.get("code") == 0:
                    api.set_gateway_status(True)
                else:
                    api.set_gateway_status(False)
            except Exception as e:
                _LOGGER.warning("LifeSmart 網關無回應: %s", e)
                api.set_gateway_status(False)
    
    # 將 Keep-alive 任務綁定到 HA 的事件迴圈
    keep_alive_task = hass.loop.create_task(_async_keep_alive())
    
    # 建立一個標準的註銷函數，避免 HA 2026 誤判返回值
    def cancel_keep_alive():
        if not keep_alive_task.done():
            keep_alive_task.cancel()
            
    # 註冊到整合解除載入的事件中
    entry.async_on_unload(cancel_keep_alive)
    
    # 擷取並整理設備清單
    devices_list = []
    if isinstance(devices_data, dict) and isinstance(devices_data.get("msg"), list):
        devices_list = devices_data["msg"]

    domain_data = hass.data.setdefault(DOMAIN, {"entries": {}, "_services_registered": False})
    domain_data["entries"][entry.entry_id] = {"api": api, "devices": devices_list}

    # 註冊你原本實作的自訂服務 (例如：紅外線發送 send_keys)
    _async_register_services(hass)

    # 將設定檔轉發給所有 Platform (switch, sensor, cover, remote 等) 進行載入
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        entry_data = hass.data[DOMAIN]["entries"].pop(entry.entry_id)
        api = entry_data.get("api")
        if api is not None:
            await api.async_stop()
        
        if not hass.data[DOMAIN]["entries"]:
            hass.data.pop(DOMAIN)
            
    return unload_ok


def _async_register_services(hass: HomeAssistant) -> None:
    """Register custom services for LifeSmart Local."""
    domain_data = hass.data.setdefault(DOMAIN, {"entries": {}, "_services_registered": False})
    if domain_data["_services_registered"]:
        return

    async def _handle_send_keys(call: ServiceCall) -> None:
        remote_id = call.data["remote_id"]
        keys = call.data["keys"]
        if isinstance(keys, str):
            keys_to_send = [keys]
        else:
            keys_to_send = list(keys)

        for entry_data in hass.data.get(DOMAIN, {}).get("entries", {}).values():
            api = entry_data.get("api")
            if isinstance(api, LifeSmartAPI):
                for key in keys_to_send:
                    await api.send_remote_key(remote_id, key)

    hass.services.async_register(
        DOMAIN,
        "send_keys",
        _handle_send_keys,
        schema=vol.Schema(
            {
                vol.Required("remote_id"): str,
                vol.Required("keys"): vol.Any(str, [str]),
            }
        ),
    )

    domain_data["_services_registered"] = True
    
    
from homeassistant.helpers.device_registry import DeviceEntry

async def async_remove_config_entry_device(
    hass: HomeAssistant, config_entry: ConfigEntry, device_entry: DeviceEntry
) -> bool:
    """允許使用者從 Home Assistant UI 手動刪除不再使用的 LifeSmart 設備。"""
    _LOGGER.info("Removing LifeSmart device: %s", device_entry.identifiers)
    # 返回 True 允許 HA 核心將該設備及其附屬的實體從資料庫中徹底清除
    return True