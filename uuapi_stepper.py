import msvcrt
import os
import re
import shutil
import subprocess
import tempfile
import time
import traceback
import winreg
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
import sys

from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By


REGISTER_URL = os.environ.get("UUAPI_REGISTER_URL", "https://uuapi.net/register")
TEMP_MAIL_URL = os.environ.get("UUAPI_TEMP_MAIL_URL", "https://10minutemail.one/")
PASSWORD = os.environ.get("UUAPI_PASSWORD", "ChangeMe_123456")
KEY_NAME = os.environ.get("UUAPI_KEY_NAME", "uuapi-key")
TIMEOUT_SECONDS = float(os.environ.get("UUAPI_TIMEOUT_SECONDS", "15"))
EMAIL_LOAD_TIMEOUT_SECONDS = float(os.environ.get("UUAPI_EMAIL_LOAD_TIMEOUT_SECONDS", "60"))
POLL_INTERVAL_SECONDS = float(os.environ.get("UUAPI_POLL_INTERVAL_SECONDS", "0.2"))
AUTOMATION_WINDOW_WIDTH = int(os.environ.get("UUAPI_WINDOW_WIDTH", "1400"))
AUTOMATION_WINDOW_HEIGHT = int(os.environ.get("UUAPI_WINDOW_HEIGHT", "1200"))
BROWSER_SILENT_MODE = os.environ.get("UUAPI_BROWSER_SILENT_MODE", "true").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
VISIBLE_WINDOW_X = int(os.environ.get("UUAPI_VISIBLE_WINDOW_X", "120"))
VISIBLE_WINDOW_Y = int(os.environ.get("UUAPI_VISIBLE_WINDOW_Y", "80"))
SILENT_WINDOW_X = int(os.environ.get("UUAPI_SILENT_WINDOW_X", "-2400"))
SILENT_WINDOW_Y = int(os.environ.get("UUAPI_SILENT_WINDOW_Y", "0"))
FLOW_MODE = int(os.environ.get("UUAPI_FLOW_MODE", "2"))

DEFAULT_KEY_OUTPUT_DIR = Path(os.environ.get("UUAPI_OUTPUT_DIR", str(Path(__file__).resolve().parent)))
FLOW_CONFIGS = {
    1: {
        "group_name": "反重力",
        "group_option_title": None,
        "key_output_path": DEFAULT_KEY_OUTPUT_DIR / "key.txt",
        "group_aliases": ["反重力"],
    },
    2: {
        "group_name": "Claude MAX满血 支持4.7",
        "group_option_title": "支持非Claude Code 使用",
        "key_output_path": DEFAULT_KEY_OUTPUT_DIR / "keypromax.txt",
        "group_aliases": [
            "Claude MAX满血 支持4.7",
            "Claude MAX",
            "MAX满血",
            "支持4.7",
        ],
    },
}

FLOW_CONFIG = FLOW_CONFIGS.get(FLOW_MODE)
if FLOW_CONFIG is None:
    raise ValueError(f"Unsupported FLOW_MODE: {FLOW_MODE}")

GROUP_NAME = os.environ.get("UUAPI_GROUP_NAME", FLOW_CONFIG["group_name"])
GROUP_OPTION_TITLE = os.environ.get("UUAPI_GROUP_OPTION_TITLE", FLOW_CONFIG["group_option_title"] or "") or None
GROUP_ALIASES = [
    alias.strip()
    for alias in os.environ.get(
        "UUAPI_GROUP_ALIASES",
        "|".join(FLOW_CONFIG.get("group_aliases", [])),
    ).split("|")
    if alias.strip()
]
KEY_OUTPUT_PATH = Path(
    os.environ.get("UUAPI_KEY_OUTPUT_PATH", str(FLOW_CONFIG["key_output_path"]))
).expanduser()

MODAL_CLOSE_TEXTS = (
    "标记已读",
    "知道了",
    "我知道了",
    "关闭",
    "确定",
    "确认",
    "明白了",
    "稍后",
)
CLIPBOARD_LOCK_PATH = Path(__file__).resolve().with_name(".uuapi_clipboard.lock")


def get_browser_window_position() -> tuple[int, int]:
    if BROWSER_SILENT_MODE:
        return SILENT_WINDOW_X, SILENT_WINDOW_Y
    return VISIBLE_WINDOW_X, VISIBLE_WINDOW_Y


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", value or "")


def group_match_candidates() -> list[str]:
    candidates: list[str] = []
    for value in [GROUP_NAME, GROUP_OPTION_TITLE, *GROUP_ALIASES]:
        normalized = normalize_text(value or "")
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def first_visible_element(
    driver: webdriver.Chrome,
    selectors: list[tuple[By, str]],
    timeout_seconds: float = TIMEOUT_SECONDS,
):
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        for by, value in selectors:
            elements = driver.find_elements(by, value)
            for element in elements:
                if element.is_displayed() and element.is_enabled():
                    return element
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Element not found in time: {selectors!r}")


@contextmanager
def interprocess_lock(lock_path: Path, poll_interval_seconds: float = 0.1):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_file:
        lock_file.seek(0, 2)
        if lock_file.tell() == 0:
            lock_file.write(b"0")
            lock_file.flush()

        while True:
            try:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                break
            except OSError:
                time.sleep(poll_interval_seconds)

        try:
            yield
        finally:
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)


def _detect_chrome_binary_and_major_version() -> tuple[Optional[str], Optional[int]]:
    chrome_candidates = [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
        Path.home() / "AppData" / "Local" / "Google" / "Chrome" / "Application" / "chrome.exe",
    ]

    registry_keys = [
        (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\Google\Chrome\BLBeacon"),
    ]

    for hive, key_path in registry_keys:
        try:
            with winreg.OpenKey(hive, key_path) as key:
                version_text, _ = winreg.QueryValueEx(key, "version")
            match = re.search(r"^(\d+)\.", version_text)
            if match:
                for chrome_path in chrome_candidates:
                    if chrome_path.exists():
                        return str(chrome_path), int(match.group(1))
        except OSError:
            pass

    for chrome_path in chrome_candidates:
        if not chrome_path.exists():
            continue

        try:
            result = subprocess.run(
                [str(chrome_path), "--version"],
                capture_output=True,
                timeout=5,
            )
            raw_output = result.stdout or result.stderr or b""
            try:
                version_text = raw_output.decode("utf-8")
            except UnicodeDecodeError:
                version_text = raw_output.decode("gbk", errors="ignore")
            match = re.search(r"(\d+)\.", version_text)
            if match:
                return str(chrome_path), int(match.group(1))
        except Exception:
            pass

    return None, None


def _find_local_chromedriver(chrome_major: Optional[int]) -> Optional[str]:
    if not chrome_major:
        return None

    cache_root = Path.home() / ".cache" / "selenium" / "chromedriver" / "win64"
    if not cache_root.exists():
        return None

    matching_drivers = sorted(cache_root.glob(f"{chrome_major}.*\\chromedriver.exe"), reverse=True)
    if matching_drivers:
        return str(matching_drivers[0])

    return None


def _prepare_writable_chromedriver(chrome_major: Optional[int]) -> Optional[str]:
    source_driver = _find_local_chromedriver(chrome_major)
    if not source_driver:
        return None

    target_dir = Path.cwd() / ".local_chromedriver"
    target_dir.mkdir(exist_ok=True)
    target_path = target_dir / f"chromedriver-{chrome_major}.exe"

    if not target_path.exists():
        shutil.copy2(source_driver, target_path)

    return str(target_path)


def _cleanup_driver_artifacts(driver: webdriver.Chrome | None) -> None:
    if not driver:
        return

    profile_dir = getattr(driver, "_codex_temp_profile_dir", None)
    if not profile_dir:
        return

    try:
        shutil.rmtree(profile_dir, ignore_errors=True)
    except Exception:
        pass


def build_driver() -> webdriver.Chrome:
    options = Options()
    temp_profile_dir = Path(tempfile.mkdtemp(prefix="uuapi-stepper-", dir=str(Path.cwd())))
    window_x, window_y = get_browser_window_position()

    options.add_argument("--incognito")
    options.add_argument(f"--user-data-dir={temp_profile_dir}")
    options.add_argument(f"--window-size={AUTOMATION_WINDOW_WIDTH},{AUTOMATION_WINDOW_HEIGHT}")
    options.add_argument(f"--window-position={window_x},{window_y}")
    options.add_argument("--disable-session-crashed-bubble")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-search-engine-choice-screen")
    options.page_load_strategy = "eager"

    chrome_binary, chrome_major = _detect_chrome_binary_and_major_version()
    local_driver = _prepare_writable_chromedriver(chrome_major)

    chrome_kwargs = {"options": options}
    if chrome_binary:
        options.binary_location = chrome_binary
    if local_driver:
        chrome_kwargs["service"] = Service(local_driver)

    driver = webdriver.Chrome(**chrome_kwargs)
    driver._codex_temp_profile_dir = str(temp_profile_dir)
    try:
        driver.set_window_position(window_x, window_y)
        driver.set_window_size(AUTOMATION_WINDOW_WIDTH, AUTOMATION_WINDOW_HEIGHT)
    except Exception:
        pass
    driver.get(REGISTER_URL)
    return driver


def wait_for_visible_element(
    driver: webdriver.Chrome,
    by: By,
    value: str,
    timeout_seconds: float = TIMEOUT_SECONDS,
):
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        elements = driver.find_elements(by, value)
        for element in elements:
            if element.is_displayed() and element.is_enabled():
                return element
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Element not found in time: {by}={value}")


def click_element(driver: webdriver.Chrome, element) -> None:
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});",
        element,
    )
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)


def real_user_mouse_click(driver: webdriver.Chrome, element) -> None:
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});",
        element,
    )
    ActionChains(driver).move_to_element(element).pause(0.08).click().perform()


def click_element_with_fallbacks(driver: webdriver.Chrome, element) -> None:
    try:
        real_user_mouse_click(driver, element)
        return
    except Exception:
        pass

    try:
        click_element(driver, element)
        return
    except Exception as native_error:
        raise native_error


def get_group_select_button(driver: webdriver.Chrome):
    return wait_for_visible_element(
        driver,
        By.CSS_SELECTOR,
        "div[data-tour='key-form-group'] button.select-trigger",
        timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
    )


def get_group_select_icon(driver: webdriver.Chrome):
    select_button = get_group_select_button(driver)
    candidate_selectors = [
        "span.select-icon",
        "span.select-icon svg",
    ]
    for selector in candidate_selectors:
        icons = select_button.find_elements(By.CSS_SELECTOR, selector)
        for icon in icons:
            if icon.is_displayed():
                return icon
    raise TimeoutError("Group select icon was not visible.")


def is_group_selected(driver: webdriver.Chrome) -> bool:
    select_button = get_group_select_button(driver)
    value_elements = select_button.find_elements(By.CSS_SELECTOR, ".select-value .truncate, .select-value")
    group_candidates = group_match_candidates()
    for element in value_elements:
        element_text = normalize_text(element.text or "")
        if any(candidate in element_text or element_text in candidate for candidate in group_candidates):
            return True
    return False


def get_visible_group_portal(driver: webdriver.Chrome):
    portals = driver.find_elements(
        By.CSS_SELECTOR,
        "div.select-dropdown-portal[role='listbox']",
    )
    for portal in portals:
        try:
            if not portal.is_displayed():
                continue

            visible_options = [
                option
                for option in portal.find_elements(By.CSS_SELECTOR, "div.select-options > div[role='option']")
                if option.is_displayed()
            ]
            search_inputs = [
                item
                for item in portal.find_elements(By.CSS_SELECTOR, "input.select-search-input")
                if item.is_displayed()
            ]
            if visible_options or search_inputs:
                return portal, visible_options
        except Exception:
            continue

    return None


def wait_for_group_dropdown(driver: webdriver.Chrome, timeout_seconds: float = EMAIL_LOAD_TIMEOUT_SECONDS):
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        portal_info = get_visible_group_portal(driver)
        if portal_info is not None:
            return portal_info
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Group dropdown did not finish rendering visible options.")


def set_search_input_value(driver: webdriver.Chrome, search_input, value: str) -> None:
    driver.execute_script(
        """
        const input = arguments[0];
        const value = arguments[1];
        input.focus();
        input.value = '';
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.value = value;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        search_input,
        value,
    )


def wait_for_email_value(driver: webdriver.Chrome) -> str:
    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        email_input = wait_for_visible_element(
            driver,
            By.CSS_SELECTOR,
            "input[aria-label='Email Address']",
            timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
        )
        email_address = (email_input.get_attribute("value") or "").strip()
        if email_address and email_address.lower() != "loading...":
            return email_address
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Temporary email address stayed in Loading... state.")


def wait_until_register_email_input_ready(driver: webdriver.Chrome):
    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        email_input = wait_for_visible_element(
            driver,
            By.ID,
            "email",
            timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
        )
        current_value = (email_input.get_attribute("value") or "").strip()
        placeholder = (email_input.get_attribute("placeholder") or "").strip()

        if current_value.lower() != "loading..." and placeholder != "Loading...":
            return email_input

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Register email input stayed in Loading... state.")


def wait_for_new_tab(driver: webdriver.Chrome, old_handles: list[str], timeout_seconds: float = 5.0) -> str:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        current_handles = list(driver.window_handles)
        new_handles = [handle for handle in current_handles if handle not in old_handles]
        if new_handles:
            return new_handles[-1]
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError("Failed to open a new tab in the current Chrome window.")


def open_temp_mail_and_get_address(driver: webdriver.Chrome, register_tab: str) -> tuple[str, str]:
    driver.switch_to.window(register_tab)
    before_handles = list(driver.window_handles)
    driver.execute_script("window.open(arguments[0], '_blank');", TEMP_MAIL_URL)

    temp_mail_tab = wait_for_new_tab(driver, before_handles)
    driver.switch_to.window(temp_mail_tab)
    email_address = wait_for_email_value(driver)

    driver.switch_to.window(register_tab)
    return temp_mail_tab, email_address


def fill_register_email(driver: webdriver.Chrome, email_address: str) -> None:
    email_input = wait_until_register_email_input_ready(driver)
    email_input.clear()
    email_input.send_keys(email_address)


def fill_register_password(driver: webdriver.Chrome) -> None:
    password_input = wait_for_visible_element(driver, By.ID, "password")
    password_input.clear()
    password_input.send_keys(PASSWORD)


def click_continue(driver: webdriver.Chrome) -> None:
    continue_button = first_visible_element(
        driver,
        [
            (By.CSS_SELECTOR, "form button[type='submit']"),
            (By.XPATH, "//button[@type='submit' and contains(normalize-space(.), '继续')]"),
            (By.XPATH, "//button[@type='submit' and contains(normalize-space(.), '下一步')]"),
        ],
    )
    click_element(driver, continue_button)


def try_extract_code_from_mail_view(driver: webdriver.Chrome) -> Optional[str]:
    code_blocks = driver.find_elements(By.CSS_SELECTOR, "div.content div.code")
    for code_block in code_blocks:
        if not code_block.is_displayed():
            continue
        code_text = (code_block.text or "").strip()
        if re.fullmatch(r"\d{6}", code_text):
            return code_text

    page_text = driver.find_element(By.TAG_NAME, "body").text
    match = re.search(r"Your verification code is[:\s]*([0-9]{6})", page_text, re.IGNORECASE)
    if match:
        return match.group(1)

    return None


def _click_first_matching_mail(driver: webdriver.Chrome) -> bool:
    candidate_xpaths = [
        (
            "//*[self::a or self::button or @role='button' or contains(@class, 'cursor-pointer')]"
            "[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'verification')]"
        ),
        (
            "//*[self::a or self::button or @role='button' or contains(@class, 'cursor-pointer')]"
            "[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'uuapi')]"
        ),
        (
            "//*[self::a or self::button or @role='button' or contains(@class, 'cursor-pointer')]"
            "[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'code')]"
        ),
        "(//div[contains(@class, 'cursor-pointer')])[1]",
        "(//tbody//tr)[1]",
    ]

    for xpath in candidate_xpaths:
        elements = driver.find_elements(By.XPATH, xpath)
        for element in elements:
            if not element.is_displayed():
                continue
            try:
                click_element(driver, element)
            except Exception:
                continue
            return True

    return False


def get_verification_code(driver: webdriver.Chrome, temp_mail_tab: str) -> str:
    driver.switch_to.window(temp_mail_tab)
    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS
    next_refresh_at = time.monotonic() + 5.0

    while time.monotonic() < deadline:
        code = try_extract_code_from_mail_view(driver)
        if code:
            return code

        if _click_first_matching_mail(driver):
            time.sleep(1.0)
            code = try_extract_code_from_mail_view(driver)
            if code:
                return code

        now = time.monotonic()
        if now >= next_refresh_at:
            driver.refresh()
            next_refresh_at = now + 5.0

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Verification code was not found in the mailbox in time.")


def fill_verification_code(driver: webdriver.Chrome, register_tab: str, code: str) -> None:
    driver.switch_to.window(register_tab)
    code_input = wait_for_visible_element(driver, By.ID, "code", timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS)
    code_input.clear()
    code_input.send_keys(code)


def click_verify_and_create_account(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    submit_button = first_visible_element(
        driver,
        [
            (By.XPATH, "//button[@type='submit' and contains(normalize-space(.), '验证并创建账户')]"),
            (By.XPATH, "//button[@type='submit' and contains(normalize-space(.), '验证并创建账号')]"),
            (By.XPATH, "//button[@type='submit' and contains(normalize-space(.), '创建账户')]"),
            (By.XPATH, "//button[@type='submit' and contains(normalize-space(.), '创建账号')]"),
            (By.CSS_SELECTOR, "form button[type='submit']"),
            (By.CSS_SELECTOR, "button[type='submit']"),
        ],
        timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
    )

    click_element(driver, submit_button)


def wait_for_dashboard_ready(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        create_key_buttons = driver.find_elements(By.CSS_SELECTOR, "button[data-tour='keys-create-btn']")
        menu_buttons = driver.find_elements(
            By.XPATH,
            "//button[.//*[name()='svg']/*[name()='path' and contains(@d, 'M3.75 6.75h16.5')]]",
        )
        api_key_links = driver.find_elements(By.CSS_SELECTOR, "a[data-tour='sidebar-my-keys'], a[href='/keys']")
        if any(button.is_displayed() for button in create_key_buttons):
            return
        if any(button.is_displayed() for button in menu_buttons):
            return
        if any(link.is_displayed() for link in api_key_links):
            return
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Dashboard navigation did not appear in time after creating the account.")


def click_sidebar_menu(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    create_key_buttons = driver.find_elements(By.CSS_SELECTOR, "button[data-tour='keys-create-btn']")
    if any(button.is_displayed() for button in create_key_buttons):
        return

    api_key_links = driver.find_elements(By.CSS_SELECTOR, "a[data-tour='sidebar-my-keys'], a[href='/keys']")
    if any(link.is_displayed() for link in api_key_links):
        return

    try:
        menu_button = wait_for_visible_element(
            driver,
            By.XPATH,
            "//button[.//*[name()='svg']/*[name()='path' and contains(@d, 'M3.75 6.75h16.5')]]",
            timeout_seconds=1.0,
        )
        click_element(driver, menu_button)
    except TimeoutError:
        print("Sidebar menu button was not present. Skipping this step.")


def click_api_keys_link(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    if "/keys" in driver.current_url:
        return

    create_key_buttons = driver.find_elements(By.CSS_SELECTOR, "button[data-tour='keys-create-btn']")
    if any(button.is_displayed() for button in create_key_buttons):
        return

    candidate_selectors = [
        (By.CSS_SELECTOR, "a[data-tour='sidebar-my-keys']"),
        (By.CSS_SELECTOR, "a[href='/keys']"),
        (By.XPATH, "//a[@href='/keys' and .//span[contains(normalize-space(.), 'API 密钥')]]"),
        (By.XPATH, "//a[@href='/keys' and .//*[contains(normalize-space(.), '密钥')]]"),
    ]

    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        for by, value in candidate_selectors:
            elements = driver.find_elements(by, value)
            for element in elements:
                if not element.is_displayed():
                    continue
                click_element(driver, element)
                return

        try:
            menu_button = wait_for_visible_element(
                driver,
                By.XPATH,
                "//button[.//*[name()='svg']/*[name()='path' and contains(@d, 'M3.75 6.75h16.5')]]",
                timeout_seconds=1.0,
            )
            click_element(driver, menu_button)
        except TimeoutError:
            pass

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Could not locate the 'API 密钥' navigation link.")


def wait_for_api_keys_page(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    wait_for_visible_element(
        driver,
        By.CSS_SELECTOR,
        "button[data-tour='keys-create-btn']",
        timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
    )


def click_create_key_button(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    create_button = wait_for_visible_element(
        driver,
        By.CSS_SELECTOR,
        "button[data-tour='keys-create-btn']",
        timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
    )
    click_element(driver, create_button)


def fill_key_name(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    key_name_input = wait_for_visible_element(
        driver,
        By.CSS_SELECTOR,
        "input[data-tour='key-form-name']",
        timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
    )
    key_name_input.clear()
    key_name_input.send_keys(KEY_NAME)


def close_welcome_modal_if_present(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    candidate_xpath = (
        "//div[contains(@class, 'rounded-3xl') and .//h2[contains(normalize-space(.), '欢迎使用 UU API')]]"
        "//button[.//span[contains(normalize-space(.), '标记已读')]]"
    )
    buttons = driver.find_elements(By.XPATH, candidate_xpath)
    visible_buttons = [button for button in buttons if button.is_displayed()]
    if not visible_buttons:
        return

    click_element_with_fallbacks(driver, visible_buttons[0])

    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        buttons = driver.find_elements(By.XPATH, candidate_xpath)
        if not any(button.is_displayed() for button in buttons):
            return
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Welcome modal did not close after clicking '标记已读'.")


def find_modal_close_button(driver: webdriver.Chrome):
    container_xpath = (
        "//*["
        "@role='dialog' "
        "or contains(@class, 'dialog') "
        "or contains(@class, 'modal') "
        "or contains(@class, 'drawer') "
        "or contains(@class, 'rounded-3xl')"
        "]"
    )
    containers = driver.find_elements(By.XPATH, container_xpath)

    for container in containers:
        try:
            if not container.is_displayed():
                continue

            buttons = [
                button
                for button in container.find_elements(By.XPATH, ".//button")
                if button.is_displayed() and button.is_enabled()
            ]
            if not buttons:
                continue

            for button in buttons:
                button_text = normalize_text(button.text)
                aria_label = normalize_text(button.get_attribute("aria-label") or "")
                title_text = normalize_text(button.get_attribute("title") or "")
                if any(
                    keyword in button_text or keyword in aria_label or keyword in title_text
                    for keyword in MODAL_CLOSE_TEXTS
                ):
                    return button

            if len(buttons) == 1:
                return buttons[0]
        except Exception:
            continue

    return None


def close_blocking_modals_if_present(
    driver: webdriver.Chrome,
    register_tab: str,
    timeout_seconds: float = 8.0,
) -> None:
    driver.switch_to.window(register_tab)
    deadline = time.monotonic() + timeout_seconds
    idle_rounds = 0

    while time.monotonic() < deadline and idle_rounds < 3:
        close_button = find_modal_close_button(driver)
        if close_button is None:
            idle_rounds += 1
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        try:
            click_element_with_fallbacks(driver, close_button)
            idle_rounds = 0
            time.sleep(0.6)
        except Exception:
            idle_rounds += 1
            time.sleep(POLL_INTERVAL_SECONDS)


def open_group_selector(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        select_button = get_group_select_button(driver)
        if select_button.get_attribute("aria-expanded") == "true":
            wait_for_group_dropdown(driver, timeout_seconds=2.0)
            return

        click_targets = []
        try:
            click_targets.append(get_group_select_icon(driver))
        except Exception:
            pass
        click_targets.append(select_button)

        for target in click_targets:
            try:
                click_element_with_fallbacks(driver, target)
                time.sleep(0.3)
                select_button = get_group_select_button(driver)
                if select_button.get_attribute("aria-expanded") == "true":
                    wait_for_group_dropdown(driver, timeout_seconds=2.0)
                    return
            except Exception:
                continue

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Could not open the group selector reliably.")


def choose_group(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    deadline = time.monotonic() + EMAIL_LOAD_TIMEOUT_SECONDS
    group_candidates = group_match_candidates()
    exact_group_names = []
    for candidate in [GROUP_NAME, *GROUP_ALIASES]:
        candidate = (candidate or "").strip()
        if candidate and candidate not in exact_group_names:
            exact_group_names.append(candidate)

    option_clauses = []
    for candidate in exact_group_names:
        option_clauses.extend(
            [
                f".//span[contains(@class, 'truncate') and normalize-space(.)='{candidate}']",
                f".//*[contains(@class, 'groupOptionItemBadge') and .//span[normalize-space(.)='{candidate}']]",
                f"contains(normalize-space(.), '{candidate}')",
            ]
        )
    if GROUP_OPTION_TITLE:
        option_clauses.append(f".//*[@title='{GROUP_OPTION_TITLE}']")

    option_xpath = f".//div[@role='option' and ({' or '.join(option_clauses)})]"

    while time.monotonic() < deadline:
        if is_group_selected(driver):
            return

        try:
            visible_portal, _ = wait_for_group_dropdown(driver, timeout_seconds=2.0)
        except TimeoutError:
            open_group_selector(driver, register_tab)
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        try:
            search_input = visible_portal.find_element(By.CSS_SELECTOR, "input.select-search-input")
            search_terms = exact_group_names or [GROUP_NAME]
            for search_term in search_terms:
                set_search_input_value(driver, search_input, search_term)
                time.sleep(0.4)

                elements = visible_portal.find_elements(By.XPATH, option_xpath)
                for element in elements:
                    if not element.is_displayed():
                        continue
                    try:
                        click_element_with_fallbacks(driver, element)
                        time.sleep(0.5)
                        if is_group_selected(driver):
                            return
                    except Exception:
                        continue
        except Exception:
            pass

        elements = visible_portal.find_elements(By.XPATH, option_xpath)
        for element in elements:
            if not element.is_displayed():
                continue
            try:
                click_element_with_fallbacks(driver, element)
                time.sleep(0.5)
                if is_group_selected(driver):
                    return
            except Exception:
                continue

        for element in visible_portal.find_elements(By.CSS_SELECTOR, "div.select-options > div[role='option']"):
            if not element.is_displayed():
                continue
            element_text = normalize_text(element.text or "")
            if not element_text:
                continue
            if not any(candidate in element_text or element_text in candidate for candidate in group_candidates):
                continue
            try:
                click_element_with_fallbacks(driver, element)
                time.sleep(0.5)
                if is_group_selected(driver):
                    return
            except Exception:
                continue
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Could not select group: {GROUP_NAME} | aliases={GROUP_ALIASES}")


def click_key_form_submit(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    submit_button = wait_for_visible_element(
        driver,
        By.CSS_SELECTOR,
        "button[data-tour='key-form-submit'][type='submit']",
        timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
    )
    click_element_with_fallbacks(driver, submit_button)


def click_copy_key_button(driver: webdriver.Chrome, register_tab: str) -> None:
    driver.switch_to.window(register_tab)
    copy_button = wait_for_visible_element(
        driver,
        By.XPATH,
        "//button[.//*[name()='svg']/*[name()='path' and contains(@d, 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7')]]",
        timeout_seconds=EMAIL_LOAD_TIMEOUT_SECONDS,
    )
    click_element_with_fallbacks(driver, copy_button)


def read_windows_clipboard() -> str:
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", "Get-Clipboard -Raw"],
        capture_output=True,
        check=True,
    )
    raw_output = result.stdout or b""
    try:
        return raw_output.decode("utf-8").strip()
    except UnicodeDecodeError:
        return raw_output.decode("gbk", errors="ignore").strip()


def wait_for_clipboard_text(previous_text: str, timeout_seconds: float = 10.0) -> str:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        current_text = read_windows_clipboard()
        if current_text and current_text != previous_text:
            return current_text
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Clipboard did not receive the copied API key in time.")


def append_key_record(api_key: str) -> None:
    KEY_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with KEY_OUTPUT_PATH.open("a", encoding="utf-8") as output_file:
        output_file.write(f"{api_key}\n")


def copy_api_key_and_append(driver: webdriver.Chrome, register_tab: str) -> str:
    with interprocess_lock(CLIPBOARD_LOCK_PATH):
        clipboard_before = read_windows_clipboard()
        click_copy_key_button(driver, register_tab)
        api_key = wait_for_clipboard_text(clipboard_before)
        append_key_record(api_key)
        return api_key


def main() -> None:
    driver = None
    try:
        driver = build_driver()
        register_tab = driver.current_window_handle
        temp_mail_tab, email_address = open_temp_mail_and_get_address(driver, register_tab)
        fill_register_email(driver, email_address)
        fill_register_password(driver)
        click_continue(driver)
        verification_code = get_verification_code(driver, temp_mail_tab)
        fill_verification_code(driver, register_tab, verification_code)
        click_verify_and_create_account(driver, register_tab)
        wait_for_dashboard_ready(driver, register_tab)
        click_sidebar_menu(driver, register_tab)
        click_api_keys_link(driver, register_tab)
        wait_for_api_keys_page(driver, register_tab)
        close_blocking_modals_if_present(driver, register_tab)
        click_create_key_button(driver, register_tab)
        fill_key_name(driver, register_tab)
        open_group_selector(driver, register_tab)
        choose_group(driver, register_tab)
        click_key_form_submit(driver, register_tab)
        api_key = copy_api_key_and_append(driver, register_tab)

        print(f"Opened: {REGISTER_URL}")
        print(f"Filled email: {email_address}")
        print(f"Filled password: {PASSWORD}")
        print(f"Filled verification code: {verification_code}")
        print(f"Prepared API key form with name: {KEY_NAME}")
        print(f"Selected group: {GROUP_NAME}")
        print(f"Copied API key: {api_key}")
        print(f"Appended credentials to: {KEY_OUTPUT_PATH}")
        return 0
    except KeyboardInterrupt:
        print("\nClosing browser...")
        return 130
    except Exception:
        print("Failed to launch Chrome.")
        traceback.print_exc()
        return 1
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        _cleanup_driver_artifacts(driver)


if __name__ == "__main__":
    sys.exit(main())
