from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time
import logging

_log = logging.getLogger("Codigo.Prueba_Paginas")
_log.info("Inicio de librerias")

options = Options()
options.binary_location = "/usr/bin/chromium"
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--ignore-certificate-errors")

# Crear el navegador solo una vez con las opciones correctas
driver = webdriver.Chrome(options=options)

# Abrir URL
driver.get("https://sapbo.marco.com.pe:40000/ControlCenter/sbo.html?saml2TokenRef=01368ad1-240c-4782-9c30-fc406baf2c6c")
time.sleep(2)
_log.info("Abre pagina web")

# Interactuar con la página
combo = driver.find_element(By.ID, "sbo_company")
select = Select(combo)
select.select_by_value("SBO_MARCO_PE")
_log.info("Selecciona opcion")

input_usuario = driver.find_element(By.ID, "sbo_user")
input_usuario.send_keys("AsiPla06")

input_contraseña = driver.find_element(By.ID, "sbo_password")
input_contraseña.send_keys("MP1234")
_log.info("Usuario y contraseña")

boton = driver.find_element(By.ID, "logon_sbo_btn")
boton.click()
_log.info("Click boton")

# Cerrar navegador
driver.quit()
_log.info("Cierra navegador")





