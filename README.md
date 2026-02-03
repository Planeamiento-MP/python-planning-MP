# Análisis de Datos Automatizado con Envío de Correos

Sistema en Python + Docker que ejecuta análisis de datos a horas programadas, envía reportes por correo y/o ejecuta el flujo **Origen de Inventario** (conexión a SAP HANA, procesamiento y guardado en PostgreSQL).

## Estructura del proyecto

```
python-jobs/
├── src/
│   ├── main.py              # Punto de entrada con scheduler (APScheduler)
│   ├── config.py            # Configuración desde variables de entorno
│   ├── db.py                # Conexión PostgreSQL y funciones reutilizables
│   ├── analyzer.py          # Lógica de análisis de datos
│   ├── email_sender.py      # Envío de correos SMTP
│   ├── jobs/
│   │   ├── tasks.py         # Tareas registradas (analisis_email, origen_inventario)
│   │   └── registry.py     # Registro de tareas
│   ├── origen_inventario/   # Flujo Origen de Inventario (lógica de negocio)
│   │   ├── main.py         # Orquestador: Validacion OV/OF, Stock_0, Kardex, BD
│   │   ├── validacion_ov.py
│   │   ├── validacion_of.py
│   │   ├── stock_0.py
│   │   └── stock_historico.py
│   └── conexiones_sap/      # Scripts de conexión a SAP HANA (consultas)
│       ├── detalle_ordenes_fabricacion.py
│       ├── detalle_ordenes_fabricacion1.py
│       ├── detalle_ordenes_venta.py
│       ├── detalle_ordenes_ventaV2.py
│       ├── kardex_general_prov.py
│       ├── kardex_mpsa_version_ap_prueba.py
│       └── socios.py
├── Dockerfile
├── docker-compose.yml
├── deploy.sh                # Reconstruir y levantar (Linux/EC2); requiere chmod +x
├── build.ps1                # Reconstruir y levantar (Windows)
├── rename_for_git.ps1       # Forzar renombrado a minúsculas en Git (Windows)
├── requirements.txt
└── .env.example
```

## Requisitos previos

- Docker y Docker Compose
- Cuenta de correo con SMTP (Gmail, Outlook, etc.)

## Configuración

1. Copia el archivo de ejemplo de variables de entorno:

```bash
cp .env.example .env
```

2. Edita `.env` con tus credenciales SMTP. Para Gmail necesitas una [Contraseña de aplicación](https://support.google.com/accounts/answer/185833):

```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=tu_email@gmail.com
SMTP_PASSWORD=tu_app_password
SMTP_TO=destinatario@ejemplo.com
```

3. Ajusta las tareas programadas: `cron:nombre_tarea` separadas por `;`:

| SCHEDULES | Significado |
|-----------|-------------|
| `0 9 * * *:analisis_email` | Análisis + email a las 9:00 |
| `0 6 * * *:origen_inventario` | Flujo Origen de Inventario + BD diario a las 6:00 |
| `0 6 * * *:origen_inventario;0 9 * * *:analisis_email` | Origen BD 6:00, análisis+email 9:00 |

Tareas disponibles: `analisis_email`, `origen_inventario`. Puedes añadir más en `src/jobs/tasks.py` con el decorador `@register`.

4. **RUN_ON_STARTUP** (opcional): si está en `true`, al iniciar el contenedor se ejecuta de inmediato la primera tarea de `SCHEDULES`, sin esperar a la hora programada. Útil para pruebas.

## Despliegue en Ubuntu

### Opción 1: Docker Compose (recomendado)

El contenedor corre 24/7 y el scheduler interno ejecuta el análisis a la hora configurada.

```bash
# Instalar Docker en Ubuntu (si no lo tienes)
sudo apt update
sudo apt install -y docker.io docker-compose
sudo usermod -aG docker $USER
# Cierra sesión y vuelve a entrar

# Clonar/copiar el proyecto
cd /ruta/del/proyecto

# Configurar .env
cp .env.example .env
nano .env   # Editar credenciales

# Construir y ejecutar
docker-compose up -d --build

# Ver logs
docker-compose logs -f
```

Si más adelante usas `./deploy.sh` para reconstruir, la primera vez hazlo ejecutable: `chmod +x deploy.sh`.

### Opción 2: Cron + Docker (ejecutar a una hora específica)

Útil si prefieres usar el cron del sistema en lugar del scheduler interno:

1. Crea un script `run_cron.sh` en el proyecto:

```bash
#!/bin/bash
cd /ruta/del/proyecto
docker-compose run --rm -e RUN_MODE=once analisis-email
```

2. Hazlo ejecutable y añádelo al crontab:

```bash
chmod +x run_cron.sh
crontab -e
```

Añade esta línea (ejecuta a las 9:00 AM todos los días):

```
0 9 * * * /ruta/del/proyecto/run_cron.sh >> /var/log/analisis-email.log 2>&1
```

### Opción 3: Solo Docker (sin Compose)

```bash
docker build -t analisis-email .
docker run -d --name analisis-email --env-file .env --restart unless-stopped analisis-email
```

## Modos de ejecución

| Variable | Valores | Descripción |
|----------|---------|-------------|
| `RUN_MODE` | `scheduler` | El contenedor corre 24/7 y ejecuta a la hora configurada |
| `RUN_MODE` | `once` | Ejecuta la primera tarea de SCHEDULES una vez y termina (ideal para cron) |
| `RUN_ON_STARTUP` | `true` / `false` | Si es `true`, al iniciar ejecuta de inmediato la primera tarea de SCHEDULES (útil para pruebas) |

## Desarrollo local (sin Docker)

```bash
python -m venv .venv
source .venv/bin/activate   # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Editar .env
python src/main.py
```

## Base de datos PostgreSQL

Con Docker Compose se levanta un contenedor PostgreSQL; la aplicación espera a que esté listo antes de arrancar.

**Variables en `.env`:** `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`. En Docker el host es `postgres` (ya configurado en `docker-compose`); en local usa `DB_HOST=localhost`.

**SAP HANA (flujo Origen de Inventario):** `HANA_HOST`, `HANA_PORT`. En Docker con túnel SSH en el host usa `HANA_HOST=host.docker.internal` y que el túnel escuche en `0.0.0.0:30015`; en local `HANA_HOST=127.0.0.1`.

**Uso del módulo `src/db.py`** (sentencias y funciones reutilizables):

```python
from db import execute, fetch_one, fetch_all, transaction, call_function

# INSERT/UPDATE/DELETE
execute("INSERT INTO logs (mensaje) VALUES (%s)", ("hola",))

# SELECT una fila (diccionario o None)
row = fetch_one("SELECT * FROM usuarios WHERE id = %s", (1,))

# SELECT todas las filas (lista de diccionarios)
filas = fetch_all("SELECT * FROM eventos ORDER BY fecha DESC")

# Varias sentencias en una transacción
with transaction() as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO ...")
        cur.execute("UPDATE ...")

# Llamar a una función de PostgreSQL
resultado = call_function("mi_funcion", (arg1, arg2))
```

### Job Origen de Inventario (almacenamiento diario en BD)

La tarea `origen_inventario` ejecuta el flujo de `src/origen_inventario/main.py`:

1. **Conexión a SAP HANA**: usa los scripts de `src/conexiones_sap/` (consultas a HANA: órdenes de venta/fabricación, kardex, socios). Las variables `HANA_HOST` y `HANA_PORT` en `.env` definen la conexión; en Docker con túnel SSH en el host usa `HANA_HOST=host.docker.internal` y que el túnel escuche en `0.0.0.0:30015`.
2. **Procesamiento**: `origen_inventario` carga `validacion_ov.py`, `validacion_of.py`, `stock_0.py` y `kardex_mpsa_version_ap_prueba.py` (este último desde `conexiones_sap`), calcula el resultado y genera el archivo `Resultado_YYYYMMDD.txt`.
3. **PostgreSQL**: inserta cada fila del resultado en la tabla **`origen_inventario`** con **una columna por campo** (no JSON): `id`, `fecha_corte`, `fecha_carga`, y una columna por cada campo del resultado (OF, OV, ItemCode, Cant Final, etc.). Los nombres con espacios o puntos se guardan entre comillas en PostgreSQL.

Para ejecutarlo **diariamente** (por ejemplo a las 6:00):

```
SCHEDULES="0 6 * * *:origen_inventario"
```

O combínalo con otras tareas: `SCHEDULES="0 6 * * *:origen_inventario;0 9 * * *:analisis_email"`.

**Logs**: los scripts de `conexiones_sap` y `origen_inventario` usan `logging` (no `print`), por lo que todo el progreso aparece en `docker logs`.

## Añadir tareas personalizadas

Edita `src/jobs/tasks.py` y usa el decorador `@register`:

```python
from jobs.registry import register

@register("mi_tarea")
def mi_tarea():
    # Tu lógica aquí
    print("Ejecutando mi tarea...")
```

Luego en `.env`: `SCHEDULES="0 10 * * *:mi_tarea"` o combina con otras: `SCHEDULES="0 10 * * *:mi_tarea;0 15 * * *:analisis_email"`.

## Personalizar el análisis

Edita `src/analyzer.py` para adaptar el análisis a tus datos reales. Puedes:

- Conectarte a una base de datos
- Leer archivos CSV/Excel
- Cambiar las métricas calculadas
- Modificar el formato del reporte HTML

## Reconstruir y actualizar variables

Las variables de `.env` se cargan al **crear** el contenedor, no al reiniciarlo. Si cambiaste el `.env`, usa:

**Windows (PowerShell):**
```powershell
.\build.ps1
```

**Linux / EC2:**
```bash
chmod +x deploy.sh   # Solo la primera vez (Permission denied si no)
./deploy.sh
```

Ambos hacen: `down` → `build --no-cache` → `up -d` para recrear todo con la config actual.

## Solución de problemas

### Git no detecta renombres a minúsculas (Windows)

En Windows el sistema de archivos suele ser insensible a mayúsculas/minúsculas, por eso Git a veces no detecta solo un cambio de capitalización (p. ej. `Main.py` → `main.py`). Para forzar que Git registre los renombres, usa el script de dos pasos:

```powershell
.\rename_for_git.ps1
git status
git add -A
git commit -m "fix: renombrar archivos a minúsculas (conexiones_sap y origen_inventario)"
git push
```

El script renombra en dos pasos (nombre temporal y luego nombre final) los archivos de `src/conexiones_sap/` y `src/origen_inventario/` para que Git los detecte.

### Error "exporting to image" al hacer build en Windows

Este error suele aparecer con Docker BuildKit. Prueba:

**Opción 1** – Usar el script de build (builder legacy):

```powershell
.\build.ps1
```

**Opción 2** – Desactivar BuildKit manualmente:

```powershell
$env:DOCKER_BUILDKIT = "0"
docker-compose build --no-cache
```

**Opción 3** – Usar `docker build` directamente:

```powershell
docker build -t analisis-email .
docker-compose up -d
```

## Notas

- Sin credenciales SMTP configuradas, el sistema simula el envío y muestra el reporte en los logs.
- Para producción, considera usar secretos de Docker o un gestor de secretos.
