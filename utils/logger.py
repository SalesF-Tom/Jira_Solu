# utils/logger.py
import logging

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[94m',    # Azul
        'INFO': '\033[92m',     # Verde
        'WARNING': '\033[93m',  # Amarillo
        'ERROR': '\033[91m',    # Rojo
        'CRITICAL': '\033[95m', # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        level_color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{level_color}{message}{self.RESET}"

def configurar_logger(nombre="jira_etl"):
    logger = logging.getLogger(nombre)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler (sin color)
    file_handler = logging.FileHandler("etl.log", encoding="utf-8")
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler (con color)
    console_handler = logging.StreamHandler()
    console_formatter = ColoredFormatter("%(asctime)s [%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    print("Logger configurado con handlers y colores.")
    return logger












def configurar_logger_NOVA(nombre="jira_etl"):
    logger = logging.getLogger(nombre)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        print("Agregando handler al logger")  # Debug local
        handler = logging.FileHandler("etl.log", encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        print("Logger ya tenía handler")  # Debug local

    return logger
