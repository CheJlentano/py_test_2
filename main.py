import os
import re
import time
import json
import logging
from ftplib import FTP_TLS, FTP

SLEEP_INTERVAL_SECONDS = 5 * 60
SERVERS_JSON = 'servers.json'
LOG_FILE = 'log.txt'

pattern = re.compile(r'^(\d+)_(\d{4})-(\d{2})-(\d{2})_(\d{2})_(\d{2})_(\d{2})\.wav$')

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()  # вывод также в консоль
    ]
)

class Explicit_FTP_TLS(FTP_TLS):
    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn, server_hostname=self.host, session=self.sock.session)
        return conn, size

def load_servers():
    with open(SERVERS_JSON, 'r', encoding='utf-8') as f:
        return json.load(f)

def connect_ftps(config):
    ftps = Explicit_FTP_TLS()
    ftps.connect(config['host'], config.get('port', 21))
    ftps.login(config['username'], config['password'])
    ftps.prot_p()
    return ftps

def list_remote_files(ftps, remote_dir):
    ftps.cwd(remote_dir)
    files = ftps.nlst()
    return [f for f in files if pattern.match(f)]

def download_file(ftps, filename, local_dir):
    local_path = os.path.join(local_dir, filename)
    with open(local_path, 'wb') as f:
        ftps.retrbinary(f'RETR {filename}', f.write)
    remote_size = ftps.size(filename)
    return local_path, remote_size

def check_size(local_path, remote_size):
    try:
        local_size = os.path.getsize(local_path)
        if local_size == remote_size:
            logging.info(f"✅ Размер совпадает: {local_size} байт")
            return True
        else:
            logging.warning(f"❌ Размер не совпадает: локально {local_size}, на сервере {remote_size}")
            return False
    except Exception as e:
        logging.error(f"Ошибка при проверке размера файла: {e}")
        return False

def rename_file(old_path, z_prefix, local_dir):
    old_name = os.path.basename(old_path)
    m = pattern.match(old_name)
    if not m:
        logging.warning(f"❌ Пропущен (не совпадает с шаблоном): {old_name}")
        return None
    num, yyyy, dd, mm, hh, mi, ss = m.groups()
    new_name = f"{z_prefix}_{num}-{dd}_{mm}_{yyyy}-{hh}_{mi}_{ss}.wav"
    new_path = os.path.join(local_dir, new_name)
    os.rename(old_path, new_path)
    logging.info(f"🔄 Переименован: {old_name} → {new_name}")
    return new_name

def delete_remote_files(ftps, files, remote_dir):
    ftps.cwd(remote_dir)
    for f in files:
        try:
            ftps.delete(f)
            logging.info(f"🗑️ Удалён файл на сервере: {f}")
        except Exception as e:
            logging.warning(f"⚠️ Ошибка удаления файла {f}: {e}")

def process_server(config):
    logging.info(f"🔗 Подключение к серверу: {config['host']}")

    local_dir = config.get('local_dir')
    if not local_dir:
        logging.error("❌ local_dir не указан в конфигурации сервера")
        return

    os.makedirs(local_dir, exist_ok=True)

    try:
        ftps = connect_ftps(config)
        logging.info("🔐 Подключение успешно")

        remote_files = list_remote_files(ftps, config['remote_dir'])
        if not remote_files:
            logging.info("📂 Нет подходящих файлов.")
        else:
            for file in remote_files:
                logging.info(f"⬇️ Загружается: {file}")
                local_path, remote_size = download_file(ftps, file, local_dir)

                if check_size(local_path, remote_size):
                    z_prefix = config.get('z_prefix', 'DEFAULT')
                    renamed = rename_file(local_path, z_prefix, local_dir)
                    if renamed:
                        delete_remote_files(ftps, [file], config['remote_dir'])
                else:
                    logging.warning(f"⚠️ Пропущен файл {file} (размер не совпадает)")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")
    finally:
        try:
            ftps.quit()
            logging.info("🔒 Соединение закрыто")
        except:
            pass

def main_loop():
    while True:
        logging.info("🔁 Новый общий цикл")
        try:
            servers = load_servers()
        except Exception as e:
            logging.error(f"❌ Не удалось загрузить список серверов: {e}")
            time.sleep(60)
            continue

        for server in servers:
            process_server(server)

        logging.info(f"⏳ Ожидание {SLEEP_INTERVAL_SECONDS // 60} минут...\n")
        time.sleep(SLEEP_INTERVAL_SECONDS)

if __name__ == '__main__':
    main_loop()
