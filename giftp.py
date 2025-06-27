from telethon import TelegramClient, sync, errors
import asyncio
import aiohttp
import re
import csv
import time
import logging
from tqdm import tqdm
import os
from telethon.errors import FloodWaitError, ChannelInvalidError, UsernameInvalidError

# logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nft_parser.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# cfg
API_ID = ''
API_HASH = ''
PHONE = '+'  # В формате +12345678901

# ссылка на подарок (LolPop можно заменить на другой подарок)
BASE_URL = 'https://t.me/nft/LolPop-{}'

# файлы для результатов
RESULTS_FILE = 'nft_owners.csv'
VALID_NFTS_FILE = 'valid_nft_links.csv'

async def check_nft_link(client, nft_id, session):
    try:
        url = BASE_URL.format(nft_id)
        logger.debug(f"Проверка NFT ID {nft_id}, URL: {url}")
        
        
        try:

            nft_message = await client.get_messages("nft", ids=nft_id)
            if nft_message:
                
                if hasattr(nft_message, 'sender') and nft_message.sender:
                    sender = nft_message.sender
                    username = sender.username if hasattr(sender, 'username') and sender.username else None
                    first_name = sender.first_name if hasattr(sender, 'first_name') else None
                    last_name = sender.last_name if hasattr(sender, 'last_name') else None
                    
                    if username:
                        return nft_id, f"@{username}", url
                    elif first_name:
                        full_name = f"{first_name} {last_name}" if last_name else first_name
                        return nft_id, full_name, url
                    
                
                if hasattr(nft_message, 'message') and nft_message.message:
                    owner_match = re.search(r'Владелец:\s*(.*?)(?:\n|$)', nft_message.message)
                    if owner_match:
                        return nft_id, owner_match.group(1).strip(), url
        except Exception as e:
            logger.debug(f"Ошибка при получении данных через Telethon для NFT ID {nft_id}: {str(e)}")
        

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logger.warning(f"NFT ID {nft_id}: получен статус {response.status}")
                return nft_id, None, None
            
            html = await response.text()
            
            
            owner_patterns = [
                r'Владелец:[\s\n]*<[^>]*>([^<]+)</[^>]*>',
                r'Владелец:[\s\n]*([^<\n]+)',
                r'owner["\']?\s*:\s*["\']([^"\']+)["\']',
                r'username["\']?\s*:\s*["\']([^"\']+)["\']',
                r'data-username=["\']([^"\']+)["\']',
                r'class=["\'](?:.*?)owner(?:.*?)["\'][^>]*>([^<]+)<',
                r'tgme_page_owner_name">([^<]+)<'
            ]
            
            for pattern in owner_patterns:
                owner_match = re.search(pattern, html, re.IGNORECASE)
                if owner_match:
                    return nft_id, owner_match.group(1).strip(), url
            

            return nft_id, None, None
            
    except Exception as e:
        logger.error(f"Ошибка при обработке NFT ID {nft_id}: {str(e)}")
        return nft_id, None, None

async def process_batch(client, batch, session):
    tasks = []
    for nft_id in batch:
        tasks.append(check_nft_link(client, nft_id, session))
    
    return await asyncio.gather(*tasks)

async def main():
    # кфг на парс
    start_id = 1
    end_id = 100000
    batch_size = 5
    delay = 2
    SAVE_INTERVAL = 100
    RESUME_FROM = None  # поставьте число, если нужно продолжить с определённого ID


    max_retries = 5
    for attempt in range(max_retries):
        try:
            client = TelegramClient('nft_parser_session', API_ID, API_HASH, connection_retries=5, timeout=30)
            await client.connect()
            break
        except Exception as e:
            if "database is locked" in str(e).lower():
                logger.warning(f"Database locked, retrying ({attempt + 1}/{max_retries})...")
                time.sleep(5)
            else:
                raise
    else:
        logger.critical("Failed to connect after maximum retries due to database lock")
        return


    if not await client.is_user_authorized():
        logger.info("Требуется авторизация. Пожалуйста, введите код, который был отправлен в Telegram.")
        await client.sign_in(PHONE)
        code = input('Введите код авторизации: ')
        try:
            await client.sign_in(code=code)
        except errors.SessionPasswordNeededError:
            logger.info("Обнаружена двухэтапная аутентификация. Введите ваш пароль.")
            password = input("Введите пароль двухэтапной аутентификации: ")
            await client.sign_in(password=password)

    if RESUME_FROM is not None and isinstance(RESUME_FROM, int) and RESUME_FROM > start_id:
        start_id = RESUME_FROM
        logger.info(f"Продолжаем парсинг с ID {start_id}")

    file_exists = os.path.isfile(RESULTS_FILE)
    valid_file_exists = os.path.isfile(VALID_NFTS_FILE)


    with open(RESULTS_FILE, 'a' if file_exists and RESUME_FROM else 'w', newline='', encoding='utf-8') as csvfile, \
         open(VALID_NFTS_FILE, 'a' if valid_file_exists and RESUME_FROM else 'w', newline='', encoding='utf-8') as valid_csvfile:
        writer = csv.writer(csvfile)
        valid_writer = csv.writer(valid_csvfile)


        if not file_exists or not RESUME_FROM:
            writer.writerow(['NFT ID', 'Владелец'])
        if not valid_file_exists or not RESUME_FROM:
            valid_writer.writerow(['NFT ID', 'Ссылка'])

        found_count = 0
        valid_count = 0

        async with aiohttp.ClientSession() as session:
            for batch_start in tqdm(range(start_id, end_id + 1, batch_size), desc="Парсинг NFT подарков"):
                batch_end = min(batch_start + batch_size - 1, end_id)
                batch = range(batch_start, batch_end + 1)

                try:
                    results = await process_batch(client, batch, session)

                    for nft_id, owner, url in results:
                        if nft_id is not None and owner and url:
                            # запись валид NFT в оба файла
                            writer.writerow([nft_id, owner])
                            valid_writer.writerow([nft_id, url])
                            valid_count += 1
                            found_count += 1

                            if nft_id % SAVE_INTERVAL == 0:
                                csvfile.flush()
                                valid_csvfile.flush()
                                logger.info(f"Обработано {nft_id} NFT, найдено владельцев: {found_count}, валидных ссылок: {valid_count}")

                    time.sleep(delay)

                except FloodWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"Достигнут лимит запросов к API. Ожидание {wait_time} секунд...")
                    time.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Произошла ошибка при обработке пакета {batch_start}-{batch_end}: {str(e)}")

    await client.disconnect()
    logger.info(f"Парсинг завершен. Результаты сохранены в {RESULTS_FILE}, валидные ссылки в {VALID_NFTS_FILE}")
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Парсинг был прерван пользователем.")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")