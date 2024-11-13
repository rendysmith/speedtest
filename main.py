import time
import asyncio

import speedtest

from utils.constant import ss_id, tab_name
from utils.gs_editor import get_service, append_data_to_sheet_scope

async def tst_speed():
    print('- Start!')
    st = speedtest.Speedtest()

    # Получаем серверы и выбираем лучший
    st.get_best_server()

    # Тестируем скорость загрузки (входящая скорость)
    download_speed = st.download()

    # Тестируем скорость отдачи (исходящая скорость)
    upload_speed = st.upload()

    # Преобразуем скорость в Мбит/с
    download_speed_mbps = download_speed / 1_000_000
    upload_speed_mbps = upload_speed / 1_000_000

    return download_speed_mbps, upload_speed_mbps

async def main_tst():
    # Пример использования
    download, upload = await tst_speed()
    print(f"Download speed: {download:.2f} Mbps")
    print(f"Upload speed: {upload:.2f} Mbps")

    service = await get_service()

    datas = {'Date': time.ctime(),
             'Download': download,
             'Upload': upload}

    await append_data_to_sheet_scope(service, ss_id, tab_name, datas)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asyncio.run(main_tst())


