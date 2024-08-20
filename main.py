import speedtest

def test_speed():
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Пример использования
    download, upload = test_speed()
    print(f"Download speed: {download:.2f} Mbps")
    print(f"Upload speed: {upload:.2f} Mbps")

